import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
import warnings
import time
import threading
import queue
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import finnhub

# Try to import yfinance, install if missing
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    print("⚠️ Installing yfinance for fallback data...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'yfinance'])
    import yfinance as yf
    YFINANCE_AVAILABLE = True

# Try to import Kafka components (optional)
try:
    from streamer import UnifiedKafkaManager, MLDataConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️ Kafka components not available (optional)")
    KAFKA_AVAILABLE = False

warnings.filterwarnings('ignore')
load_dotenv()

class StockPredictor:
    """
    Simplified Stock Predictor with LSTM Only
    Removed Prophet dependency for simplified deployment
    """
    
    def __init__(self, symbol='AAPL', finnhub_api_key=None):
        self.symbol = symbol.upper()
        self.finnhub_api_key = finnhub_api_key or os.getenv('FINNHUB_API_KEY')
        self.data = None
        self.real_time_data = queue.Queue()
        self.lstm_model = None
        # Removed Prophet model
        self.scaler = MinMaxScaler()
        self.window_size = 60
        
        # Initialize Finnhub client (for quotes and company info)
        self.finnhub_client = None
        self.finnhub_available = False
        
        if self.finnhub_api_key:
            try:
                self.finnhub_client = finnhub.Client(api_key=self.finnhub_api_key)
                # Test if basic functionality works
                test_quote = self.finnhub_client.quote('AAPL')
                if test_quote and test_quote.get('c') is not None:
                    self.finnhub_available = True
                    print(f"✅ Finnhub client available for {symbol}")
                else:
                    print(f"⚠️ Finnhub client limited functionality for {symbol}")
            except Exception as e:
                print(f"⚠️ Finnhub client initialization failed: {e}")
        
        # Initialize Kafka components (optional)
        self.kafka_manager = None
        self.data_consumer = None
        self.is_streaming = False
        self.last_training_metrics = {}
        
        if KAFKA_AVAILABLE and self.finnhub_api_key:
            try:
                self.kafka_manager = UnifiedKafkaManager(self.finnhub_api_key)
                self.data_consumer = MLDataConsumer()
                print(f"✅ Kafka integration initialized for {symbol}")
            except Exception as e:
                print(f"⚠️ Kafka initialization failed (optional): {e}")
    
    def fetch_data_yfinance(self, period="2y"):
        """Fetch data from Yahoo Finance as fallback"""
        if not YFINANCE_AVAILABLE:
            return False
        
        try:
            print(f"📊 Fetching data from Yahoo Finance for {self.symbol}...")
            
            ticker = yf.Ticker(self.symbol)
            data = ticker.history(period=period)
            
            if data.empty:
                print(f"❌ No data from Yahoo Finance for {self.symbol}")
                return False
            
            # Clean and standardize data
            data = data.dropna()
            
            # Rename columns to match our format
            if 'Adj Close' in data.columns:
                data['Close'] = data['Adj Close']  # Use adjusted close
            
            # Ensure we have required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in data.columns for col in required_cols):
                print(f"❌ Missing required columns in Yahoo Finance data")
                return False
            
            self.data = data[required_cols]
            print(f"✅ Yahoo Finance: Fetched {len(self.data)} records for {self.symbol}")
            print(f"   Date range: {self.data.index[0].date()} to {self.data.index[-1].date()}")
            
            return True
            
        except Exception as e:
            print(f"❌ Yahoo Finance error: {e}")
            return False
    
    def fetch_data_finnhub(self, days_back=365):
        """Try to fetch data from Finnhub (may fail on free tier)"""
        if not self.finnhub_client:
            return False
        
        try:
            print(f"📊 Attempting Finnhub historical data for {self.symbol}...")
            
            # Calculate timestamps
            end_time = int(datetime.now().timestamp())
            start_time = int((datetime.now() - timedelta(days=days_back)).timestamp())
            
            # Add rate limiting
            time.sleep(1)
            
            data = self.finnhub_client.stock_candles(self.symbol, 'D', start_time, end_time)
            
            if data and data.get('s') == 'ok' and data.get('c'):
                df = pd.DataFrame({
                    'timestamp': pd.to_datetime(data['t'], unit='s'),
                    'Open': data['o'],
                    'High': data['h'],
                    'Low': data['l'],
                    'Close': data['c'],
                    'Volume': data['v']
                })
                df.set_index('timestamp', inplace=True)
                df.index.name = 'Date'
                
                # Clean data
                df = df.dropna()
                
                if len(df) >= 100:
                    self.data = df
                    print(f"✅ Finnhub: Fetched {len(df)} records for {self.symbol}")
                    return True
                else:
                    print(f"⚠️ Finnhub: Insufficient data ({len(df)} records)")
                    return False
            else:
                status = data.get('s', 'unknown') if data else 'no response'
                print(f"⚠️ Finnhub: No data - status: {status}")
                return False
                
        except Exception as e:
            error_str = str(e)
            if "403" in error_str or "You don't have access" in error_str:
                print("📋 Finnhub historical data requires subscription - using fallback")
                return False
            elif "429" in error_str:
                print("⚠️ Finnhub rate limit - using fallback")
                return False
            else:
                print(f"⚠️ Finnhub error: {e} - using fallback")
                return False
    
    def fetch_data(self, days_back=730):
        """Smart data fetching with multiple sources (backward compatible)"""
        print(f"🔄 Fetching data for {self.symbol}...")
        
        # Strategy 1: Try Finnhub first (if available)
        if self.finnhub_available:
            if self.fetch_data_finnhub(days_back):
                return True
            else:
                print("📋 Finnhub historical data not accessible (likely free tier limitation)")
        
        # Strategy 2: Use Yahoo Finance as reliable fallback
        print("🔄 Switching to Yahoo Finance (reliable and free)...")
        if YFINANCE_AVAILABLE:
            if self.fetch_data_yfinance():
                return True
            
            # Strategy 3: Try with different periods
            print("🔄 Trying shorter time period...")
            if self.fetch_data_yfinance("1y"):
                return True
        
        print(f"❌ All data sources failed for {self.symbol}")
        return False
    
    def get_current_quote(self):
        """Get current quote - Finnhub free tier usually supports this"""
        if not self.finnhub_client:
            return None
        
        try:
            time.sleep(0.5)  # Rate limiting
            quote = self.finnhub_client.quote(self.symbol)
            return quote
        except Exception as e:
            print(f"⚠️ Error getting quote: {e}")
            return None
    
    def validate_symbol(self):
        """Validate symbol using multiple methods"""
        # Method 1: Try Finnhub quote
        if self.finnhub_available:
            try:
                quote = self.get_current_quote()
                if quote and quote.get('c') is not None and quote.get('c') > 0:
                    return True
            except:
                pass
        
        # Method 2: Try Yahoo Finance
        if YFINANCE_AVAILABLE:
            try:
                ticker = yf.Ticker(self.symbol)
                info = ticker.info
                if info and 'symbol' in info:
                    return True
            except:
                pass
        
        return False
    
    def prepare_lstm_data(self, data, window_size=60):
        """Prepare data for LSTM model"""
        prices = data['Close'].values.reshape(-1, 1)
        scaled_data = self.scaler.fit_transform(prices)
        
        X, y = [], []
        for i in range(window_size, len(scaled_data)):
            X.append(scaled_data[i-window_size:i, 0])
            y.append(scaled_data[i, 0])
        
        return np.array(X), np.array(y)
    
    def build_lstm_model(self, input_shape):
        """Build enhanced LSTM model with more layers for better predictions"""
        model = Sequential([
            LSTM(128, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(64, return_sequences=True),
            Dropout(0.2),
            LSTM(32, return_sequences=False),
            Dropout(0.2),
            Dense(32, activation='relu'),
            Dense(16, activation='relu'),
            Dense(1)
        ])
        
        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='huber',
            metrics=['mae']
        )
        return model
    
    def train_lstm(self, epochs=1, batch_size=32, validation_split=0.2):
        """Train enhanced LSTM model"""
        if self.data is None:
            print("❌ No data available. Please fetch data first.")
            return False
        
        try:
            print(f"🧠 Training enhanced LSTM model for {self.symbol}...")
            
            X, y = self.prepare_lstm_data(self.data, self.window_size)
            
            if len(X) < 100:
                print(f"⚠️ Insufficient data for training: {len(X)} samples")
                return False
            
            # Split data
            split_idx = int(len(X) * 0.8)
            X_train, X_test = X[:split_idx], X[split_idx:]
            y_train, y_test = y[:split_idx], y[split_idx:]
            
            # Reshape for LSTM
            X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
            X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
            
            # Build and train model
            self.lstm_model = self.build_lstm_model((X_train.shape[1], 1))
            
            callbacks = [
                EarlyStopping(monitor='val_loss', patience=15, restore_best_weights=True),
                ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=8, min_lr=0.0001)
            ]
            
            start_time = time.time()
            history = self.lstm_model.fit(
                X_train, y_train,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=validation_split,
                callbacks=callbacks,
                verbose=1
            )
            training_time = time.time() - start_time
            
            # Evaluate model
            train_pred = self.lstm_model.predict(X_train, verbose=0)
            test_pred = self.lstm_model.predict(X_test, verbose=0)
            
            # Inverse transform predictions
            train_pred = self.scaler.inverse_transform(train_pred)
            test_pred = self.scaler.inverse_transform(test_pred)
            y_train_actual = self.scaler.inverse_transform(y_train.reshape(-1, 1))
            y_test_actual = self.scaler.inverse_transform(y_test.reshape(-1, 1))
            
            # Calculate metrics
            train_mae = mean_absolute_error(y_train_actual, train_pred)
            test_mae = mean_absolute_error(y_test_actual, test_pred)
            train_rmse = np.sqrt(mean_squared_error(y_train_actual, train_pred))
            test_rmse = np.sqrt(mean_squared_error(y_test_actual, test_pred))
            
            self.last_training_metrics['lstm'] = {
                'train_mae': float(train_mae),
                'test_mae': float(test_mae),
                'train_rmse': float(train_rmse),
                'test_rmse': float(test_rmse),
                'epochs_trained': len(history.history['loss']),
                'training_time': float(training_time),
                'data_points': len(X)
            }
            
            print(f"✅ Enhanced LSTM Model Training Complete:")
            print(f"   Train MAE: ${train_mae:.2f}, Test MAE: ${test_mae:.2f}")
            print(f"   Training time: {training_time:.1f} seconds")
            
            # Send metrics to Kafka (optional)
            if self.kafka_manager:
                try:
                    self.kafka_manager.send_model_metrics('LSTM', self.symbol, self.last_training_metrics['lstm'])
                except Exception as e:
                    print(f"⚠️ Kafka metrics send failed (optional): {e}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error training LSTM model: {e}")
            return False
    
    # REMOVED: All Prophet-related methods
    # - prepare_prophet_data()
    # - train_prophet()
    # - predict_prophet()
    
    def predict_lstm(self, days_ahead=30):
        """Make enhanced LSTM predictions with confidence estimation"""
        if self.lstm_model is None:
            print("❌ LSTM model not trained. Please train the model first.")
            return None
        
        try:
            last_data = self.data['Close'].values[-self.window_size:]
            scaled_data = self.scaler.transform(last_data.reshape(-1, 1))
            
            predictions = []
            current_batch = scaled_data.reshape(1, self.window_size, 1)
            
            # Generate multiple predictions for confidence estimation
            num_simulations = 10
            all_predictions = []
            
            for sim in range(num_simulations):
                sim_predictions = []
                sim_batch = current_batch.copy()
                
                for _ in range(days_ahead):
                    pred = self.lstm_model.predict(sim_batch, verbose=0)
                    # Add small noise for Monte Carlo simulation
                    noise = np.random.normal(0, 0.01, pred.shape)
                    pred_with_noise = pred + noise
                    sim_predictions.append(pred_with_noise[0, 0])
                    sim_batch = np.append(sim_batch[:, 1:, :], 
                                        pred_with_noise.reshape(1, 1, 1), axis=1)
                
                all_predictions.append(sim_predictions)
            
            # Calculate mean and confidence intervals
            all_predictions = np.array(all_predictions)
            mean_predictions = np.mean(all_predictions, axis=0)
            lower_bound = np.percentile(all_predictions, 25, axis=0)
            upper_bound = np.percentile(all_predictions, 75, axis=0)
            
            # Inverse transform
            mean_predictions = self.scaler.inverse_transform(mean_predictions.reshape(-1, 1))
            lower_bound = self.scaler.inverse_transform(lower_bound.reshape(-1, 1))
            upper_bound = self.scaler.inverse_transform(upper_bound.reshape(-1, 1))
            
            last_date = self.data.index[-1]
            future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), 
                                       periods=days_ahead, freq='D')
            
            prediction_df = pd.DataFrame({
                'Date': future_dates,
                'LSTM_Prediction': mean_predictions.flatten(),
                'LSTM_Lower': lower_bound.flatten(),
                'LSTM_Upper': upper_bound.flatten()
            })
            
            # Send predictions to Kafka (optional)
            if self.kafka_manager:
                try:
                    predictions_data = {
                        'model_type': 'Enhanced_LSTM',
                        'symbol': self.symbol,
                        'days_ahead': days_ahead,
                        'predictions': {
                            'dates': future_dates.strftime('%Y-%m-%d').tolist(),
                            'prices': mean_predictions.flatten().tolist(),
                            'lower_bound': lower_bound.flatten().tolist(),
                            'upper_bound': upper_bound.flatten().tolist()
                        },
                        'last_actual_price': float(self.data['Close'].iloc[-1]),
                        'prediction_timestamp': datetime.now().isoformat()
                    }
                    self.kafka_manager.send_prediction_results(self.symbol, predictions_data)
                except Exception as e:
                    print(f"⚠️ Kafka prediction send failed (optional): {e}")
            
            print(f"📈 Generated {days_ahead}-day enhanced LSTM predictions for {self.symbol}")
            return prediction_df
            
        except Exception as e:
            print(f"❌ Error making LSTM predictions: {e}")
            return None
    
    def get_current_data(self):
        """Get current stock data with mixed sources"""
        if self.data is None:
            return None
        
        try:
            # Get latest from historical data
            current_price = self.data['Close'].iloc[-1]
            prev_price = self.data['Close'].iloc[-2] if len(self.data) > 1 else current_price
            
            # Try to get real-time quote from Finnhub
            if self.finnhub_available:
                quote = self.get_current_quote()
                if quote and quote.get('c'):
                    current_price = quote['c']
                    prev_price = quote.get('pc', prev_price)
            
            change = current_price - prev_price
            change_pct = (change / prev_price) * 100 if prev_price != 0 else 0
            
            # Calculate indicators
            ma_20 = self.data['Close'].rolling(window=20).mean().iloc[-1]
            ma_50 = self.data['Close'].rolling(window=50).mean().iloc[-1]
            
            returns = self.data['Close'].pct_change().dropna()
            volatility = returns.rolling(window=30).std().iloc[-1] * np.sqrt(252) * 100
            
            current_data = {
                'symbol': self.symbol,
                'current_price': round(float(current_price), 2),
                'change': round(float(change), 2),
                'change_pct': round(float(change_pct), 2),
                'volume': int(self.data['Volume'].iloc[-1]),
                'ma_20': round(float(ma_20), 2),
                'ma_50': round(float(ma_50), 2),
                'volatility': round(float(volatility), 2),
                'high_52w': round(float(self.data['High'].rolling(window=252).max().iloc[-1]), 2),
                'low_52w': round(float(self.data['Low'].rolling(window=252).min().iloc[-1]), 2),
                'data_source': 'hybrid_lstm_only',
                'last_updated': datetime.now().isoformat(),
                'is_real_time': self.finnhub_available
            }
            
            # Send to Kafka (optional)
            if self.kafka_manager:
                try:
                    self.kafka_manager.send_stock_data(current_data)
                except Exception as e:
                    print(f"⚠️ Kafka data send failed (optional): {e}")
            
            return current_data
            
        except Exception as e:
            print(f"❌ Error getting current data: {e}")
            return None

    def get_historical_data(self, days=100):
        """Get enhanced historical data for plotting with better trend visibility"""
        if self.data is None:
            return None
        
        try:
            # Get more data points for smoother curves
            recent_data = self.data.tail(days)
            
            # Calculate additional technical indicators for better visualization
            recent_data = recent_data.copy()
            
            # Calculate moving averages for trend lines
            recent_data['MA_5'] = recent_data['Close'].rolling(window=5).mean()
            recent_data['MA_20'] = recent_data['Close'].rolling(window=20).mean()
            
            # Calculate daily returns for volatility visualization
            recent_data['Daily_Return'] = recent_data['Close'].pct_change()
            recent_data['Volatility'] = recent_data['Daily_Return'].rolling(window=10).std() * 100
            
            # Calculate price momentum
            recent_data['Price_Change'] = recent_data['Close'].diff()
            recent_data['Price_Change_Pct'] = recent_data['Close'].pct_change() * 100
            
            # Calculate support and resistance levels
            rolling_max = recent_data['High'].rolling(window=20).max()
            rolling_min = recent_data['Low'].rolling(window=20).min()
            
            return {
                'dates': recent_data.index.strftime('%Y-%m-%d').tolist(),
                'prices': recent_data['Close'].round(2).tolist(),
                'volumes': recent_data['Volume'].tolist(),
                'opens': recent_data['Open'].round(2).tolist(),
                'highs': recent_data['High'].round(2).tolist(),
                'lows': recent_data['Low'].round(2).tolist(),
                
                # Enhanced data for better visualization
                'ma_5': recent_data['MA_5'].round(2).tolist(),
                'ma_20': recent_data['MA_20'].round(2).tolist(),
                'daily_returns': recent_data['Daily_Return'].round(4).tolist(),
                'volatility': recent_data['Volatility'].round(2).tolist(),
                'price_changes': recent_data['Price_Change'].round(2).tolist(),
                'price_change_pct': recent_data['Price_Change_Pct'].round(2).tolist(),
                'resistance_levels': rolling_max.round(2).tolist(),
                'support_levels': rolling_min.round(2).tolist(),
                
                # Statistical info for auto-scaling
                'price_stats': {
                    'min': float(recent_data['Close'].min()),
                    'max': float(recent_data['Close'].max()),
                    'mean': float(recent_data['Close'].mean()),
                    'std': float(recent_data['Close'].std()),
                    'variation_pct': float((recent_data['Close'].max() - recent_data['Close'].min()) / recent_data['Close'].mean() * 100)
                },
                
                'data_source': 'enhanced_historical'
            }
            
        except Exception as e:
            print(f"❌ Error getting enhanced historical data: {e}")
            
            # Fallback to basic data if enhanced calculation fails
            try:
                recent_data = self.data.tail(days)
                return {
                    'dates': recent_data.index.strftime('%Y-%m-%d').tolist(),
                    'prices': recent_data['Close'].round(2).tolist(),
                    'volumes': recent_data['Volume'].tolist(),
                    'opens': recent_data['Open'].round(2).tolist(),
                    'highs': recent_data['High'].round(2).tolist(),
                    'lows': recent_data['Low'].round(2).tolist(),
                    'data_source': 'basic_fallback'
                }
            except Exception as fallback_error:
                print(f"❌ Error in fallback historical data: {fallback_error}")
                return None
    
    def start_real_time_updates(self):
        """Start real-time data updates (backward compatible)"""
        if self.kafka_manager and not self.is_streaming:
            return self.start_real_time_streaming()
        return False
    
    def start_real_time_streaming(self):
        """Start real-time data streaming (optional, backward compatible)"""
        if not self.kafka_manager or self.is_streaming:
            return False
        
        try:
            self.is_streaming = True
            
            def real_time_callback(data):
                """Callback for real-time data"""
                try:
                    if data.get('type') == 'ohlcv_1min':
                        # Add new OHLCV data to queue
                        self.real_time_data.put(data)
                        print(f"📊 Real-time update for {self.symbol}: ${data['close']:.2f}")
                except Exception as e:
                    print(f"❌ Error in real-time callback: {e}")
            
            # Start consuming real-time data in a separate thread
            if self.data_consumer:
                consumer_thread = threading.Thread(
                    target=self.data_consumer.get_real_time_ohlcv,
                    args=(self.symbol, real_time_callback),
                    daemon=True
                )
                consumer_thread.start()
                
                print(f"📈 Started real-time streaming for {self.symbol}")
                return True
            
        except Exception as e:
            print(f"❌ Error starting real-time streaming: {e}")
            self.is_streaming = False
            return False
    
    def stop_real_time_updates(self):
        """Stop real-time data updates"""
        if self.kafka_manager:
            self.kafka_manager.stop_streaming()
            self.is_streaming = False
    
    def get_model_performance(self):
        """Get performance metrics for LSTM model only"""
        performance = {
            'lstm_trained': self.lstm_model is not None,
            'prophet_trained': False,  # Always False in simplified version
            'data_points': len(self.data) if self.data is not None else 0,
            'symbol': self.symbol,
            'is_streaming': self.is_streaming,
            'last_training_metrics': self.last_training_metrics,
            'finnhub_connected': self.finnhub_available,
            'data_source': 'lstm_only',
            'model_type': 'Enhanced LSTM with Confidence Intervals'
        }
        
        return performance


# For backward compatibility
SimplifiedStockPredictor = StockPredictor

# Example usage and testing
if __name__ == "__main__":
    print("🧪 Testing Simplified Stock Predictor (LSTM Only)")
    
    try:
        predictor = StockPredictor('IBM')
        
        print("\n🔍 Testing symbol validation...")
        if predictor.validate_symbol():
            print("✅ IBM is a valid symbol")
        
        print("\n📊 Fetching data...")
        if predictor.fetch_data():
            print("✅ Data fetched successfully!")
            print(f"   Data shape: {predictor.data.shape}")
            print(f"   Date range: {predictor.data.index[0].date()} to {predictor.data.index[-1].date()}")
            
            print("\n💲 Testing current data...")
            current = predictor.get_current_data()
            if current:
                print(f"✅ Current price: ${current['current_price']}")
                print(f"   Change: ${current['change']} ({current['change_pct']:.1f}%)")
                print(f"   Data source: {current['data_source']}")
            
            print("\n🧠 Training enhanced LSTM model...")
            if predictor.train_lstm(epochs=1):  
                print("✅ Enhanced LSTM model trained!")
                
                pred = predictor.predict_lstm(7)
                if pred is not None:
                    print("📈 Enhanced LSTM predictions (next 3 days with confidence):")
                    print(pred.head(3))
        
        else:
            print("❌ Failed to fetch data!")
    
    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()
        print("   Simplified version uses LSTM only for faster deployment")