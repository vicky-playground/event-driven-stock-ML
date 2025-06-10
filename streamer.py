import json
import time
import websocket
import threading
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import numpy as np
from collections import deque, defaultdict
import os
from dotenv import load_dotenv
import ssl
import finnhub

load_dotenv()

class UnifiedKafkaManager:
    """
    Unified Kafka manager with Official Finnhub Python Client
    """
    
    def __init__(self, finnhub_api_key=None, kafka_servers=['localhost:9092']):
        self.finnhub_api_key = finnhub_api_key or os.getenv('FINNHUB_API_KEY')
        self.kafka_servers = kafka_servers
        self.producer = None
        self.ws = None
        self.is_streaming = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Initialize Finnhub client
        self.finnhub_client = None
        if self.finnhub_api_key:
            try:
                self.finnhub_client = finnhub.Client(api_key=self.finnhub_api_key)
                print("Finnhub client initialized")
            except Exception as e:
                print(f"Error initializing Finnhub client: {e}")
        
        # Data buffers for real-time aggregation
        self.price_buffers = defaultdict(lambda: deque(maxlen=1000))
        self.volume_buffers = defaultdict(lambda: deque(maxlen=1000))
        self.last_prices = {}
        self.subscribed_symbols = set()
        
        # Kafka topics organization
        self.topics = {
            # Real-time data topics
            'finnhub_trades': 'finnhub-trades',
            'finnhub_ohlc': 'finnhub-ohlc-1min',
            'finnhub_historical': 'finnhub-historical',
            'finnhub_news': 'finnhub-news',
            'finnhub_company_profiles': 'finnhub-company-profiles',
            
            # ML model topics
            'model_predictions': 'ml-predictions',
            'model_metrics': 'ml-model-metrics',
            'model_training_status': 'ml-training-status',
            
            # Application topics
            'stock_data': 'stock-current-data',
            'app_events': 'app-events',
            'user_actions': 'user-actions',
            'alerts': 'price-alerts'
        }
        
        self.connect_kafka()
    
    def connect_kafka(self, max_retries=5, retry_delay=2):
        """Connect to Kafka with retries - more tolerant"""
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10,
                    compression_type='gzip',
                    request_timeout_ms=30000,
                    api_version=(0, 10, 1)
                )
                print(f"Connected to Kafka on attempt {attempt + 1}")
                return True
            except NoBrokersAvailable:
                print(f"Kafka connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            except Exception as e:
                print(f"Kafka connection error (attempt {attempt + 1}): {e}")
                time.sleep(retry_delay)
        
        print("⚠️ Could not connect to Kafka - continuing without Kafka")
        return False

    
    # ===========================================
    # ML MODEL INTEGRATION METHODS
    # ===========================================
    
    def send_prediction_results(self, symbol, predictions_data):
        """Send ML model prediction results to Kafka"""
        if not self.producer:
            return False
        
        message = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'type': 'prediction_results',
            'predictions': predictions_data
        }
        
        return self.send_to_kafka(self.topics['model_predictions'], symbol, message)
    
    def send_model_metrics(self, model_type, symbol, metrics):
        """Send model training/performance metrics to Kafka"""
        if not self.producer:
            return False
        
        message = {
            'model_type': model_type,
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'metrics': metrics,
            'type': 'model_metrics'
        }
        
        return self.send_to_kafka(self.topics['model_metrics'], f"{model_type}-{symbol}", message)
    
    def send_training_status(self, symbol, status, progress=None):
        """Send model training status updates"""
        if not self.producer:
            return False
        
        message = {
            'symbol': symbol,
            'status': status,  # 'started', 'in_progress', 'completed', 'failed'
            'progress': progress,
            'timestamp': datetime.now().isoformat(),
            'type': 'training_status'
        }
        
        return self.send_to_kafka(self.topics['model_training_status'], symbol, message)
    
    # ===========================================
    # APPLICATION DATA METHODS
    # ===========================================
    
    def send_stock_data(self, stock_data):
        """Send current stock data"""
        if not self.producer:
            return False
        
        message = {
            'timestamp': datetime.now().isoformat(),
            'data': stock_data,
            'type': 'current_stock_data'
        }
        
        return self.send_to_kafka(self.topics['stock_data'], stock_data.get('symbol', 'unknown'), message)
    
    def send_user_action(self, action, details):
        """Send user action events for analytics"""
        if not self.producer:
            return False
        
        message = {
            'action': action,
            'details': details,
            'timestamp': datetime.now().isoformat(),
            'type': 'user_action'
        }
        
        return self.send_to_kafka(self.topics['user_actions'], action, message)
    
    def send_price_alert(self, symbol, current_price, alert_condition):
        """Send price alerts"""
        if not self.producer:
            return False
        
        message = {
            'symbol': symbol,
            'current_price': current_price,
            'alert_condition': alert_condition,
            'timestamp': datetime.now().isoformat(),
            'type': 'price_alert'
        }
        
        return self.send_to_kafka(self.topics['alerts'], symbol, message)
    
    # ===========================================
    # UTILITY METHODS
    # ===========================================
    
    def send_to_kafka(self, topic, key, data):
        """Generic method to send data to Kafka topic"""
        if not self.producer:
            return False
        
        try:
            future = self.producer.send(topic, key=key, value=data)
            future.get(timeout=5)
            return True
        except Exception as e:
            return False
    
    def on_error(self, ws, error):
        """Handle WebSocket errors gracefully"""
        pass  # Ignore WebSocket errors - they're optional
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        self.reconnect_attempts += 1
        
        # Try to reconnect if not too many attempts
        if self.reconnect_attempts < self.max_reconnect_attempts and self.is_streaming:
            time.sleep(5)  # Wait before reconnecting
            self.start_finnhub_streaming()
    
    def on_open(self, ws):
        """Handle WebSocket open"""
        print("Real-time WebSocket connected")
        self.reconnect_attempts = 0
        
        # Subscribe to existing symbols
        for symbol in self.subscribed_symbols:
            try:
                subscribe_message = {"type": "subscribe", "symbol": symbol}
                ws.send(json.dumps(subscribe_message))
            except Exception as e:
                pass  # Ignore subscription errors
    
    def get_current_price(self, symbol):
        """Get current price for a symbol (try real-time first, then API)"""
        # Try real-time price first
        if symbol.upper() in self.last_prices:
            return self.last_prices[symbol.upper()]
        
        # Fall back to API quote
        quote = self.get_quote(symbol)
        if quote and quote.get('c'):
            return quote['c']
        
        return None
    
    def get_subscribed_symbols(self):
        """Get list of currently subscribed symbols"""
        return list(self.subscribed_symbols)
    
    def stop_streaming(self):
        """Stop all streaming and close connections"""
        self.is_streaming = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
            except:
                pass
        print("🛑 Streaming stopped")

# ===========================================
# KAFKA CONSUMER FOR ML MODELS
# ===========================================

class MLDataConsumer:
    """Consumer specifically for ML models to get data from Kafka"""
    
    def __init__(self, kafka_servers=['localhost:9092']):
        self.kafka_servers = kafka_servers
        
    def create_consumer(self, topics, group_id='ml-models'):
        """Create Kafka consumer for specific topics"""
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.kafka_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=5000
            )
            return consumer
        except Exception as e:
            print(f"⚠️ Error creating consumer: {e}")
            return None
    
    def get_historical_data(self, symbol, timeout=10000):
        """Get historical data for a specific symbol from Kafka"""
        consumer = self.create_consumer(['finnhub-historical'], f'historical-{symbol}')
        if not consumer:
            return None
        
        try:
            start_time = time.time()
            
            for message in consumer:
                data = message.value
                if (data.get('symbol', '').upper() == symbol.upper() and 
                    data.get('type') == 'historical'):
                    
                    df_data = data['data']
                    df = pd.DataFrame(df_data)
                    if 'timestamp' in df.columns:
                        df['Date'] = pd.to_datetime(df['timestamp'])
                        df.set_index('Date', inplace=True)
                    
                    df = df[['open', 'high', 'low', 'close', 'volume']]
                    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                    
                    consumer.close()
                    return df
                
                # Timeout check
                if (time.time() - start_time) * 1000 > timeout:
                    break
            
            consumer.close()
            return None
            
        except Exception as e:
            print(f"⚠️ Error consuming historical data: {e}")
            try:
                consumer.close()
            except:
                pass
            return None
    
    def get_real_time_ohlcv(self, symbol, callback, timeout=None):
        """Get real-time OHLCV data and call callback function"""
        consumer = self.create_consumer(['finnhub-ohlc-1min'], f'realtime-{symbol}')
        
        if not consumer:
            return
        
        try:
            start_time = time.time()
            
            for message in consumer:
                data = message.value
                if (data.get('symbol', '').upper() == symbol.upper() and 
                    data.get('type') == 'ohlcv_1min'):
                    callback(data)
                
                # Timeout check
                if timeout and (time.time() - start_time) > timeout:
                    break
                    
        except Exception as e:
            print(f"⚠️ Error consuming real-time data: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass

# ===========================================
# EXAMPLE USAGE
# ===========================================

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Test the fixed Kafka manager
    api_key = os.getenv('FINNHUB_API_KEY')

    
    try:
        kafka_manager = UnifiedKafkaManager(api_key)
        
        # Test basic functionality
        if kafka_manager.validate_symbol('AAPL'):
            print("AAPL is a valid symbol")
        else:
            print("❌ AAPL validation failed")
        
        df = kafka_manager.get_historical_data('AAPL', days_back=30)
        if df is not None and not df.empty:
            print(f"✅ Historical data fetched: {len(df)} records")
            print(f"   Latest price: ${df['close'].iloc[-1]:.2f}")
        else:
            print("❌ Could not fetch historical data")
        
        print("\n📊 Testing real-time quote...")
        quote = kafka_manager.get_quote('AAPL')
        if quote and quote.get('c'):
            print(f"✅ Current price: ${quote['c']:.2f}")
        else:
            print("❌ Could not get current quote")
        
        print("\n📊 Testing company profile...")
        profile = kafka_manager.get_company_profile('AAPL')
        if profile and profile.get('name'):
            print(f"✅ Company: {profile['name']}")
        else:
            print("❌ Could not get company profile")
        
        print("\n📊 Testing symbols fetch...")
        symbols = kafka_manager.get_stock_symbols('US')
        if symbols:
            print(f"✅ Fetched {len(symbols)} US symbols")
        else:
            print("❌ Could not fetch symbols")
        
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"❌ Test error: {e}")
        print("   Make sure your Finnhub API key is valid and active")
