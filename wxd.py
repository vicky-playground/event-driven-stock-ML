# ============================================================================
# FIXED WATSONX.DATA INTEGRATION - Local Development Compatible
# High-performance analytics with local Spark and optional watsonx.data
# ============================================================================

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv

# Import your existing components
try:
    from event_store import Event, StockAnalysisEvents
    EVENT_STORE_AVAILABLE = True
except ImportError:
    EVENT_STORE_AVAILABLE = False
    print("⚠️ Event store not available")

try:
    from stock_predictor import StockPredictor
    STOCK_PREDICTOR_AVAILABLE = True
except ImportError:
    STOCK_PREDICTOR_AVAILABLE = False
    print("⚠️ Stock predictor not available")

# Try to import Spark (optional for local development)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
    print("✅ PySpark available for local development")
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️ PySpark not available - using pandas fallback")

load_dotenv()

# watsonx.data Configuration (from environment)
CATALOG_NAME = os.getenv('CATALOG_NAME', 'local_catalog')
SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'stock')
DB_BUCKET = os.getenv('DB_BUCKET', 'local-bucket')
DB_BUCKET_ENDPOINT = os.getenv('DB_BUCKET_ENDPOINT', 'localhost')
DB_BUCKET_ACCESS_KEY = os.getenv('DB_BUCKET_ACCESS_KEY', 'test-key')
DB_BUCKET_SECRET_KEY = os.getenv('DB_BUCKET_SECRET_KEY', 'test-secret')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WatsonXDataManager:
    """
    Enhanced Data Manager with local fallback support
    Works with both watsonx.data and local Spark/pandas
    """
    
    def __init__(self):
        self.spark = None
        self.catalog_name = CATALOG_NAME
        self.schema_name = SCHEMA_NAME
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.local_mode = True  # Start in local mode
        self.data_cache = {}  # Local cache for development
        
        # Try to initialize Spark, fallback to local mode
        self.initialize_spark()
        self.setup_storage()
    
    def initialize_spark(self):
        """Initialize Spark with local fallback"""
        if not SPARK_AVAILABLE:
            logger.info("🔧 PySpark not available - using local pandas mode")
            self.local_mode = True
            return
        
        try:
            # Try local Spark first (for development)
            logger.info("🚀 Initializing local Spark session...")
            self.spark = SparkSession.builder \
                .appName("LocalStockPrediction") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.master", "local[*]") \
                .getOrCreate()
            
            # Test basic functionality
            test_df = self.spark.range(1).toDF("test")
            test_df.count()  # Simple test
            
            self.local_mode = False
            logger.info("✅ Local Spark session initialized successfully")
            
        except Exception as e:
            logger.warning(f"⚠️ Spark initialization failed: {e}")
            logger.info("🔧 Falling back to pandas-only mode")
            self.local_mode = True
            self.spark = None
    
    def setup_storage(self):
        """Setup storage - creates local directories or Spark tables"""
        try:
            if self.local_mode:
                # Create local directories for data storage
                os.makedirs('local_data/events', exist_ok=True)
                os.makedirs('local_data/stock_data', exist_ok=True)
                os.makedirs('local_data/technical_indicators', exist_ok=True)
                os.makedirs('local_data/predictions', exist_ok=True)
                os.makedirs('local_data/read_models', exist_ok=True)
                logger.info("✅ Local storage directories created")
            else:
                # Create Spark tables (without catalog commands for local Spark)
                self.setup_spark_tables()
                
        except Exception as e:
            logger.error(f"❌ Error setting up storage: {e}")
    
    def setup_spark_tables(self):
        """Create Spark tables (compatible with local Spark)"""
        try:
            # Create database/schema (local Spark compatible)
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}")
            self.spark.sql(f"USE {self.schema_name}")
            
            # 1. Events Table (simplified for local Spark)
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS events (
                    event_id STRING,
                    event_type STRING,
                    aggregate_id STRING,
                    aggregate_type STRING,
                    event_data STRING,
                    metadata STRING,
                    timestamp TIMESTAMP,
                    version INT,
                    processing_date DATE
                ) USING DELTA
            """)
            
            # 2. Stock Data Table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol STRING,
                    date DATE,
                    timestamp TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume LONG,
                    adj_close DOUBLE,
                    data_source STRING,
                    created_at TIMESTAMP
                ) USING DELTA
            """)
            
            # 3. Technical Indicators Table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    symbol STRING,
                    date DATE,
                    ma_5 DOUBLE,
                    ma_10 DOUBLE,
                    ma_20 DOUBLE,
                    ma_50 DOUBLE,
                    rsi DOUBLE,
                    macd DOUBLE,
                    bollinger_upper DOUBLE,
                    bollinger_lower DOUBLE,
                    volatility DOUBLE,
                    volume_avg DOUBLE,
                    price_change_pct DOUBLE,
                    updated_at TIMESTAMP
                ) USING DELTA
            """)
            
            # 4. Predictions Table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS predictions (
                    symbol STRING,
                    prediction_date DATE,
                    target_date DATE,
                    model_type STRING,
                    predicted_price DOUBLE,
                    confidence_lower DOUBLE,
                    confidence_upper DOUBLE,
                    model_version STRING,
                    accuracy_score DOUBLE,
                    created_at TIMESTAMP,
                    analysis_id STRING
                ) USING DELTA
            """)
            
            # 5. Read Models Table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS read_models (
                    symbol STRING,
                    model_type STRING,
                    status STRING,
                    analysis_id STRING,
                    current_price DOUBLE,
                    price_change DOUBLE,
                    price_change_pct DOUBLE,
                    predictions_json STRING,
                    last_updated TIMESTAMP,
                    completion_timestamp TIMESTAMP
                ) USING DELTA
            """)
            
            logger.info("✅ Local Spark tables created successfully")
            
        except Exception as e:
            logger.error(f"❌ Error setting up Spark tables: {e}")
            # Fallback to local mode
            self.local_mode = True
            self.spark = None
            self.setup_storage()
    
    # ============================================================================
    # ENHANCED EVENT SOURCING (LOCAL COMPATIBLE)
    # ============================================================================
    
    async def store_event_async(self, event) -> bool:
        """Store event with local fallback"""
        try:
            if self.local_mode:
                return await self.store_event_local(event)
            else:
                return await self.store_event_spark(event)
        except Exception as e:
            logger.error(f"❌ Error storing event: {e}")
            return False
    
    async def store_event_local(self, event) -> bool:
        """Store event locally using JSON files"""
        try:
            event_data = {
                'event_id': event.event_id if hasattr(event, 'event_id') else str(event.get('event_id', '')),
                'event_type': event.event_type if hasattr(event, 'event_type') else str(event.get('event_type', '')),
                'aggregate_id': event.aggregate_id if hasattr(event, 'aggregate_id') else str(event.get('aggregate_id', '')),
                'aggregate_type': event.aggregate_type if hasattr(event, 'aggregate_type') else str(event.get('aggregate_type', '')),
                'data': event.data if hasattr(event, 'data') else event.get('data', {}),
                'metadata': event.metadata if hasattr(event, 'metadata') else event.get('metadata', {}),
                'timestamp': datetime.now().isoformat(),
                'version': event.version if hasattr(event, 'version') else event.get('version', 1)
            }
            
            # Store in local file
            filename = f"local_data/events/{event_data['aggregate_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(event_data, f, indent=2)
            
            logger.info(f"📝 Event stored locally: {event_data['event_type']} for {event_data['aggregate_id']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing event locally: {e}")
            return False
    
    async def store_event_spark(self, event) -> bool:
        """Store event using Spark"""
        try:
            event_data = [(
                event.event_id,
                event.event_type,
                event.aggregate_id,
                event.aggregate_type,
                json.dumps(event.data),
                json.dumps(event.metadata),
                datetime.now(),
                event.version,
                datetime.now().date()
            )]
            
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("aggregate_id", StringType(), True),
                StructField("aggregate_type", StringType(), True),
                StructField("event_data", StringType(), True),
                StructField("metadata", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("version", IntegerType(), True),
                StructField("processing_date", DateType(), True)
            ])
            
            df = self.spark.createDataFrame(event_data, schema)
            df.write.mode("append").saveAsTable("events")
            
            logger.info(f"📝 Event stored in Spark: {event.event_type} for {event.aggregate_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing event in Spark: {e}")
            return False
    
    # ============================================================================
    # ENHANCED STOCK DATA METHODS (LOCAL COMPATIBLE)
    # ============================================================================
    
    async def store_stock_data_bulk(self, symbol: str, data: pd.DataFrame) -> bool:
        """Store stock data with local fallback"""
        try:
            if self.local_mode:
                return await self.store_stock_data_local(symbol, data)
            else:
                return await self.store_stock_data_spark(symbol, data)
        except Exception as e:
            logger.error(f"❌ Error storing stock data: {e}")
            return False
    
    async def store_stock_data_local(self, symbol: str, data: pd.DataFrame) -> bool:
        """Store stock data locally"""
        try:
            # Add metadata
            data_copy = data.copy()
            data_copy['symbol'] = symbol
            data_copy['data_source'] = 'local_enhanced'
            data_copy['created_at'] = datetime.now()
            
            # Save to local file
            filename = f"local_data/stock_data/{symbol}_{datetime.now().strftime('%Y%m%d')}.csv"
            data_copy.to_csv(filename)
            
            # Cache in memory for fast access
            self.data_cache[f"stock_data_{symbol}"] = data_copy
            
            logger.info(f"📊 Stock data stored locally for {symbol}: {len(data)} records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing stock data locally: {e}")
            return False
    
    async def store_stock_data_spark(self, symbol: str, data: pd.DataFrame) -> bool:
        """Store stock data using Spark"""
        try:
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(data.reset_index())
            
            # Add metadata
            spark_df = spark_df.withColumn("symbol", lit(symbol)) \
                             .withColumn("data_source", lit("spark_enhanced")) \
                             .withColumn("created_at", current_timestamp()) \
                             .withColumn("date", col("Date").cast("date"))
            
            # Save to table
            spark_df.write.mode("append").saveAsTable("stock_data")
            
            logger.info(f"📊 Stock data stored in Spark for {symbol}: {len(data)} records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing stock data in Spark: {e}")
            return False
    
    async def get_stock_data_optimized(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """Get stock data with local fallback"""
        try:
            if self.local_mode:
                return await self.get_stock_data_local(symbol, days)
            else:
                return await self.get_stock_data_spark(symbol, days)
        except Exception as e:
            logger.error(f"❌ Error getting stock data: {e}")
            return pd.DataFrame()
    
    async def get_stock_data_local(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """Get stock data from local cache/files"""
        try:
            # Check cache first
            cache_key = f"stock_data_{symbol}"
            if cache_key in self.data_cache:
                cached_data = self.data_cache[cache_key]
                logger.info(f"📈 Retrieved cached stock data for {symbol}: {len(cached_data)} records")
                return cached_data
            
            # Try to load from file
            filename = f"local_data/stock_data/{symbol}_{datetime.now().strftime('%Y%m%d')}.csv"
            if os.path.exists(filename):
                data = pd.read_csv(filename, index_col=0)
                self.data_cache[cache_key] = data
                logger.info(f"📈 Retrieved local file stock data for {symbol}: {len(data)} records")
                return data
            
            # Return empty DataFrame if no data found
            logger.info(f"📈 No cached data found for {symbol}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Error getting local stock data: {e}")
            return pd.DataFrame()
    
    async def get_stock_data_spark(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """Get stock data using Spark"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            query = f"""
                SELECT date, open, high, low, close, volume, adj_close
                FROM stock_data
                WHERE symbol = '{symbol}'
                AND date >= '{start_date}'
                AND date <= '{end_date}'
                ORDER BY date ASC
            """
            
            df = self.spark.sql(query)
            pandas_df = df.toPandas()
            
            if not pandas_df.empty:
                pandas_df.set_index('date', inplace=True)
            
            logger.info(f"📈 Retrieved Spark stock data for {symbol}: {len(pandas_df)} records")
            return pandas_df
            
        except Exception as e:
            logger.error(f"❌ Error getting Spark stock data: {e}")
            return pd.DataFrame()
    
    # ============================================================================
    # TECHNICAL INDICATORS (LOCAL COMPATIBLE)
    # ============================================================================
    
    async def compute_technical_indicators_local(self, symbol: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Compute technical indicators using pandas"""
        try:
            if data.empty:
                return {}
            
            # Calculate technical indicators using pandas
            indicators = {}
            
            # Moving averages
            indicators['ma_5'] = data['Close'].rolling(window=5).mean().iloc[-1] if len(data) >= 5 else data['Close'].iloc[-1]
            indicators['ma_10'] = data['Close'].rolling(window=10).mean().iloc[-1] if len(data) >= 10 else data['Close'].iloc[-1]
            indicators['ma_20'] = data['Close'].rolling(window=20).mean().iloc[-1] if len(data) >= 20 else data['Close'].iloc[-1]
            indicators['ma_50'] = data['Close'].rolling(window=50).mean().iloc[-1] if len(data) >= 50 else data['Close'].iloc[-1]
            
            # Volatility
            returns = data['Close'].pct_change()
            indicators['volatility'] = returns.rolling(window=30).std().iloc[-1] * np.sqrt(252) if len(data) >= 30 else 0.0
            
            # Price change
            if len(data) >= 2:
                indicators['price_change_pct'] = ((data['Close'].iloc[-1] - data['Close'].iloc[-2]) / data['Close'].iloc[-2]) * 100
            else:
                indicators['price_change_pct'] = 0.0
            
            # Volume average
            indicators['volume_avg'] = data['Volume'].rolling(window=20).mean().iloc[-1] if len(data) >= 20 else data['Volume'].iloc[-1]
            
            # Bollinger bands
            if len(data) >= 20:
                ma_20 = data['Close'].rolling(window=20).mean().iloc[-1]
                std_20 = data['Close'].rolling(window=20).std().iloc[-1]
                indicators['bollinger_upper'] = ma_20 + (2 * std_20)
                indicators['bollinger_lower'] = ma_20 - (2 * std_20)
            else:
                indicators['bollinger_upper'] = data['Close'].iloc[-1]
                indicators['bollinger_lower'] = data['Close'].iloc[-1]
            
            # Simple RSI and MACD (simplified versions)
            indicators['rsi'] = 50.0  # Placeholder
            indicators['macd'] = 0.0  # Placeholder
            
            indicators['symbol'] = symbol
            indicators['date'] = datetime.now().date()
            indicators['updated_at'] = datetime.now()
            
            logger.info(f"📊 Technical indicators computed locally for {symbol}")
            return indicators
            
        except Exception as e:
            logger.error(f"❌ Error computing local technical indicators: {e}")
            return {}
    
    # ============================================================================
    # READ MODELS (LOCAL COMPATIBLE)
    # ============================================================================
    
    async def update_read_model_optimized(self, symbol: str, analysis_data: Dict[str, Any]) -> bool:
        """Update read model with local fallback"""
        try:
            if self.local_mode:
                return await self.update_read_model_local(symbol, analysis_data)
            else:
                return await self.update_read_model_spark(symbol, analysis_data)
        except Exception as e:
            logger.error(f"❌ Error updating read model: {e}")
            return False
    
    async def update_read_model_local(self, symbol: str, analysis_data: Dict[str, Any]) -> bool:
        """Update read model locally"""
        try:
            read_model_data = {
                'symbol': symbol,
                'model_type': analysis_data.get('model_type', 'local_enhanced_lstm'),
                'status': analysis_data.get('status', 'completed'),
                'analysis_id': analysis_data.get('analysis_id', ''),
                'current_price': analysis_data.get('current_price', 0.0),
                'price_change': analysis_data.get('price_change', 0.0),
                'price_change_pct': analysis_data.get('price_change_pct', 0.0),
                'predictions': analysis_data.get('predictions', {}),
                'last_updated': datetime.now().isoformat(),
                'completion_timestamp': datetime.now().isoformat()
            }
            
            # Store locally
            filename = f"local_data/read_models/{symbol}_read_model.json"
            with open(filename, 'w') as f:
                json.dump(read_model_data, f, indent=2)
            
            # Cache in memory
            self.data_cache[f"read_model_{symbol}"] = read_model_data
            
            logger.info(f"📊 Read model updated locally for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating local read model: {e}")
            return False
    
    async def update_read_model_spark(self, symbol: str, analysis_data: Dict[str, Any]) -> bool:
        """Update read model using Spark"""
        try:
            read_model_data = [(
                symbol,
                analysis_data.get('model_type', 'spark_enhanced_lstm'),
                analysis_data.get('status', 'completed'),
                analysis_data.get('analysis_id', ''),
                analysis_data.get('current_price', 0.0),
                analysis_data.get('price_change', 0.0),
                analysis_data.get('price_change_pct', 0.0),
                json.dumps(analysis_data.get('predictions', {})),
                datetime.now(),
                datetime.now()
            )]
            
            schema = StructType([
                StructField("symbol", StringType(), True),
                StructField("model_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("analysis_id", StringType(), True),
                StructField("current_price", DoubleType(), True),
                StructField("price_change", DoubleType(), True),
                StructField("price_change_pct", DoubleType(), True),
                StructField("predictions_json", StringType(), True),
                StructField("last_updated", TimestampType(), True),
                StructField("completion_timestamp", TimestampType(), True)
            ])
            
            df = self.spark.createDataFrame(read_model_data, schema)
            df.write.mode("append").saveAsTable("read_models")
            
            logger.info(f"📊 Read model updated in Spark for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating Spark read model: {e}")
            return False
    
    async def get_read_model_ultra_fast(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get read model with local fallback"""
        try:
            if self.local_mode:
                return await self.get_read_model_local(symbol)
            else:
                return await self.get_read_model_spark(symbol)
        except Exception as e:
            logger.error(f"❌ Error getting read model: {e}")
            return None
    
    async def get_read_model_local(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get read model locally"""
        try:
            # Check cache first
            cache_key = f"read_model_{symbol}"
            if cache_key in self.data_cache:
                return self.data_cache[cache_key]
            
            # Try to load from file
            filename = f"local_data/read_models/{symbol}_read_model.json"
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    data = json.load(f)
                self.data_cache[cache_key] = data
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting local read model: {e}")
            return None
    
    async def get_read_model_spark(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get read model using Spark"""
        try:
            query = f"""
                SELECT *
                FROM read_models
                WHERE symbol = '{symbol}'
                ORDER BY last_updated DESC
                LIMIT 1
            """
            
            df = self.spark.sql(query)
            result = df.collect()
            
            if result:
                row = result[0].asDict()
                if 'predictions_json' in row and row['predictions_json']:
                    row['predictions'] = json.loads(row['predictions_json'])
                return row
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting Spark read model: {e}")
            return None
    
    # ============================================================================
    # MARKET INSIGHTS (LOCAL COMPATIBLE)
    # ============================================================================
    
    async def get_market_insights_realtime(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market insights with local fallback"""
        try:
            insights = {
                'symbols': {},
                'market_summary': {
                    'total_symbols': len(symbols),
                    'avg_change_pct': 0.0,
                    'positive_count': 0,
                    'negative_count': 0
                },
                'generated_at': datetime.now().isoformat(),
                'mode': 'local' if self.local_mode else 'spark'
            }
            
            total_change = 0
            processed_symbols = 0
            
            for symbol in symbols:
                try:
                    # Get read model for each symbol
                    read_model = await self.get_read_model_ultra_fast(symbol)
                    if read_model:
                        symbol_data = {
                            'symbol': symbol,
                            'current_price': read_model.get('current_price', 0.0),
                            'change_pct': read_model.get('price_change_pct', 0.0),
                            'status': read_model.get('status', 'unknown')
                        }
                        
                        insights['symbols'][symbol] = symbol_data
                        
                        change_pct = symbol_data['change_pct']
                        total_change += change_pct
                        processed_symbols += 1
                        
                        if change_pct > 0:
                            insights['market_summary']['positive_count'] += 1
                        else:
                            insights['market_summary']['negative_count'] += 1
                            
                except Exception as e:
                    logger.warning(f"⚠️ Could not get insights for {symbol}: {e}")
            
            if processed_symbols > 0:
                insights['market_summary']['avg_change_pct'] = total_change / processed_symbols
            
            return insights
            
        except Exception as e:
            logger.error(f"❌ Error getting market insights: {e}")
            return {}
    
    # ============================================================================
    # PERFORMANCE MONITORING
    # ============================================================================
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics with local fallback"""
        try:
            base_metrics = {
                'mode': 'local' if self.local_mode else 'spark',
                'optimization_status': 'local_enhanced' if self.local_mode else 'spark_optimized',
                'cache_entries': len(self.data_cache),
                'storage_type': 'local_files' if self.local_mode else 'spark_tables'
            }
            
            if not self.local_mode and self.spark:
                try:
                    spark_metrics = {
                        'spark_version': self.spark.version,
                        'active_jobs': len(self.spark.sparkContext.statusTracker().getActiveJobIds()),
                        'executor_count': len(self.spark.sparkContext.statusTracker().getExecutorInfos())
                    }
                    base_metrics.update(spark_metrics)
                except:
                    pass
            
            return base_metrics
            
        except Exception as e:
            logger.error(f"❌ Error getting performance metrics: {e}")
            return {'mode': 'local', 'status': 'error'}
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.spark:
                self.spark.stop()
            if self.executor:
                self.executor.shutdown(wait=True)
            self.data_cache.clear()
            logger.info("✅ Enhanced data manager cleaned up")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")

# ============================================================================
# ENHANCED STOCK PREDICTOR (LOCAL COMPATIBLE)
# ============================================================================

class WatsonXEnhancedStockPredictor:
    """
    Enhanced Stock Predictor with local development support
    Falls back gracefully when watsonx.data is not available
    """
    
    def __init__(self, symbol='AAPL', finnhub_api_key=None):
        self.symbol = symbol.upper()
        self.finnhub_api_key = finnhub_api_key
        self.data = None
        self.watsonx_manager = WatsonXDataManager()
        self.performance_boost = "enhanced_with_local_fallback"
        
        # Import StockPredictor for fallback
        if STOCK_PREDICTOR_AVAILABLE:
            self.base_predictor = StockPredictor(symbol, finnhub_api_key)
        else:
            self.base_predictor = None
            logger.warning("⚠️ Base StockPredictor not available")
    
    async def fetch_data_optimized(self, days_back=730):
        """Fetch data with local optimization"""
        try:
            logger.info(f"🚀 Fetching optimized data for {self.symbol}")
            
            # Try to get from local cache first
            cached_data = await self.watsonx_manager.get_stock_data_optimized(self.symbol, days_back)
            
            if not cached_data.empty:
                self.data = cached_data
                logger.info(f"⚡ Data retrieved from local cache: {len(self.data)} records")
                return True
            
            # Fallback to base predictor if available
            if self.base_predictor:
                success = self.base_predictor.fetch_data(days_back)
                if success and self.base_predictor.data is not None:
                    self.data = self.base_predictor.data
                    # Store in local cache for future fast access
                    await self.watsonx_manager.store_stock_data_bulk(self.symbol, self.data)
                    logger.info(f"📊 Data fetched and cached locally: {len(self.data)} records")
                return success
            else:
                logger.error("❌ No data source available")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error in optimized data fetch: {e}")
            return False
    
    async def get_technical_indicators_fast(self) -> Dict[str, Any]:
        """Get technical indicators using local computation"""
        try:
            if self.data is not None and not self.data.empty:
                return await self.watsonx_manager.compute_technical_indicators_local(self.symbol, self.data)
            else:
                logger.warning(f"⚠️ No data available for technical indicators: {self.symbol}")
                return {}
        except Exception as e:
            logger.error(f"❌ Error getting technical indicators: {e}")
            return {}
    
    def get_current_data(self):
        """Get current data with enhanced information"""
        try:
            if self.base_predictor and hasattr(self.base_predictor, 'get_current_data'):
                base_data = self.base_predictor.get_current_data()
                if base_data:
                    base_data['data_source'] = 'enhanced_local'
                    base_data['optimization'] = 'local_cache_enabled'
                    return base_data
            
            # Fallback implementation
            if self.data is not None and not self.data.empty:
                current_price = float(self.data['Close'].iloc[-1])
                prev_price = float(self.data['Close'].iloc[-2]) if len(self.data) > 1 else current_price
                
                return {
                    'symbol': self.symbol,
                    'current_price': round(current_price, 2),
                    'change': round(current_price - prev_price, 2),
                    'change_pct': round((current_price - prev_price) / prev_price * 100, 2) if prev_price != 0 else 0,
                    'volume': int(self.data['Volume'].iloc[-1]),
                    'data_source': 'enhanced_local_fallback',
                    'last_updated': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting current data: {e}")
            return None
    
    def train_lstm(self, epochs=5, batch_size=32, validation_split=0.2):
        """Train LSTM using base predictor"""
        if self.base_predictor and hasattr(self.base_predictor, 'train_lstm'):
            self.base_predictor.data = self.data  # Ensure data is available
            return self.base_predictor.train_lstm(epochs, batch_size, validation_split)
        else:
            logger.warning("⚠️ LSTM training not available")
            return False
    
    def predict_lstm(self, days_ahead=30):
        """Generate LSTM predictions using base predictor"""
        if self.base_predictor and hasattr(self.base_predictor, 'predict_lstm'):
            self.base_predictor.data = self.data  # Ensure data is available
            return self.base_predictor.predict_lstm(days_ahead)
        else:
            logger.warning("⚠️ LSTM prediction not available")
            return None
    
    def cleanup(self):
        """Cleanup resources"""
        self.watsonx_manager.cleanup()

# ============================================================================
# USAGE EXAMPLE
# ============================================================================

async def demo_local_enhanced_system():
    """Demonstrate the local enhanced system"""
    print("🚀 Enhanced Stock Prediction System Demo (Local Compatible)")
    print("=" * 70)
    
    try:
        # Test enhanced predictor
        predictor = WatsonXEnhancedStockPredictor('AAPL')
        
        print("📊 Testing enhanced data fetch...")
        success = await predictor.fetch_data_optimized()
        
        if success:
            print(f"✅ Data fetched successfully: {len(predictor.data)} records")
            
            # Get technical indicators
            indicators = await predictor.get_technical_indicators_fast()
            print(f"📈 Technical indicators computed: {len(indicators)} indicators")
            
            # Get current data
            current_data = predictor.get_current_data()
            if current_data:
                print(f"💲 Current price: ${current_data['current_price']}")
                print(f"📊 Data source: {current_data['data_source']}")
        else:
            print("❌ Data fetch failed")
        
        # Test watsonx manager
        manager = predictor.watsonx_manager
        metrics = manager.get_performance_metrics()
        print(f"📊 Performance metrics: {metrics}")
        
        # Cleanup
        predictor.cleanup()
        
        print("\n✅ Demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(demo_local_enhanced_system())