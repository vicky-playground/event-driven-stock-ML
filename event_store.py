# ============================================================================
# COMPLETE EVENT STORE MODULE - Fixed Version Without Circular Imports
# CQRS + Event Sourcing + Kafka implementation for Stock Prediction System
# ============================================================================

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import asyncio
import threading
import time
import logging
import traceback

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka imports with graceful fallback
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    KAFKA_AVAILABLE = True
    logger.info("✅ Kafka components available")
except ImportError:
    logger.warning("⚠️ kafka-python not installed. Running in mock mode.")
    KAFKA_AVAILABLE = False
    
    # Mock classes for when Kafka is not available
    class KafkaProducer:
        def __init__(self, *args, **kwargs):
            logger.info("🔧 Using mock Kafka producer")
        def send(self, *args, **kwargs):
            logger.debug(f"📤 Mock send: {args}")
            return MockFuture()
        def flush(self):
            pass
        def close(self):
            pass
    
    class KafkaConsumer:
        def __init__(self, *args, **kwargs):
            logger.info("🔧 Using mock Kafka consumer")
        def __iter__(self):
            return iter([])
        def close(self):
            pass
    
    class MockFuture:
        def get(self, timeout=None):
            return True
    
    class NoBrokersAvailable(Exception):
        pass

# ============================================================================
# CORE EVENT SOURCING COMPONENTS
# ============================================================================

@dataclass
class Event:
    """Base event class for event sourcing"""
    event_id: str
    event_type: str
    aggregate_id: str
    aggregate_type: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str
    version: int = 1
    
    def __post_init__(self):
        if not self.event_id:
            self.event_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

@dataclass
class Command:
    """Base command class for CQRS"""
    command_id: str
    command_type: str
    aggregate_id: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.command_id:
            self.command_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

@dataclass
class Query:
    """Base query class for CQRS"""
    query_id: str
    query_type: str
    parameters: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str
    
    def __post_init__(self):
        if not self.query_id:
            self.query_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

# ============================================================================
# DOMAIN EVENTS AND COMMANDS
# ============================================================================

class StockAnalysisEvents:
    """Domain events for stock analysis"""
    
    # Analysis Lifecycle Events
    ANALYSIS_REQUESTED = "AnalysisRequested"
    ANALYSIS_STARTED = "AnalysisStarted"
    DATA_FETCHING_STARTED = "DataFetchingStarted"
    DATA_FETCHED = "DataFetched"
    DATA_FETCHING_FAILED = "DataFetchingFailed"
    
    # Model Training Events
    LSTM_TRAINING_STARTED = "LSTMTrainingStarted"
    LSTM_TRAINING_PROGRESSED = "LSTMTrainingProgressed"
    LSTM_TRAINING_COMPLETED = "LSTMTrainingCompleted"
    LSTM_TRAINING_FAILED = "LSTMTrainingFailed"
    
    # Prediction Events
    PREDICTION_STARTED = "PredictionStarted"
    PREDICTION_COMPLETED = "PredictionCompleted"
    PREDICTION_FAILED = "PredictionFailed"
    
    # Analysis Completion Events
    ANALYSIS_COMPLETED = "AnalysisCompleted"
    ANALYSIS_FAILED = "AnalysisFailed"
    
    # Real-time Data Events
    REAL_TIME_DATA_RECEIVED = "RealTimeDataReceived"
    PRICE_ALERT_TRIGGERED = "PriceAlertTriggered"
    
    # User Interaction Events
    USER_SYMBOL_SELECTED = "UserSymbolSelected"
    USER_ANALYSIS_REQUESTED = "UserAnalysisRequested"
    USER_CHART_VIEWED = "UserChartViewed"

class StockAnalysisCommands:
    """Commands for stock analysis"""
    
    # Analysis Commands
    REQUEST_ANALYSIS = "RequestAnalysis"
    CANCEL_ANALYSIS = "CancelAnalysis"
    RETRY_ANALYSIS = "RetryAnalysis"
    
    # Data Commands
    FETCH_HISTORICAL_DATA = "FetchHistoricalData"
    FETCH_REAL_TIME_DATA = "FetchRealTimeData"
    VALIDATE_SYMBOL = "ValidateSymbol"
    
    # Model Commands
    TRAIN_LSTM_MODEL = "TrainLSTMModel"
    GENERATE_PREDICTIONS = "GeneratePredictions"
    UPDATE_MODEL_PARAMETERS = "UpdateModelParameters"
    
    # User Commands
    SELECT_SYMBOL = "SelectSymbol"
    SET_PRICE_ALERT = "SetPriceAlert"
    REQUEST_CHART_DATA = "RequestChartData"

class StockAnalysisQueries:
    """Queries for stock analysis"""
    
    # Analysis Queries
    GET_ANALYSIS_STATUS = "GetAnalysisStatus"
    GET_ANALYSIS_RESULTS = "GetAnalysisResults"
    GET_ANALYSIS_HISTORY = "GetAnalysisHistory"
    
    # Data Queries
    GET_CURRENT_PRICE = "GetCurrentPrice"
    GET_HISTORICAL_DATA = "GetHistoricalData"
    GET_CHART_DATA = "GetChartData"
    
    # Model Queries
    GET_MODEL_PERFORMANCE = "GetModelPerformance"
    GET_PREDICTIONS = "GetPredictions"
    GET_TRAINING_METRICS = "GetTrainingMetrics"
    
    # System Queries
    GET_SYSTEM_STATUS = "GetSystemStatus"
    GET_SUPPORTED_SYMBOLS = "GetSupportedSymbols"

# ============================================================================
# EVENT STORE IMPLEMENTATION
# ============================================================================

class EventStore:
    """Event Store implementation using Kafka as the backbone"""
    
    def __init__(self, kafka_servers=['localhost:9092']):
        self.kafka_servers = kafka_servers
        self.producer = None
        self.event_topic = 'event-store'
        self.command_topic = 'commands'
        self.query_topic = 'queries'
        self.notification_topic = 'notifications'
        
        # Event handlers registry
        self.event_handlers = {}
        self.command_handlers = {}
        self.query_handlers = {}
        
        self.connect_kafka()
    
    def connect_kafka(self):
        """Connect to Kafka with retry logic"""
        if not KAFKA_AVAILABLE:
            logger.warning("⚠️ Kafka not available, using mock producer")
            self.producer = KafkaProducer()  # Mock producer
            return
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type='gzip'
            )
            logger.info("✅ Event Store connected to Kafka")
        except Exception as e:
            logger.warning(f"⚠️ Event Store Kafka connection failed: {e}. Using mock producer.")
            self.producer = KafkaProducer()  # Mock producer
    
    async def append_event(self, event: Event) -> bool:
        """Append event to the event store"""
        try:
            event_data = asdict(event)
            
            # Send to event store topic
            future = self.producer.send(
                self.event_topic, 
                key=event.aggregate_id,
                value=event_data
            )
            
            if KAFKA_AVAILABLE:
                future.get(timeout=5)
            
            logger.info(f"📝 Event stored: {event.event_type} for {event.aggregate_id}")
            
            # Trigger event handlers asynchronously
            await self.dispatch_event(event)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing event: {e}")
            return False
    
    async def dispatch_event(self, event: Event):
        """Dispatch event to registered handlers"""
        handlers = self.event_handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"❌ Event handler error for {event.event_type}: {e}")
    
    def register_event_handler(self, event_type: str, handler: Callable):
        """Register event handler"""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
        logger.info(f"📋 Registered handler for {event_type}")
    
    def register_command_handler(self, command_type: str, handler: Callable):
        """Register command handler"""
        self.command_handlers[command_type] = handler
        logger.info(f"⚡ Registered command handler for {command_type}")
    
    def register_query_handler(self, query_type: str, handler: Callable):
        """Register query handler"""
        self.query_handlers[query_type] = handler
        logger.info(f"🔍 Registered query handler for {query_type}")

# ============================================================================
# AGGREGATE ROOT
# ============================================================================

class StockAnalysisAggregate:
    """Stock Analysis Aggregate with event sourcing"""
    
    def __init__(self, symbol: str):
        self.aggregate_id = symbol
        self.symbol = symbol
        self.version = 0
        self.status = "idle"
        self.current_analysis_id = None
        self.last_analysis_timestamp = None
        self.training_progress = {}
        self.predictions = None
        self.current_data = None
        self.error_messages = []
        
        # Event sourcing
        self.uncommitted_events = []
    
    def request_analysis(self, requester: str, options: Dict[str, Any] = None) -> Event:
        """Handle analysis request command"""
        analysis_id = str(uuid.uuid4())
        
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.ANALYSIS_REQUESTED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "analysis_id": analysis_id,
                "symbol": self.symbol,
                "requester": requester,
                "options": options or {},
                "previous_status": self.status
            },
            metadata={
                "requester": requester,
                "request_timestamp": datetime.now(timezone.utc).isoformat()
            },
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def start_analysis(self, analysis_id: str) -> Event:
        """Handle analysis start"""
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.ANALYSIS_STARTED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "analysis_id": analysis_id,
                "symbol": self.symbol
            },
            metadata={},
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def complete_data_fetch(self, data_info: Dict[str, Any]) -> Event:
        """Handle successful data fetch"""
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.DATA_FETCHED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "symbol": self.symbol,
                "data_info": data_info
            },
            metadata={},
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def complete_lstm_training(self, metrics: Dict[str, Any]) -> Event:
        """Handle LSTM training completion"""
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.LSTM_TRAINING_COMPLETED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "symbol": self.symbol,
                "metrics": metrics,
                "analysis_id": self.current_analysis_id
            },
            metadata={},
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def complete_predictions(self, predictions: Dict[str, Any]) -> Event:
        """Handle prediction completion"""
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.PREDICTION_COMPLETED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "symbol": self.symbol,
                "predictions": predictions,
                "analysis_id": self.current_analysis_id
            },
            metadata={},
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def complete_analysis(self, results: Dict[str, Any]) -> Event:
        """Handle analysis completion"""
        event = Event(
            event_id="",
            event_type=StockAnalysisEvents.ANALYSIS_COMPLETED,
            aggregate_id=self.aggregate_id,
            aggregate_type="StockAnalysis",
            data={
                "symbol": self.symbol,
                "analysis_id": self.current_analysis_id,
                "results": results,
                "completion_timestamp": datetime.now(timezone.utc).isoformat()
            },
            metadata={},
            timestamp=""
        )
        
        self.apply_event(event)
        return event
    
    def apply_event(self, event: Event):
        """Apply event to aggregate state"""
        if event.event_type == StockAnalysisEvents.ANALYSIS_REQUESTED:
            self.status = "requested"
            self.current_analysis_id = event.data["analysis_id"]
            
        elif event.event_type == StockAnalysisEvents.ANALYSIS_STARTED:
            self.status = "running"
            
        elif event.event_type == StockAnalysisEvents.DATA_FETCHED:
            self.current_data = event.data["data_info"]
            
        elif event.event_type == StockAnalysisEvents.LSTM_TRAINING_COMPLETED:
            self.training_progress["lstm"] = "completed"
            
        elif event.event_type == StockAnalysisEvents.PREDICTION_COMPLETED:
            self.predictions = event.data["predictions"]
            
        elif event.event_type == StockAnalysisEvents.ANALYSIS_COMPLETED:
            self.status = "completed"
            self.last_analysis_timestamp = event.data["completion_timestamp"]
        
        self.version += 1
        self.uncommitted_events.append(event)
    
    def get_uncommitted_events(self) -> List[Event]:
        """Get uncommitted events"""
        return self.uncommitted_events.copy()
    
    def mark_events_as_committed(self):
        """Mark events as committed"""
        self.uncommitted_events.clear()

# ============================================================================
# ENHANCED COMMAND HANDLER WITH IMMEDIATE PROCESSING
# ============================================================================

class StockAnalysisCommandHandler:
    """Enhanced command handler with immediate processing and debugging"""
    
    def __init__(self, event_store: EventStore, stock_predictor_factory: Callable):
        self.event_store = event_store
        self.stock_predictor_factory = stock_predictor_factory
        self.aggregates = {}  # In-memory aggregate cache
        self.processing_status = {}  # Track processing status for debugging
        
        # Register command handlers
        self.register_handlers()
        logger.info("✅ Enhanced command handler initialized")
    
    def register_handlers(self):
        """Register all command handlers"""
        self.event_store.register_command_handler(
            StockAnalysisCommands.REQUEST_ANALYSIS, 
            self.handle_request_analysis
        )
        self.event_store.register_command_handler(
            StockAnalysisCommands.FETCH_HISTORICAL_DATA, 
            self.handle_fetch_data
        )
        self.event_store.register_command_handler(
            StockAnalysisCommands.TRAIN_LSTM_MODEL, 
            self.handle_train_lstm
        )
        self.event_store.register_command_handler(
            StockAnalysisCommands.GENERATE_PREDICTIONS, 
            self.handle_generate_predictions
        )
    
    async def handle_request_analysis(self, command: Command):
        """Handle analysis request with immediate processing"""
        symbol = command.aggregate_id
        logger.info(f"🚀 STARTING immediate analysis for {symbol}")
        
        try:
            # Track processing status
            self.processing_status[symbol] = {
                'status': 'started',
                'step': 'request_received',
                'timestamp': datetime.now().isoformat(),
                'error': None
            }
            
            # Get or create aggregate
            aggregate = self.get_aggregate(symbol)
            
            # Generate analysis requested event
            event = aggregate.request_analysis(
                command.metadata.get("requester", "system"),
                command.data.get("options", {})
            )
            
            # Store event
            await self.event_store.append_event(event)
            
            # Update processing status
            self.processing_status[symbol].update({
                'step': 'event_stored',
                'analysis_id': event.data["analysis_id"]
            })
            
            # Start immediate workflow
            await self.execute_immediate_workflow(symbol, event.data["analysis_id"])
            
        except Exception as e:
            logger.error(f"❌ Error in analysis request for {symbol}: {e}")
            self.processing_status[symbol] = {
                'status': 'error',
                'step': 'request_failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            raise
    
    async def execute_immediate_workflow(self, symbol: str, analysis_id: str):
        """Execute complete workflow immediately"""
        logger.info(f"🔄 Starting immediate workflow for {symbol}")
        
        try:
            aggregate = self.get_aggregate(symbol)
            
            # Generate analysis started event
            start_event = aggregate.start_analysis(analysis_id)
            await self.event_store.append_event(start_event)
            
            # STEP 1: Fetch data
            logger.info(f"📊 STEP 1: Fetching data for {symbol}")
            self.processing_status[symbol].update({'step': 'fetching_data'})
            
            predictor = self.stock_predictor_factory(symbol)
            success = predictor.fetch_data()
            
            if not success:
                raise Exception("Data fetching failed")
            
            # Store data fetch event
            data_info = {
                "data_points": len(predictor.data),
                "date_range": {
                    "start": predictor.data.index[0].isoformat(),
                    "end": predictor.data.index[-1].isoformat()
                },
                "columns": list(predictor.data.columns)
            }
            event = aggregate.complete_data_fetch(data_info)
            await self.event_store.append_event(event)
            
            # STEP 2: Train LSTM
            logger.info(f"🧠 STEP 2: Training LSTM for {symbol}")
            self.processing_status[symbol].update({'step': 'training_lstm'})
            
            # Generate training started event
            await self.event_store.append_event(Event(
                event_id="",
                event_type=StockAnalysisEvents.LSTM_TRAINING_STARTED,
                aggregate_id=symbol,
                aggregate_type="StockAnalysis",
                data={"symbol": symbol, "analysis_id": analysis_id},
                metadata={},
                timestamp=""
            ))
            
            success = predictor.train_lstm(epochs=5, batch_size=16)  # Reduced for speed
            
            if not success:
                raise Exception("LSTM training failed")
            
            # Store training completion event
            metrics = getattr(predictor, 'last_training_metrics', {}).get('lstm', {})
            event = aggregate.complete_lstm_training(metrics)
            await self.event_store.append_event(event)
            
            # STEP 3: Generate predictions
            logger.info(f"📈 STEP 3: Generating predictions for {symbol}")
            self.processing_status[symbol].update({'step': 'generating_predictions'})
            
            # Generate prediction started event
            await self.event_store.append_event(Event(
                event_id="",
                event_type=StockAnalysisEvents.PREDICTION_STARTED,
                aggregate_id=symbol,
                aggregate_type="StockAnalysis",
                data={"symbol": symbol, "analysis_id": analysis_id},
                metadata={},
                timestamp=""
            ))
            
            lstm_pred = predictor.predict_lstm(30)
            
            if lstm_pred is None:
                raise Exception("Prediction generation failed")
            
            # Store predictions
            predictions = {
                'lstm_predictions': {
                    'dates': lstm_pred['Date'].dt.strftime('%Y-%m-%d').tolist(),
                    'prices': lstm_pred['LSTM_Prediction'].round(2).tolist(),
                    'lower_bound': lstm_pred.get('LSTM_Lower', lstm_pred['LSTM_Prediction']).round(2).tolist(),
                    'upper_bound': lstm_pred.get('LSTM_Upper', lstm_pred['LSTM_Prediction']).round(2).tolist()
                },
                'prophet_predictions': None
            }
            
            event = aggregate.complete_predictions(predictions)
            await self.event_store.append_event(event)
            
            # STEP 4: Complete analysis
            logger.info(f"✅ STEP 4: Completing analysis for {symbol}")
            self.processing_status[symbol].update({'step': 'completing_analysis'})
            
            # Get current data
            current_data = predictor.get_current_data()
            if not current_data:
                current_data = {
                    'symbol': symbol,
                    'current_price': 100.0,
                    'change': 0.0,
                    'change_pct': 0.0,
                    'volume': 1000000,
                    'source': 'fallback'
                }
            
            # Compile results
            results = {
                'request_id': analysis_id,
                'symbol': symbol,
                'status': 'completed',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'model_type': 'immediate_event_driven_lstm',
                'data': current_data,
                'predictions': aggregate.predictions,
                'models_trained': {
                    'lstm': aggregate.training_progress.get('lstm') == 'completed',
                    'prophet': False
                }
            }
            
            # Generate analysis completed event
            event = aggregate.complete_analysis(results)
            await self.event_store.append_event(event)
            
            # Final status update
            self.processing_status[symbol].update({
                'status': 'completed',
                'step': 'workflow_completed',
                'completed_at': datetime.now().isoformat()
            })
            
            logger.info(f"🎉 IMMEDIATE WORKFLOW COMPLETED for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Immediate workflow error for {symbol}: {e}")
            
            self.processing_status[symbol].update({
                'status': 'error',
                'step': 'workflow_failed',
                'error': str(e),
                'failed_at': datetime.now().isoformat()
            })
            
            # Generate failure event
            await self.event_store.append_event(Event(
                event_id="",
                event_type=StockAnalysisEvents.ANALYSIS_FAILED,
                aggregate_id=symbol,
                aggregate_type="StockAnalysis",
                data={'symbol': symbol, 'error': str(e), 'analysis_id': analysis_id},
                metadata={},
                timestamp=""
            ))
            raise
    
    def get_processing_status(self, symbol: str) -> Dict[str, Any]:
        """Get current processing status for debugging"""
        return self.processing_status.get(symbol, {
            'status': 'not_found',
            'step': 'unknown'
        })
    
    def get_aggregate(self, symbol: str) -> StockAnalysisAggregate:
        """Get or create aggregate"""
        if symbol not in self.aggregates:
            self.aggregates[symbol] = StockAnalysisAggregate(symbol)
        return self.aggregates[symbol]
    
    # Placeholder implementations for other handlers
    async def handle_fetch_data(self, command: Command):
        """Handle fetch data command"""
        pass
    
    async def handle_train_lstm(self, command: Command):
        """Handle train LSTM command"""
        pass
    
    async def handle_generate_predictions(self, command: Command):
        """Handle generate predictions command"""
        pass

# ============================================================================
# QUERY HANDLER (CQRS READ SIDE)
# ============================================================================

class StockAnalysisQueryHandler:
    """Event-driven query handler for stock analysis (CQRS Read Side)"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self.read_models = {}  # Optimized read models
        
        # Register query handlers
        self.register_handlers()
        
        # Register for events to maintain read models
        self.register_event_projections()
    
    def register_handlers(self):
        """Register query handlers"""
        self.event_store.register_query_handler(
            StockAnalysisQueries.GET_ANALYSIS_STATUS,
            self.handle_get_analysis_status
        )
        self.event_store.register_query_handler(
            StockAnalysisQueries.GET_ANALYSIS_RESULTS,
            self.handle_get_analysis_results
        )
        self.event_store.register_query_handler(
            StockAnalysisQueries.GET_SYSTEM_STATUS,
            self.handle_get_system_status
        )
    
    def register_event_projections(self):
        """Register event handlers for maintaining read models"""
        self.event_store.register_event_handler(
            StockAnalysisEvents.ANALYSIS_COMPLETED,
            self.project_analysis_completed
        )
        self.event_store.register_event_handler(
            StockAnalysisEvents.ANALYSIS_STARTED,
            self.project_analysis_started
        )
        self.event_store.register_event_handler(
            StockAnalysisEvents.LSTM_TRAINING_COMPLETED,
            self.project_training_completed
        )
    
    async def handle_get_analysis_status(self, query: Query) -> Dict[str, Any]:
        """Handle analysis status query"""
        symbol = query.parameters.get('symbol')
        
        read_model = self.read_models.get(symbol, {})
        
        return {
            'symbol': symbol,
            'status': read_model.get('status', 'not_found'),
            'analysis_id': read_model.get('analysis_id'),
            'progress': read_model.get('progress', {}),
            'last_updated': read_model.get('last_updated'),
            'error_messages': read_model.get('error_messages', [])
        }
    
    async def handle_get_analysis_results(self, query: Query) -> Dict[str, Any]:
        """Handle analysis results query"""
        symbol = query.parameters.get('symbol')
        
        read_model = self.read_models.get(symbol, {})
        
        if read_model.get('status') == 'completed':
            return {
                'status': 'success',
                'results': read_model.get('results')
            }
        elif read_model.get('status') in ['running', 'requested']:
            return {
                'status': 'not_ready',
                'message': f'Analysis for {symbol} is still running'
            }
        else:
            return {
                'status': 'not_found',
                'message': f'No analysis results for {symbol}'
            }
    
    async def handle_get_system_status(self, query: Query) -> Dict[str, Any]:
        """Handle system status query"""
        active_analyses = sum(1 for rm in self.read_models.values() 
                            if rm.get('status') in ['running', 'requested'])
        
        completed_analyses = sum(1 for rm in self.read_models.values() 
                               if rm.get('status') == 'completed')
        
        return {
            'event_driven_system': True,
            'kafka_connected': self.event_store.producer is not None,
            'active_analyses': active_analyses,
            'completed_analyses': completed_analyses,
            'total_symbols': len(self.read_models),
            'model_type': 'Event-Driven LSTM System',
            'architecture': 'CQRS + Event Sourcing'
        }
    
    async def project_analysis_completed(self, event: Event):
        """Project analysis completed event to read model"""
        symbol = event.aggregate_id
        
        if symbol not in self.read_models:
            self.read_models[symbol] = {}
        
        self.read_models[symbol].update({
            'status': 'completed',
            'analysis_id': event.data['analysis_id'],
            'results': event.data['results'],
            'completion_timestamp': event.data['completion_timestamp'],
            'last_updated': event.timestamp
        })
        
        logger.info(f"📊 Read model updated for {symbol} - analysis completed")
    
    async def project_analysis_started(self, event: Event):
        """Project analysis started event to read model"""
        symbol = event.aggregate_id
        
        if symbol not in self.read_models:
            self.read_models[symbol] = {}
        
        self.read_models[symbol].update({
            'status': 'running',
            'analysis_id': event.data['analysis_id'],
            'start_timestamp': event.timestamp,
            'last_updated': event.timestamp,
            'progress': {
                'data_fetch': 'pending',
                'lstm_training': 'pending',
                'prediction_generation': 'pending'
            }
        })
        
        logger.info(f"🚀 Read model updated for {symbol} - analysis started")
    
    async def project_training_completed(self, event: Event):
        """Project training completed event to read model"""
        symbol = event.aggregate_id
        
        if symbol in self.read_models:
            if 'progress' not in self.read_models[symbol]:
                self.read_models[symbol]['progress'] = {}
            
            self.read_models[symbol]['progress']['lstm_training'] = 'completed'
            self.read_models[symbol]['last_updated'] = event.timestamp
        
        logger.info(f"🧠 Read model updated for {symbol} - LSTM training completed")

# ============================================================================
# MAIN EVENT-DRIVEN SERVICE
# ============================================================================

class EventDrivenStockAnalysisService:
    """Main event-driven application service with immediate processing"""
    
    def __init__(self, kafka_servers=['localhost:9092']):
        self.event_store = EventStore(kafka_servers)
        self.command_handler = None
        self.query_handler = None
        self.running = False
        
        logger.info("🚀 Event-driven service initializing...")
    
    def initialize(self, stock_predictor_factory: Callable):
        """Initialize the event-driven system"""
        # Initialize CQRS handlers
        self.command_handler = StockAnalysisCommandHandler(
            self.event_store, 
            stock_predictor_factory
        )
        self.query_handler = StockAnalysisQueryHandler(self.event_store)
        
        logger.info("✅ Event-driven stock analysis service initialized")
    
    async def start_consuming(self):
        """Start consuming (immediate processing mode)"""
        self.running = True
        
        if not KAFKA_AVAILABLE:
            logger.warning("⚠️ Kafka not available, running in immediate processing mode")
        else:
            logger.info("📡 Running in immediate processing mode")
        
        # Keep service running
        while self.running:
            await asyncio.sleep(1)
    
    def stop(self):
        """Stop the event-driven service"""
        self.running = False
        logger.info("🛑 Event-driven service stopped")
    
    # Command API
    async def request_analysis(self, symbol: str, requester: str = "api") -> str:
        """Request analysis via immediate command processing"""
        logger.info(f"📤 REQUEST ANALYSIS for {symbol} by {requester}")
        
        command = Command(
            command_id="",
            command_type=StockAnalysisCommands.REQUEST_ANALYSIS,
            aggregate_id=symbol.upper(),
            data={"symbol": symbol.upper()},
            metadata={"requester": requester},
            timestamp=""
        )
        
        # Process immediately
        if self.command_handler:
            try:
                await self.command_handler.handle_request_analysis(command)
                logger.info(f"✅ Analysis completed for {symbol}: {command.command_id}")
                return command.command_id
            except Exception as e:
                logger.error(f"❌ Error processing analysis for {symbol}: {e}")
                return None
        else:
            logger.error("❌ No command handler available")
            return None
    
    # Query API  
    async def get_analysis_status(self, symbol: str) -> Dict[str, Any]:
        """Get analysis status via query"""
        if self.query_handler:
            result = await self.query_handler.handle_get_analysis_status(Query(
                query_id="",
                query_type=StockAnalysisQueries.GET_ANALYSIS_STATUS,
                parameters={"symbol": symbol.upper()},
                metadata={},
                timestamp=""
            ))
            
            # Add processing status if available
            if self.command_handler:
                processing_status = self.command_handler.get_processing_status(symbol.upper())
                result['processing_status'] = processing_status
            
            return result
        else:
            return {'status': 'not_found', 'message': 'Query handler not available'}
    
    async def get_analysis_results(self, symbol: str) -> Dict[str, Any]:
        """Get analysis results via query"""
        if self.query_handler:
            return await self.query_handler.handle_get_analysis_results(Query(
                query_id="",
                query_type=StockAnalysisQueries.GET_ANALYSIS_RESULTS,
                parameters={"symbol": symbol.upper()},
                metadata={},
                timestamp=""
            ))
        else:
            return {'status': 'not_found', 'message': 'Query handler not available'}
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get system status via query"""
        if self.query_handler:
            status = await self.query_handler.handle_get_system_status(Query(
                query_id="",
                query_type=StockAnalysisQueries.GET_SYSTEM_STATUS,
                parameters={},
                metadata={},
                timestamp=""
            ))
            
            # Add processing statistics
            if self.command_handler:
                processing_stats = {}
                for symbol, status_info in self.command_handler.processing_status.items():
                    processing_stats[symbol] = status_info
                
                status['processing_statistics'] = processing_stats
                status['total_processing'] = len(processing_stats)
            
            return status
        else:
            return {'event_driven_system': False, 'message': 'System not initialized'}

# ============================================================================
# DEBUGGING UTILITIES
# ============================================================================

def debug_event_workflow(symbol: str, service):
    """Debug utility to check workflow status"""
    print(f"\n🔍 DEBUGGING WORKFLOW FOR {symbol}")
    print("=" * 50)
    
    if hasattr(service, 'command_handler') and service.command_handler:
        status = service.command_handler.get_processing_status(symbol)
        print(f"Processing Status: {status}")
        
        if symbol in service.command_handler.aggregates:
            aggregate = service.command_handler.aggregates[symbol]
            print(f"Aggregate Status: {aggregate.status}")
            print(f"Training Progress: {aggregate.training_progress}")
            print(f"Has Predictions: {aggregate.predictions is not None}")
        else:
            print("No aggregate found")
    else:
        print("No command handler available")
    
    print("=" * 50)

# ============================================================================
# EXAMPLE USAGE AND TESTING
# ============================================================================

if __name__ == "__main__":
    print("🧪 Testing Fixed Event Store Components...")
    
    # Test basic event store
    event_store = EventStore()
    
    # Test event creation
    test_event = Event(
        event_id="",
        event_type="TestEvent",
        aggregate_id="TEST-001",
        aggregate_type="Test",
        data={"message": "Hello Fixed Event Store!"},
        metadata={"test": True},
        timestamp=""
    )
    
    print(f"✅ Created test event: {test_event.event_id}")
    print(f"📅 Event timestamp: {test_event.timestamp}")
    
    # Test aggregate
    aggregate = StockAnalysisAggregate("AAPL")
    event = aggregate.request_analysis("test_user")
    
    print(f"✅ Created aggregate event: {event.event_type}")
    print(f"📊 Aggregate status: {aggregate.status}")
    
    print("🎉 Fixed event store components test completed!")