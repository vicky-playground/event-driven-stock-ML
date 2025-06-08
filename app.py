
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import json
import plotly
import plotly.graph_objs as go
from plotly.utils import PlotlyJSONEncoder
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
import os
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Import existing modules for backward compatibility
from stock_predictor import StockPredictor

# Try to import event-driven components
try:
    from event_store import (
        EventDrivenStockAnalysisService,
        StockAnalysisCommands,
        StockAnalysisQueries,
        Command,
        Query
    )
    EVENT_DRIVEN_AVAILABLE = True
    print("✅ Standard event-driven components available")
except ImportError:
    EVENT_DRIVEN_AVAILABLE = False
    print("⚠️ Event-driven components not available")

# Try to import enhanced components
try:
    from debug_event_workflow_fix import (
        EnhancedEventDrivenStockAnalysisService,
        debug_event_workflow
    )
    ENHANCED_AVAILABLE = True
    print("✅ Enhanced event workflow available")
except ImportError:
    print("⚠️ Enhanced workflow not available, using standard version")
    ENHANCED_AVAILABLE = False

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Global event-driven service
event_service = None
event_store = None
executor = ThreadPoolExecutor(max_workers=10)

def run_async(coro):
    """Helper to run async functions in sync context"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def create_stock_predictor(symbol):
    """Factory function for creating stock predictors"""
    finnhub_api_key = os.getenv('FINNHUB_API_KEY')
    return StockPredictor(symbol, finnhub_api_key)

# ============================================================================
# ENHANCED EVENT-DRIVEN INITIALIZATION (FIXED)
# ============================================================================

def initialize_event_driven_system():
    """Initialize the enhanced event-driven system"""
    global event_service, event_store
    
    if not EVENT_DRIVEN_AVAILABLE:
        logger.warning("⚠️ Event-driven components not available")
        return False
    
    try:
        # Use enhanced service if available, otherwise use standard
        if ENHANCED_AVAILABLE:
            print("🚀 Initializing ENHANCED event-driven system...")
            kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
            event_service = EnhancedEventDrivenStockAnalysisService(kafka_servers)
            print("✅ Enhanced service created with immediate processing")
        else:
            # Fallback to standard service
            print("🚀 Initializing standard event-driven system...")
            kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
            event_service = EventDrivenStockAnalysisService(kafka_servers)
            print("✅ Standard service created")
        
        event_store = event_service.event_store
        
        # Initialize with stock predictor factory
        event_service.initialize(create_stock_predictor)
        
        # Start consuming in background thread
        def start_consumers():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(event_service.start_consuming())
        
        consumer_thread = threading.Thread(target=start_consumers, daemon=True)
        consumer_thread.start()
        
        logger.info("✅ Event-driven system fully initialized")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize event-driven system: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# ENHANCED DEBUG ENDPOINTS
# ============================================================================

@app.route('/api/debug/workflow/<symbol>')
def debug_workflow_status(symbol):
    """Debug endpoint to check workflow status"""
    symbol = symbol.upper()
    
    try:
        if not event_service:
            return jsonify({
                'status': 'error',
                'message': 'Event service not available'
            })
        
        # Get detailed processing status
        debug_info = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'service_type': 'enhanced' if ENHANCED_AVAILABLE else 'standard'
        }
        
        # Enhanced debugging for enhanced service
        if ENHANCED_AVAILABLE and hasattr(event_service, 'processing_status'):
            processing_status = event_service.processing_status.get(symbol, {})
            debug_info['processing_status'] = processing_status
            
            # Get command handler status
            if hasattr(event_service, 'command_handler') and event_service.command_handler:
                if hasattr(event_service.command_handler, 'aggregates'):
                    if symbol in event_service.command_handler.aggregates:
                        aggregate = event_service.command_handler.aggregates[symbol]
                        debug_info['aggregate'] = {
                            'status': aggregate.status,
                            'training_progress': aggregate.training_progress,
                            'has_predictions': aggregate.predictions is not None,
                            'current_analysis_id': aggregate.current_analysis_id,
                            'version': aggregate.version
                        }
            
            # Get query handler status
            if hasattr(event_service, 'query_handler') and event_service.query_handler:
                if hasattr(event_service.query_handler, 'read_models'):
                    read_model = event_service.query_handler.read_models.get(symbol, {})
                    debug_info['read_model'] = read_model
        
        return jsonify({
            'status': 'success',
            'debug_info': debug_info
        })
        
    except Exception as e:
        logger.error(f"❌ Error in debug workflow: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc()
        })

@app.route('/api/debug/process_immediate/<symbol>')
def process_immediate(symbol):
    """Manually trigger immediate processing for debugging"""
    symbol = symbol.upper()
    
    try:
        if not event_service:
            return jsonify({
                'status': 'error',
                'message': 'Event service not available'
            })
        
        # Trigger immediate processing
        async def immediate_process():
            try:
                command_id = await event_service.request_analysis(symbol, 'debug_immediate')
                return command_id
            except Exception as e:
                logger.error(f"❌ Immediate processing error: {e}")
                return None
        
        # Run the async function
        command_id = run_async(immediate_process())
        
        if command_id:
            return jsonify({
                'status': 'success',
                'message': f'Immediate processing triggered for {symbol}',
                'command_id': command_id,
                'service_type': 'enhanced' if ENHANCED_AVAILABLE else 'standard'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': f'Failed to trigger immediate processing for {symbol}'
            })
            
    except Exception as e:
        logger.error(f"❌ Error in immediate processing: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

# ============================================================================
# CORE EVENT-DRIVEN API ENDPOINTS (FIXED)
# ============================================================================

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/status')
def get_api_status():
    """Get system status via event-driven query"""
    try:
        if event_service:
            # Use async query handler
            future = executor.submit(run_async, event_service.get_system_status())
            status = future.result(timeout=15)
            
            return jsonify({
                **status,
                'message': 'Enhanced event-driven CQRS system operational' if ENHANCED_AVAILABLE else 'Event-driven CQRS system operational',
                'architecture': 'Enhanced Event Sourcing + CQRS + Kafka' if ENHANCED_AVAILABLE else 'Event Sourcing + CQRS + Kafka',
                'immediate_processing': ENHANCED_AVAILABLE
            })
        else:
            return jsonify({
                'event_driven_system': False,
                'message': 'Event system not initialized',
                'architecture': 'Fallback mode'
            })
    except Exception as e:
        logger.error(f"❌ Error getting system status: {e}")
        return jsonify({
            'event_driven_system': False,
            'message': f'System status error: {str(e)}',
            'error': True
        })

@app.route('/api/analyze/<symbol>')
def analyze_complete_event_driven(symbol):
    """Trigger event-driven analysis via command"""
    symbol = symbol.upper()
    
    if not event_service:
        return jsonify({
            'status': 'error',
            'message': 'Event-driven system not available'
        })
    
    try:
        # Send command via event-driven service with longer timeout for training
        future = executor.submit(
            run_async, 
            event_service.request_analysis(symbol, 'api')
        )
        command_id = future.result(timeout=300)  # 5 minutes timeout for training
        
        if command_id:
            logger.info(f"📤 Analysis command sent for {symbol}: {command_id}")
            return jsonify({
                'status': 'success',
                'message': f'Enhanced event-driven analysis completed for {symbol}' if ENHANCED_AVAILABLE else f'Event-driven analysis requested for {symbol}',
                'command_id': command_id,
                'symbol': symbol,
                'architecture': 'Enhanced Event-driven CQRS' if ENHANCED_AVAILABLE else 'Event-driven CQRS',
                'immediate_processing': ENHANCED_AVAILABLE,
                'note': 'Analysis completed via immediate processing' if ENHANCED_AVAILABLE else 'Analysis will complete via event sourcing workflow'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to send analysis command'
            })
            
    except Exception as e:
        logger.error(f"❌ Error triggering event-driven analysis: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f'Event-driven analysis error: {str(e)}'
        })

@app.route('/api/results/<symbol>')
def get_analysis_results_event_driven(symbol):
    """Get analysis results via event-driven query"""
    symbol = symbol.upper()
    
    if not event_service:
        return jsonify({
            'status': 'error',
            'message': 'Event-driven system not available'
        })
    
    try:
        # Query via event-driven service
        future = executor.submit(
            run_async, 
            event_service.get_analysis_results(symbol)
        )
        result = future.result(timeout=15)
        
        logger.info(f"📊 Query result for {symbol}: {result.get('status')}")
        
        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'results': result['results'],
                'architecture': 'Enhanced Event-driven CQRS read model' if ENHANCED_AVAILABLE else 'Event-driven CQRS read model',
                'immediate_processing': result.get('immediate_mode', False)
            })
        elif result['status'] == 'not_ready':
            return jsonify({
                'status': 'not_ready',
                'message': result['message'],
                'architecture': 'Enhanced Event-driven CQRS' if ENHANCED_AVAILABLE else 'Event-driven CQRS'
            })
        else:
            return jsonify({
                'status': 'not_found',
                'message': result['message'],
                'architecture': 'Enhanced Event-driven CQRS' if ENHANCED_AVAILABLE else 'Event-driven CQRS'
            })
            
    except Exception as e:
        logger.error(f"❌ Error getting event-driven results: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Event-driven query error: {str(e)}'
        })

@app.route('/api/status/<symbol>')
def get_analysis_status_event_driven(symbol):
    """Get analysis status via event-driven query"""
    symbol = symbol.upper()
    
    if not event_service:
        return jsonify({
            'status': 'error',
            'message': 'Event-driven system not available'
        })
    
    try:
        # Query analysis status
        future = executor.submit(
            run_async, 
            event_service.get_analysis_status(symbol)
        )
        status = future.result(timeout=15)
        
        return jsonify({
            'status': 'success',
            'analysis_status': status,
            'architecture': 'Enhanced Event-driven CQRS read model' if ENHANCED_AVAILABLE else 'Event-driven CQRS read model'
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting analysis status: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Status query error: {str(e)}'
        })

@app.route('/api/chart/<symbol>')
def get_chart_data_event_driven(symbol):
    """Get chart data with event-driven fallback"""
    symbol = symbol.upper()
    
    try:
        # First try to get results from event-driven system
        if event_service:
            future = executor.submit(
                run_async, 
                event_service.get_analysis_results(symbol)
            )
            result = future.result(timeout=15)
            
            if result['status'] == 'success':
                return create_chart_from_results(symbol, result['results'])
        
        # Fallback to direct chart generation
        return create_fallback_chart(symbol)
        
    except Exception as e:
        logger.error(f"❌ Error creating event-driven chart: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Chart generation error: {str(e)}'
        })

def create_chart_from_results(symbol, results):
    """Create chart from event-driven analysis results"""
    try:
        # Extract data from event-driven results
        data = results.get('data', {})
        predictions = results.get('predictions', {})
        
        # Create enhanced Plotly chart
        fig = go.Figure()
        
        # Try to get historical data from predictor
        try:
            predictor = create_stock_predictor(symbol)
            if predictor.fetch_data():
                historical = predictor.get_historical_data(200)
                
                if historical:
                    # Add historical price line
                    fig.add_trace(go.Scatter(
                        x=historical['dates'],
                        y=historical['prices'],
                        mode='lines',
                        name='Historical Price',
                        line=dict(color='#2E86C1', width=3),
                        hovertemplate='<b>%{x}</b><br>Price: $%{y:.2f}<extra></extra>'
                    ))
        except Exception as e:
            logger.warning(f"⚠️ Could not add historical data to chart: {e}")
        
        # Add LSTM predictions from event-driven results
        lstm_predictions = predictions.get('lstm_predictions')
        if lstm_predictions and lstm_predictions.get('dates'):
            # LSTM prediction line
            fig.add_trace(go.Scatter(
                x=lstm_predictions['dates'],
                y=lstm_predictions['prices'],
                mode='lines+markers',
                name='🤖 Enhanced LSTM' if ENHANCED_AVAILABLE else '🤖 Event-Driven LSTM',
                line=dict(color='#E74C3C', width=4, dash='dash'),
                marker=dict(size=6, color='#E74C3C'),
                hovertemplate='<b>%{x}</b><br>LSTM Pred: $%{y:.2f}<extra></extra>'
            ))
            
            # Add confidence intervals if available
            if (lstm_predictions.get('upper_bound') and 
                lstm_predictions.get('lower_bound')):
                
                # Upper bound
                fig.add_trace(go.Scatter(
                    x=lstm_predictions['dates'],
                    y=lstm_predictions['upper_bound'],
                    mode='lines',
                    name='Upper Confidence',
                    line=dict(color='rgba(231, 76, 60, 0)', width=0),
                    showlegend=False,
                    hoverinfo='skip'
                ))
                
                # Lower bound with fill
                fig.add_trace(go.Scatter(
                    x=lstm_predictions['dates'],
                    y=lstm_predictions['lower_bound'],
                    mode='lines',
                    name='📊 Confidence Interval',
                    line=dict(color='rgba(231, 76, 60, 0)', width=0),
                    fill='tonexty',
                    fillcolor='rgba(231, 76, 60, 0.15)',
                    hovertemplate='<b>%{x}</b><br>Lower: $%{y:.2f}<extra></extra>'
                ))
        
        # Enhanced layout
        fig.update_layout(
            title=dict(
                text=f'📈 {symbol} - {"Enhanced" if ENHANCED_AVAILABLE else "Event-Driven"} Analysis',
                x=0.5,
                font=dict(size=20, color='#2C3E50')
            ),
            xaxis=dict(
                title='Date',
                titlefont=dict(size=14, color='#34495E'),
                gridcolor='rgba(128, 128, 128, 0.2)',
                showgrid=True
            ),
            yaxis=dict(
                title='Price (USD)',
                titlefont=dict(size=14, color='#34495E'),
                tickformat='$,.2f',
                gridcolor='rgba(128, 128, 128, 0.2)',
                showgrid=True
            ),
            hovermode='x unified',
            template='plotly_white',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=500
        )
        
        # Add annotation for event-driven system
        fig.add_annotation(
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            text="⚡ Enhanced Immediate Processing" if ENHANCED_AVAILABLE else "⚡ Event-Driven CQRS System",
            showarrow=False,
            font=dict(size=10, color="#667eea"),
            bgcolor="rgba(102, 126, 234, 0.1)",
            bordercolor="rgba(102, 126, 234, 0.3)",
            borderwidth=1
        )
        
        graphJSON = json.dumps(fig, cls=PlotlyJSONEncoder)
        
        return jsonify({
            'status': 'success',
            'chart': graphJSON,
            'chart_info': {
                'data_source': 'enhanced_event_driven_cqrs' if ENHANCED_AVAILABLE else 'event_driven_cqrs',
                'architecture': 'Enhanced Event Sourcing + CQRS' if ENHANCED_AVAILABLE else 'Event Sourcing + CQRS',
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'immediate_processing': ENHANCED_AVAILABLE
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Error creating chart from event-driven results: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Chart creation error: {str(e)}'
        })

def create_fallback_chart(symbol):
    """Create fallback chart when event-driven system is not available"""
    try:
        # Simple fallback chart
        fig = go.Figure()
        
        fig.add_annotation(
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            text=f"📊 Event-driven analysis for {symbol}<br>Please run analysis first",
            showarrow=False,
            font=dict(size=16, color="#6c757d"),
            align="center"
        )
        
        fig.update_layout(
            title=f'{symbol} - Waiting for Event-Driven Analysis',
            height=400,
            template='plotly_white'
        )
        
        graphJSON = json.dumps(fig, cls=PlotlyJSONEncoder)
        
        return jsonify({
            'status': 'success',
            'chart': graphJSON,
            'chart_info': {
                'data_source': 'fallback',
                'message': 'Run analysis to see event-driven results'
            }
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Fallback chart error: {str(e)}'
        })

# ============================================================================
# COMPATIBILITY AND UTILITY ENDPOINTS
# ============================================================================

@app.route('/api/symbols')
def get_supported_symbols():
    """Get supported symbols (backward compatible)"""
    symbols = [
        {'symbol': 'AAPL', 'name': 'Apple Inc.', 'display': 'AAPL - Apple Inc.'},
        {'symbol': 'GOOGL', 'name': 'Alphabet Inc.', 'display': 'GOOGL - Alphabet Inc.'},
        {'symbol': 'MSFT', 'name': 'Microsoft Corporation', 'display': 'MSFT - Microsoft Corporation'},
        {'symbol': 'AMZN', 'name': 'Amazon.com Inc.', 'display': 'AMZN - Amazon.com Inc.'},
        {'symbol': 'TSLA', 'name': 'Tesla Inc.', 'display': 'TSLA - Tesla Inc.'},
        {'symbol': 'META', 'name': 'Meta Platforms Inc.', 'display': 'META - Meta Platforms Inc.'},
        {'symbol': 'NFLX', 'name': 'Netflix Inc.', 'display': 'NFLX - Netflix Inc.'},
        {'symbol': 'NVDA', 'name': 'NVIDIA Corporation', 'display': 'NVDA - NVIDIA Corporation'},
        {'symbol': 'IBM', 'name': 'International Business Machines Corporation', 'display': 'IBM - International Business Machines Corporation'},
        {'symbol': 'JPM', 'name': 'JPMorgan Chase & Co.', 'display': 'JPM - JPMorgan Chase & Co.'},
    ]
    
    return jsonify({
        'status': 'success',
        'symbols': symbols,
        'count': len(symbols),
        'data_source': 'enhanced_event_driven_system' if ENHANCED_AVAILABLE else 'event_driven_system',
        'architecture': 'Enhanced Event-driven CQRS' if ENHANCED_AVAILABLE else 'Event-driven CQRS'
    })

@app.route('/api/model_info')
def get_model_info():
    """Get information about the event-driven model system"""
    return jsonify({
        'status': 'success',
        'model_info': {
            'type': 'Enhanced Event-Driven LSTM System' if ENHANCED_AVAILABLE else 'Event-Driven LSTM System',
            'architecture': 'Enhanced CQRS + Event Sourcing + Kafka + Immediate Processing' if ENHANCED_AVAILABLE else 'CQRS + Event Sourcing + Kafka',
            'description': 'Enhanced immediate processing event-driven ML system' if ENHANCED_AVAILABLE else 'Fully event-driven ML system with command/query separation',
            'immediate_processing': ENHANCED_AVAILABLE,
            'features': [
                'Enhanced immediate processing' if ENHANCED_AVAILABLE else 'Event Sourcing for complete audit trail',
                'CQRS for optimized read/write operations',
                'Synchronous workflow execution' if ENHANCED_AVAILABLE else 'Asynchronous command processing',
                'Real-time event streaming via Kafka',
                'Aggregate-based domain modeling',
                'Enhanced debugging capabilities' if ENHANCED_AVAILABLE else 'Eventual consistency',
                'No async timing issues' if ENHANCED_AVAILABLE else 'Horizontal scalability',
                'Immediate result availability' if ENHANCED_AVAILABLE else 'Fault tolerance and replay capability'
            ],
            'prediction_horizon': '30 days',
            'confidence_intervals': True,
            'real_time_updates': True,
            'event_store': 'Kafka-based',
            'consistency_model': 'Immediate consistency' if ENHANCED_AVAILABLE else 'Eventual consistency'
        }
    })

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'status': 'error',
        'message': 'Endpoint not found',
        'architecture': 'Enhanced Event-driven CQRS system' if ENHANCED_AVAILABLE else 'Event-driven CQRS system'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"❌ Internal server error: {error}")
    return jsonify({
        'status': 'error',
        'message': 'Internal server error in event-driven system',
        'architecture': 'Enhanced Event-driven CQRS system' if ENHANCED_AVAILABLE else 'Event-driven CQRS system'
    }), 500

# ============================================================================
# APPLICATION STARTUP
# ============================================================================

def create_templates_directory():
    """Create templates directory if it doesn't exist"""
    if not os.path.exists('templates'):
        os.makedirs('templates')
        logger.info("📁 Created templates directory")

if __name__ == '__main__':
    print("🚀 Starting Enhanced Event-Driven Stock Prediction System...")
    print("=" * 80)
    print("🏗️  Architecture: Enhanced CQRS + Event Sourcing + Kafka + Immediate Processing")
    print("🔄 Pattern: Command Query Responsibility Segregation with Immediate Execution")
    print("📚 Event Store: Kafka-based with full audit trail")
    print("⚡ Processing: Enhanced immediate workflow execution")
    print("🚀 Enhancement: No async timing issues, immediate results")
    print("=" * 80)
    
    # Record start time for metrics
    app.config['START_TIME'] = time.time()
    
    # Create templates directory
    create_templates_directory()
    
    # Initialize the event-driven system
    event_system_available = initialize_event_driven_system()
    
    print("\n📊 System Status:")
    print(f"✅ Event-Driven System: {'Active' if event_system_available else 'Failed'}")
    print(f"✅ Enhanced Processing: {'Enabled' if ENHANCED_AVAILABLE else 'Disabled'}")
    print(f"✅ CQRS Architecture: {'Enabled' if event_system_available else 'Disabled'}")
    print(f"✅ Event Sourcing: {'Active' if event_system_available else 'Disabled'}")
    print(f"✅ Kafka Integration: {'Connected' if event_system_available else 'Failed'}")
    print(f"🧠 Model Type: {'Enhanced' if ENHANCED_AVAILABLE else 'Standard'} Event-Driven LSTM System")
    
    if event_system_available:
        print("\n🔄 Event-Driven Features Active:")
        if ENHANCED_AVAILABLE:
            print("- ⚡ ENHANCED: Immediate processing (no async delays)")
            print("- ⚡ ENHANCED: Synchronous workflow execution")
            print("- ⚡ ENHANCED: Instant result availability")
            print("- ⚡ ENHANCED: Enhanced debugging capabilities")
            print("- ⚡ ENHANCED: No stuck workflow issues")
        print("- Complete audit trail via Event Sourcing")
        print("- Command/Query separation (CQRS)")
        print("- Event-driven workflow orchestration")
        print("- Optimized read models for fast queries")
        print("- Domain-driven aggregate modeling")
    
    print("\n🌐 Starting Flask application...")
    print("Available endpoints:")
    print("Enhanced Event-Driven Core:")
    print("- http://localhost:5500/api/analyze/<SYMBOL> (Send analysis command - ENHANCED)")
    print("- http://localhost:5500/api/results/<SYMBOL> (Query analysis results)")
    print("- http://localhost:5500/api/status/<SYMBOL> (Query analysis status)")
    print("- http://localhost:5500/api/chart/<SYMBOL> (Get enhanced chart)")
    
    print("\nEnhanced Debug Endpoints:")
    print("- http://localhost:5500/api/debug/workflow/<SYMBOL> (Debug workflow status)")
    print("- http://localhost:5500/api/debug/process_immediate/<SYMBOL> (Force immediate processing)")
    
    print("\nSystem Health:")
    print("- http://localhost:5500/api/health (Health check)")
    print("- http://localhost:5500/api/model_info (Enhanced architecture info)")
    
    print("\n🤖 How to use the Enhanced Event-Driven System:")
    print("1. POST /api/analyze/AAPL (processes IMMEDIATELY - no waiting!)")
    print("2. GET /api/results/AAPL (queries results from read model)")
    print("3. GET /api/chart/AAPL (displays enhanced analysis chart)")
    print("4. Everything processes synchronously - no async delays!")
    
    if ENHANCED_AVAILABLE:
        print("\n⚡ ENHANCED FEATURES ACTIVE:")
        print("   - Immediate processing eliminates stuck workflows")
        print("   - Synchronous execution prevents async timing issues")
        print("   - Enhanced debugging with detailed status tracking")
        print("   - Instant result availability after command completion")
        print("   - No more waiting for event workflow completion")
    
    print("\n" + "=" * 80)
    
    if not event_system_available:
        print("⚠️  CRITICAL: Event-driven system failed to initialize!")
        print("   1. Check Kafka server is running on localhost:9092")
        print("   2. Verify event_store.py and debug_event_workflow_fix.py are available")
        print("   3. System will run in limited mode without event features")
    elif ENHANCED_AVAILABLE:
        print("🎉 ENHANCED EVENT-DRIVEN SYSTEM READY!")
        print("   ✅ Immediate processing active")
        print("   ✅ No stuck workflows")
        print("   ✅ Enhanced debugging available")
        print("   ✅ Synchronous execution")
    
    try:
        app.run(debug=True, host='0.0.0.0', port=5500)
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
        
        # Cleanup event system
        if event_service:
            if hasattr(event_service, 'stop'):
                event_service.stop()
            print("✅ Enhanced event-driven system stopped")
        
        print("✅ Enhanced event-driven CQRS system shutdown complete")
        
    except Exception as e:
        print(f"❌ Application error: {e}")
        import traceback
        traceback.print_exc()
