import yfinance as yf
import pandas as pd
import numpy as np
import json
from typing import Dict, Any, List
import asyncio
from datetime import datetime
from .scheduler import PipelineTask, Priority
from .utils import get_data_path
from .utils import setup_logger, get_data_path
import aiofiles
import logging

logger = setup_logger(__name__) 

async def fetch_spy_data(config: Dict[str, Any]) -> pd.DataFrame:
    """Fetch SPY historical data."""
    spy_data = await asyncio.to_thread(
        lambda: yf.download(
            config['data']['symbol'],
            start=config['data']['start_date'],
            end=pd.Timestamp.today().strftime('%Y-%m-%d'),
            progress=False
        )
    )
    
    # Flatten multi-index columns if present
    if isinstance(spy_data.columns, pd.MultiIndex):
        spy_data.columns = [col[0] for col in spy_data.columns]
    
    return spy_data

def transform_data(config: Dict[str, Any], dep_results: Dict[str, Any]) -> pd.DataFrame:
    """Transform SPY data with technical indicators."""
    df = dep_results['fetch'].copy()
    
    # Calculate daily returns
    df['Daily_Return'] = df['Close'].pct_change()
    
    # Calculate moving averages
    df['SMA_50'] = df['Close'].rolling(window=config['analysis']['sma_short']).mean()
    df['SMA_200'] = df['Close'].rolling(window=config['analysis']['sma_long']).mean()
    
    # Calculate volatility
    df['Volatility'] = df['Daily_Return'].rolling(
        window=config['analysis']['volatility_window']
    ).std()
    
    # RSI calculation
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=config['analysis']['rsi_period']).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=config['analysis']['rsi_period']).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df['Close'].ewm(span=config['analysis']['macd_fast'], adjust=False).mean()
    exp2 = df['Close'].ewm(span=config['analysis']['macd_slow'], adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(
        span=config['analysis']['macd_signal'], adjust=False
    ).mean()
    
    # Market regime
    df['Market_Regime'] = np.where(df['SMA_50'] > df['SMA_200'], 'Bullish', 'Bearish')
    
    return df

def validate_data(config: Dict[str, Any], dep_results: Dict[str, Any]) -> Dict[str, Any]:
    """Validate transformed data and generate quality metrics."""
    df = dep_results['transform']
    validation_results = {
        'is_valid': True,
        'warnings': [],
        'info_messages': [],  # Using info_messages instead of expected_gaps
        'metrics': {}
    }
    
    # Data completeness checks with expected missing values
    missing_values = df.isnull().sum()
    expected_missing = {
        'Daily_Return': 1,
        'SMA_50': config['analysis']['sma_short'] - 1,
        'SMA_200': config['analysis']['sma_long'] - 1,
        'Volatility': config['analysis']['volatility_window'] - 1,
        'RSI': config['analysis']['rsi_period'] - 1
    }

    # Check for unexpected vs expected missing values
    for column, count in missing_values.items():
        if count > 0:
            if column in expected_missing and count == expected_missing[column]:
                validation_results['info_messages'].append(
                    f"{column}: {count} gaps (normal for calculation window)"
                )
            else:
                validation_results['warnings'].append(
                    f"Unexpected gaps in {column}: {count} values"
                )
    
    # Data range validations
    rsi_max = df['RSI'].max()
    rsi_min = df['RSI'].min()
    if rsi_max > 100 or rsi_min < 0:
        validation_results['is_valid'] = False
        validation_results['warnings'].append(
            f"RSI values out of valid range: min={rsi_min:.2f}, max={rsi_max:.2f}"
        )
    
    # Logical validations
    invalid_prices = (df['High'] < df['Low']).any()
    if invalid_prices:
        validation_results['is_valid'] = False
        validation_results['warnings'].append("Found instances where High < Low")
    
    # Calculate quality metrics
    validation_results['metrics'] = {
        'data_points': len(df),
        'date_range': f"{df.index.min()} to {df.index.max()}",
        'avg_daily_volume': float(df['Volume'].mean()),
        'volatility_mean': float(df['Volatility'].mean()),
        'missing_data_pct': float((df.isnull().sum().sum() / df.size) * 100),
        'current_market_regime': str(df['Market_Regime'].iloc[-1]),
        'current_rsi': float(df['RSI'].iloc[-1])
    }
    
    return validation_results

async def save_analysis(config: Dict[str, Any], dep_results: Dict[str, Any]) -> bool:
    """Save transformed data and validation results."""
    logger = logging.getLogger(__name__)
    
    try:
        df = dep_results['transform']
        validation = dep_results['validate']
        
        # Get paths using utility function
        data_path = get_data_path(config, 'spy_analysis.csv')
        validation_path = get_data_path(config, 'validation_report.json')
        metrics_path = get_data_path(config, 'latest_metrics.json')
        
        logger.info(f"Saving analysis data to: {data_path}")
        await asyncio.to_thread(df.to_csv, data_path)
        
        # Latest metrics calculation
        latest_metrics = {
            'last_price': float(df['Close'].iloc[-1]),
            'daily_return': float(df['Daily_Return'].iloc[-1]),
            'current_rsi': float(df['RSI'].iloc[-1]),
            'market_regime': str(df['Market_Regime'].iloc[-1]),
            'volatility': float(df['Volatility'].iloc[-1]),
            'sma_50': float(df['SMA_50'].iloc[-1]),
            'sma_200': float(df['SMA_200'].iloc[-1]),
            'macd': float(df['MACD'].iloc[-1]),
            'signal_line': float(df['Signal_Line'].iloc[-1])
        }
        
        # Save validation and metrics files
        logger.info(f"Saving validation report to: {validation_path}")
        async with aiofiles.open(validation_path, 'w') as f:
            await f.write(json.dumps(validation, indent=4, default=str))
        
        logger.info(f"Saving latest metrics to: {metrics_path}")
        async with aiofiles.open(metrics_path, 'w') as f:
            await f.write(json.dumps(latest_metrics, indent=4))
            
        logger.info("All files saved successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in save_analysis: {str(e)}")
        logger.exception("Full exception details:")
        raise

def create_pipeline_tasks(config: Dict[str, Any]) -> List[PipelineTask]:
    """Create all pipeline tasks with their configurations."""
    tasks = [
        PipelineTask(
            name="fetch",
            function=fetch_spy_data,
            priority=Priority[config['pipeline']['priorities']['fetch']]
        ),
        PipelineTask(
            name="transform",
            function=transform_data,
            priority=Priority[config['pipeline']['priorities']['transform']],
            dependencies=["fetch"]
        ),
        PipelineTask(
            name="validate",
            function=validate_data,  # Direct reference to function
            priority=Priority[config['pipeline']['priorities']['validate']],
            dependencies=["transform"]
        ),
        PipelineTask(
            name="save",
            function=save_analysis,  # Direct reference to async function
            priority=Priority[config['pipeline']['priorities']['save']],
            dependencies=["transform", "validate"]
        )
    ]
    return tasks

# Make create_pipeline_tasks available for import
__all__ = ['create_pipeline_tasks']