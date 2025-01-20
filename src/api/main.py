from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import json
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path
import logging
import yaml

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = FastAPI(title="SPY Analysis API")

class MarketMetrics(BaseModel):
    last_price: float
    daily_return: float
    current_rsi: float
    market_regime: str
    volatility: float
    sma_50: float
    sma_200: float
    macd: float
    signal_line: float

# Load config to get data directory
def load_config():
    config_path = Path("config/config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)

config = load_config()
PROCESSED_DATA_DIR = Path(config['data']['processed_dir'])

@app.get("/")
async def root():
    return {"message": "SPY Analysis API"}

@app.get("/metrics/latest")
async def get_latest_metrics():
    try:
        metrics_path = PROCESSED_DATA_DIR / 'latest_metrics.json'
        with open(metrics_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Metrics not found")

@app.get("/data/historical")
async def get_historical_data(days: Optional[int] = 252):
    try:
        csv_path = PROCESSED_DATA_DIR / 'spy_analysis.csv'
        logger.info(f"Reading CSV from: {csv_path.absolute()}")
        
        if not csv_path.exists():
            logger.error(f"File not found at {csv_path.absolute()}")
            raise HTTPException(
                status_code=404, 
                detail="Data file not found. Please ensure the pipeline has run successfully."
            )
            
        df = pd.read_csv(csv_path, parse_dates=True, index_col=0)
        
        if df.empty:
            logger.error("CSV file is empty")
            raise HTTPException(
                status_code=404,
                detail="No data available in the CSV file"
            )
            
        logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        logger.debug(f"DataFrame shape: {df.shape}")
        
        if days:
            df = df.tail(days)
            
        data = {
            "dates": df.index.astype(str).tolist(),
            "close": df['Close'].tolist(),
            "volume": df['Volume'].tolist(),
            "sma_50": df['SMA_50'].tolist(),
            "sma_200": df['SMA_200'].tolist(),
            "rsi": df['RSI'].tolist(),
            "macd": df['MACD'].tolist(),
            "signal_line": df['Signal_Line'].tolist(),
            "market_regime": df['Market_Regime'].tolist()
        }
        
        if not data['dates']:
            logger.error("No dates found in processed data")
            raise HTTPException(
                status_code=500,
                detail="No dates found in processed data"
            )
            
        return data
        
    except Exception as e:
        logger.error(f"Error processing historical data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing data: {str(e)}"
        )
    
@app.get("/analysis/validation")
async def get_validation_report():
    try:
        validation_path = PROCESSED_DATA_DIR / 'validation_report.json'
        with open(validation_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Validation report not found")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)