from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from pathlib import Path
import os, pandas as pd, logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

current_dir = Path(__file__).resolve().parent
log_dir = current_dir.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'alpaca.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path)

logger.info("Alpaca Client Initialization")
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    logger.error("No API_KEY or SECRET_KEY .env file found")
    raise ValueError("API credentials not found")

data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

all_data = []
symbols = ["AAPL", "GOOGL", "MSFT"]

end_date = datetime.now()
start_date = end_date - timedelta(days=30)
start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

logger.info(f"Downloading data from {start_str} to {end_str}")

try:
    for symbol in symbols:
        logger.info(f"Requesting data for {symbol}")
        
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=start_str,
            end=end_str,
            feed='iex'
        )
        
        bars = data_client.get_stock_bars(request_params)
        df = bars.df
        logger.info(f"Downloaded {len(df)} records for {symbol}")
        all_data.append(df)

    combined_df = pd.concat(all_data)
    logger.info(f"Total records: {len(combined_df)}")
    
    data_dir = current_dir.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    save_path = data_dir / 'market_data.csv'
    combined_df.to_csv(save_path)
    logger.info(f"Dataset saved to: {save_path}")

except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    raise