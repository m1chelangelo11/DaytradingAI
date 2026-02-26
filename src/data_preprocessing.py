import pandas as pd, logging
from pathlib import Path

current_dir = Path(__file__).resolve().parent
log_dir = current_dir.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'data_preprocessing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

raw_data_dir = current_dir.parent / "data" / "raw"
df = pd.read_csv(raw_data_dir / "market_data.csv")
logger.info("Loaded raw data")

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(['symbol', 'timestamp'])

# Check for duplicates
duplicates_count = df.duplicated(subset=['timestamp']).sum()
logger.info(f"Found {duplicates_count} duplicate timestamps")

if duplicates_count > 0:
    logger.warning(f"Duplicate timestamps found: {df[df.duplicated(subset=['timestamp'], keep=False)]['timestamp'].unique()}")
    df = df.drop_duplicates(subset=['timestamp'], keep='first')
    logger.info(f"Removed duplicates, remaining records: {len(df)}")

df = df.dropna()
logger.info(f"After dropna: {len(df)} records")

df = df.set_index('timestamp')
logger.info(f"Set timestamp as index")

if not df.index.is_monotonic_increasing:
    raise ValueError("Timestamps should be monotonically increasing")

processed_dir = current_dir.parent / "data" / "processed"
processed_dir.mkdir(exist_ok=True)
df.to_csv(processed_dir / "preprocessed_data.csv")
logger.info(f"Saved preprocessed data to {processed_dir}")
