import pandas as pd
import os
import logging
from mysql_client import get_mysql_engine

# Suppress SQLAlchemy query logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

engine = get_mysql_engine()
engine.echo = False

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")

FILES = {
    "raw_partner_headlines.csv": "news_raw",
    "reliance_prices.csv": "stock_prices",
    "news_with_sentiment.csv": "news_sentiment",
    "final_sentiment_price.csv": "analysis_output"
}

for file, table in FILES.items():
    print(f"\nLoading {file} into {table}")

    csv_path = os.path.join(DATA_PATH, file)
    print("Reading from:", csv_path)

    # Special handling for reliance_prices.csv - skip row 1 (ticker symbol row)
    if file == "reliance_prices.csv":
        df = pd.read_csv(csv_path, skiprows=[1])
    else:
        df = pd.read_csv(csv_path)

    # Remove unwanted index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Handle news_raw table schema
    if table == "news_raw":
        df = df.rename(columns={
            "stock": "company",
            "publisher": "source"
        })
        # Add missing article_text column
        if "article_text" not in df.columns:
            df["article_text"] = ""
        # Keep only required columns
        df = df[["date", "company", "headline", "article_text", "source"]]

    # Handle stock_prices table schema
    elif table == "stock_prices":
        # Convert numeric columns
        df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
        df['High'] = pd.to_numeric(df['High'], errors='coerce')
        df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        df['Date'] = pd.to_datetime(df['Date'])
        # Drop rows with NaN dates
        df = df.dropna(subset=['Date'])
        # Rename to match database schema (lowercase)
        df = df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        # Add company column and reorder
        df['company'] = 'RELIANCE'
        df = df[['date', 'company', 'open', 'high', 'low', 'close', 'volume']]

    # Handle news_sentiment and analysis_output
    elif table in ["news_sentiment", "analysis_output"]:
        # Just convert date columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        # Select only the columns that match the expected table schema
        if table == "news_sentiment" and all(col in df.columns for col in ['headline', 'date']):
            df = df[['headline', 'date']]

    print("Final columns:", df.columns.tolist())
    print("Final shape:", df.shape)

    # Chunked insert for large data
    try:
        # Use 'replace' to create table with correct schema
        if_exists_mode = "replace" if table in ["stock_prices", "analysis_output"] else "append"
        df.to_sql(
            table,
            engine,
            if_exists=if_exists_mode,
            index=False,
            chunksize=500 if table == "stock_prices" else 5000
        )
        print(f"[OK] Successfully loaded {file} into {table}")
    except Exception as e:
        print(f"[ERROR] Error loading {file}: {str(e)}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"First few rows:\n{df.head()}")
        raise

print("\n[COMPLETE] All CSVs loaded into MySQL")
