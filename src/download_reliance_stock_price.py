import yfinance as yf
import pandas as pd
from src.logger import logger
from src.config import STOCK_PRICE_PATH


def download_price_data():
    logger.info("Downloading Reliance stock price data...")

    ticker = "RELIANCE.NS"
    df = yf.download(
        ticker,
        start="2012-01-01",
        end="2025-01-01",
        progress=False
    )

    if df.empty:
        raise ValueError("No data downloaded from Yahoo Finance")

    # Reset index to get Date column
    df.reset_index(inplace=True)

    # Keep only required columns
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

    df.to_csv(STOCK_PRICE_PATH, index=False)
    logger.info(f"Stock price data saved to {STOCK_PRICE_PATH}")


if __name__ == "__main__":
    download_price_data()
