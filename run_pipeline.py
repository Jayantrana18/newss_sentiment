import subprocess


def run_step(description, command):
    print(f"\n=== {description} ===")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Step failed: {description}")
        exit(1)

import os

if __name__ == "__main__":
    # 1. Download news data from S3 (if not present)
    news_file = os.path.join("data", "raw_partner_headlines.csv")
    if not os.path.exists(news_file):
        run_step("Downloading news data from S3 (if needed)", "python -m src.s3_loader")
    else:
        print(f"News data already present at {news_file}, skipping S3 download.")
    # 2. Download Reliance stock price data
    run_step("Downloading Reliance stock price data", "python -m src.download_reliance_stock_price")
    # 3. Run sentiment analysis
    run_step("Running sentiment analysis", "python -m src.sentiment")
    # 4. Run PySpark aggregation and analytics
    run_step("Running PySpark aggregation and analytics", "python -m src.spark_analysis")
    # 5. Load all CSVs into MySQL (optional, comment out if not needed)
    run_step("Loading CSVs into MySQL", "python -m src.DB.load_csv_to_mysql")

    print("\nAll pipeline steps completed successfully!")