from binance_downloader import BinanceDataDownloader
from datetime import date

# Create downloader instance
downloader = BinanceDataDownloader()

# Download some data
df = downloader.download_data(
    symbol="BTCUSDT",
    timeframe="1h",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31)
)

# Do something with the data
if df is not None:
    print(df.head())

# Export the data to a CSV file
df.to_csv('exported_data.csv', index=False)

# Print a message to confirm the export
print("Data exported to exported_data.csv")