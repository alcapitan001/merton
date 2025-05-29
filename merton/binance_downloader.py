"""
Binance Historical Data Downloader
---------------------------------
A simple module to download historical klines/candlestick data from Binance.
Because sometimes you need more than just a crystal ball to predict crypto prices.

Example:
    >>> downloader = BinanceDataDownloader()
    >>> df = downloader.download_data("BTCUSDT", "1h", date(2024, 1, 1), date(2024, 1, 31))
    >>> print(f"Downloaded {len(df)} candles. Time to make some questionable trading decisions!")
"""

from datetime import date, timedelta
import requests
import pandas as pd
import zipfile
import io
import os
from typing import Optional, Union
from pathlib import Path

class BinanceDataDownloader:
    """
    Downloads historical data from Binance. 
    Warning: May cause excessive coffee consumption and sleep deprivation.
    """
    
    def __init__(self, base_path: Union[str, Path] = "data"):
        """
        Initialize the downloader.
        
        Args:
            base_path: Where to save the data. Defaults to "data".
                      Because we all need a place to store our hopes and dreams.
        """
        self.kline_url_prefix = "https://data.binance.vision/data/spot/daily/klines"
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def get_url(self, timeframe: str, symbol: str, dt: date) -> str:
        """
        Generate URL for Binance historical data.
        
        Args:
            timeframe: Time interval (e.g., "1m", "1h", "1d")
            symbol: Trading pair (e.g., "BTCUSDT")
            dt: Date to download
            
        Returns:
            URL string that will make your internet connection cry
        """
        date_str = dt.strftime("%Y-%m-%d")
        return f"{self.kline_url_prefix}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date_str}.zip"
    
    def download_data(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: date, 
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """
        Download historical data for a given symbol and date range.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Time interval (e.g., "1m", "1h", "1d")
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            DataFrame with OHLCV data or None if download fails.
            Warning: May contain traces of market manipulation.
        """
        current_date = start_date
        all_data = []
        
        while current_date <= end_date:
            url = self.get_url(timeframe, symbol, current_date)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    # Extract zip file
                    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                        # Read the CSV file inside the zip
                        csv_filename = zip_file.namelist()[0]
                        with zip_file.open(csv_filename) as csv_file:
                            df = pd.read_csv(csv_file, header=None)
                            all_data.append(df)
                else:
                    print(f"No data available for {current_date} - Binance is probably having a coffee break")
            except Exception as e:
                print(f"Error downloading data for {current_date}: {str(e)} - Time to blame the internet")
            
            current_date += timedelta(days=1)
        
        if all_data:
            # Combine all dataframes
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Set column names (because Binance loves their unnamed columns)
            combined_df.columns = [
                'timestamp', 'open', 'high', 'low', 'close', 
                'volume', 'close_time', 'quote_volume', 'trades',
                'taker_buy_base', 'taker_buy_quote', 'ignore'
            ]
            
            # Convert timestamp to datetime
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], unit='ms')
            combined_df.set_index('timestamp', inplace=True)
            
            # Save to CSV
            output_file = self.base_path / f"{symbol}_{timeframe}_{start_date}_{end_date}.csv"
            combined_df.to_csv(output_file)
            print(f"Data saved to {output_file} - Your hard drive is now slightly more valuable")
            
            return combined_df
        return None

if __name__ == "__main__":
    # Example usage
    downloader = BinanceDataDownloader()
    df = downloader.download_data(
        "BTCUSDT", 
        "1m", 
        date(2024, 1, 1), 
        date(2024, 1, 31)
    )
    if df is not None:
        print(f"Downloaded {len(df)} candles. Time to make some questionable trading decisions!")