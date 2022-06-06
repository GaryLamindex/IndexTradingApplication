import pathlib
import datetime as dt
import requests
import zipfile

class crypto_data_engine:
    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_min'
        self.base_url = 'https://data.binance.vision'

    def get_historical_data_by_range(self, ticker, start_timestamp, end_timestamp, bar_size):
        start_dt = dt.datetime.fromtimestamp(start_timestamp).strftime('%Y-%m-%d')
        end_dt = dt.datetime.fromtimestamp(end_timestamp).strftime('%Y-%m-%d')
        ticker = ticker.upper()
        url = f'{self.base_url}/data/spot/daily/klines/{ticker}/{bar_size}/{ticker}-{bar_size}-{start_dt}.zip'
        response = requests.get(url)
        zip_filename = f'{self.ticker_data_path}/{ticker}.zip'
        open(zip_filename, 'wb').write(response.content)


def main():
    engine = crypto_data_engine()
    engine.get_historical_data_by_range('BTCUSDT', 1654058694, 0, '1m')


if __name__ == '__main__':
    main()

