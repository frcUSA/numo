import time

import ray

from numo.alpaca import AlpacaStreamListener, AlpacaAuthConfig, AlpacaHistoricalRetriever
from numo.data.stock_intraday_numoer import StockIntradayNumoer, StockIntradayNumoerConfig

run_forever = True
if __name__ == '__main__':
    ray.init(num_cpus=24)
    tickers = ['SQ', 'AAPL', 'SPY', 'TSLA', 'ABBV', 'GOOG', 'AMZN', 'MSFT', 'FB', 'BABA', 'BAC', 'JPM', 'WFC', 'C']
    # tickers=['SQ','BABA', 'BAC', 'JPM', 'WFC', 'C']
    # tickers=['AEAE', 'SWSS', 'MBTC']
    tickers = ['SQ', 'AAPL', 'SPY', 'TSLA']#, 'GOOG', ]#'AMZN', 'MSFT', 'FB']

    threads = []
    ahc = AlpacaHistoricalRetriever(
        tickers=tickers,
        auth_config=AlpacaAuthConfig(
            key_id="AK8EBC4D9NDD10MG0YWL",
            secret_key="kmAdiUZoqO2lSUm7UJZpY0PssIk5ag8dcFPz7Lpf",
        ),
        config=StockIntradayNumoerConfig(
            ticker="ANY",
            directory="/tmp/test_alpaca_historical_data_downloading",
            data_type="UNKNOWN",
            data_source="alpaca"
        ),
    )
    thread = ahc.run_in_thread()

    print("Waiting for thread to exit")
    thread.join()
    print("Stopping ray")
    ray.shutdown()
    print("Done")
