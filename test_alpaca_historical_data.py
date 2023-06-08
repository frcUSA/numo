import ray

from numo.alpaca import AlpacaAuthConfig, AlpacaHistoricalRetriever
from numo.data.stock_intraday_numoer import StockIntradayNumoerConfig

run_forever = True
if __name__ == '__main__':
    ray.init(num_cpus=24)
    tickers = ['SQ', 'AAPL', 'SPY', 'TSLA', 'ABBV', 'GOOG', 'AMZN', 'MSFT', 'FB', 'BABA', 'BAC', 'JPM', 'WFC', 'C']
    # tickers=['SQ','BABA', 'BAC', 'JPM', 'WFC', 'C']
    # tickers=['AEAE', 'SWSS', 'MBTC']
    tickers = ['SQ', 'AAPL', 'SPY', 'TSLA']  # , 'GOOG', ]#'AMZN', 'MSFT', 'FB']
    formats = ['parquet', 'jsonl']
    threads = []
    for ff in formats:
        ahc = AlpacaHistoricalRetriever(
            tickers=tickers,
            auth_config=AlpacaAuthConfig(
                key_id="AK8EBC4D9NDD10MG0YWL",
                secret_key="kmAdiUZoqO2lSUm7UJZpY0PssIk5ag8dcFPz7Lpf",
            ),
            config=StockIntradayNumoerConfig(
                ticker="ANY",
                directory="/tmp/test_alpaca_numoer",
                data_type="UNKNOWN",
                data_source="alpaca_historical",
                file_format=ff,
            ),
        )
        thread = ahc.run_in_thread()
        threads.append(thread)

    print("Waiting for thread to exit")
    [thread.join() for thread in threads]
    print("Stopping ray")
    ray.shutdown()
    print("Done")
