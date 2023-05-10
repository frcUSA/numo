import time

import ray

from numo.alpaca import AlpacaStreamListener, AlpacaAuthConfig, AlpacaHistoricalRetriever
from numo.data.stock_intraday_numoer import StockIntradayNumoer, StockIntradayNumoerConfig

run_forever = True
if __name__ == '__main__':
    ray.init(num_cpus=64)

    ahc = AlpacaHistoricalRetriever(
        tickers=['SQ', 'AAPL', 'SPY', 'TSLA', 'ABBV', 'GOOG', 'AMZN', 'MSFT', 'FB', 'BABA', 'BAC', 'JPM', 'WFC', 'C'],
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
    time.sleep(1)


    if run_forever:
        ahc.run()  # This is a blocking call, use the following instead
    thread = ahc.run_in_thread()
    print("Sleeping for 5 minutes")
    time.sleep(300)
    print("Done sleeping, sending quit signal")
    ahc.quit()
    print("Waiting for thread to exit")
    thread.join()
    print("Stopping ray")
    ray.shutdown()
    print("Done")
