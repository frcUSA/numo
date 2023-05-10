import time

import ray

from numo.alpaca import AlpacaStreamListener, AlpacaAuthConfig
from numo.data.stock_intraday_numoer import StockIntradayNumoer, StockIntradayNumoerConfig

run_forever = True
if __name__ == '__main__':
    ray.init()
    adc = AlpacaStreamListener(
        tickers=['SQ', 'AAPL', 'SPY', 'TSLA', 'ABBV', 'GOOG', 'AMZN', 'MSFT', 'FB', 'BABA', 'BAC', 'JPM', 'WFC', 'C'],
        auth_config=AlpacaAuthConfig(
            key_id="AK8EBC4D9NDD10MG0YWL",
            secret_key="kmAdiUZoqO2lSUm7UJZpY0PssIk5ag8dcFPz7Lpf",
        ),
        config=StockIntradayNumoerConfig(
            ticker="NADA",
            directory="/tmp/test_alpaca_streaming_data_logging",
            data_type="UNKNOWN",
            data_source="alpaca"
        ),
        ray_inst=ray,
    )
    print("Starting the run now")
    if run_forever:
        adc.run()  # This is a blocking call, use the following instead
    thread = adc.run_in_thread()
    print("Sleeping for 5 minutes")
    time.sleep(300)
    print("Done sleeping, sending quit signal")
    adc.quit()
    print("Waiting for thread to exit")
    thread.join()
    print("Stopping ray")
    ray.shutdown()
    print("Done")
