import time

import ray

from numo.alpaca import AlpacaStreamListener, AlpacaAuthConfig
from numo.data.stock_intraday_numoer import StockIntradayNumoerConfig

run_forever = True
if __name__ == '__main__':
    ray.init()

    tickerss = [['SQ', 'AAPL', 'SPY', 'TSLA', 'ABBV', 'GOOG'],
                ['AMZN', 'MSFT', 'FB', 'BABA', 'BAC', 'JPM', 'WFC', 'C']],
    threads = []
    adcs = []
    for ff, tkrs in zip(['parquet', 'jsonl'], tickerss):
        print(f"Starting the {ff} on {tkrs} run now")
        adc = AlpacaStreamListener(
            tickers=tkrs,
            auth_config=AlpacaAuthConfig(
                key_id="AK8EBC4D9NDD10MG0YWL",
                secret_key="kmAdiUZoqO2lSUm7UJZpY0PssIk5ag8dcFPz7Lpf",
            ),
            config=StockIntradayNumoerConfig(
                ticker="NADA",
                directory="/tmp/test_alpaca_numoer",
                data_type="UNKNOWN",
                data_source="alpaca_streaming",
                file_per_second_modulus=5 * 60,  # every five minutes is a new file
                file_format=ff
            ),
        )
        adcs.append(adc)
        threads.append(adc.run())

    if run_forever:
        adc.run()  # This is a blocking call, use the following instead
    thread = adc.run_in_thread()
    print("Sleeping for 5 minutes")
    time.sleep(300)
    print("Done sleeping, sending quit signal to all downloads")
    [adc.quit() for adc in adcs]
    print("Waiting for thread to exit")
    [thread.join() for thread in threads]
    print("Stopping ray")
    ray.shutdown()
    print("Done")
