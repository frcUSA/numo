from threading import Thread
from typing import Final, List

from alpaca.data.live import StockDataStream

from numo.alpaca import AlpacaAuthConfig
from numo.data import StockIntradayNumoerFeeder, StockIntradayNumoer
import ray

class AlpacaStreamListener(StockIntradayNumoerFeeder):

    def __init__(self, tickers, auth_config: AlpacaAuthConfig, config: StockIntradayNumoer):
        self.c = config
        self.ac = ac = auth_config
        self.asds = StockDataStream(ac.key_id, ac.secret_key, raw_data=True, )
        self.tickers = tickers
        self.thread = None
        self.start_remote_numoers(
            base_config=config,
            data_source='alpaca',
            feed_trade_numoer=lambda numoer_call, ticker: self.asds.subscribe_trades(numoer_call, ticker),
            feed_quote_numoer=lambda numoer_call, ticker: self.asds.subscribe_quotes(numoer_call, ticker),
        )

    def run(self):
        self.asds.run()

    def run_in_thread(self, daemon=True):
        thread = self.thread = Thread(target=self.run, daemon=daemon)
        thread.start()
        return thread

    def quit(self):
        waits = []
        for numoer in self.numoers:
            waits.append(numoer.quit.remote())  # closeup the files and stop reacting to streamed data
        ray.get(waits)
        self.asds.stop()
        if self.thread is not None:
            self.thread.join()
        raise NotImplementedError()
