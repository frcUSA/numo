import os
from datetime import datetime
from typing import Final, List

from numo.data.data_numoer import DataNumoerConfig
from numo.utils import configclass, singletonremote, jsdump, now_in_nyc
import logging


@configclass
class StockIntradayNumoerConfig(DataNumoerConfig):
    data_source: Final[str]
    ticker: Final[str]
    flush_size = 1000
    file_flush_limit = 1000
    per_datum_datetime_extration = "now_in_nyc"
    serializer = "alpaca_json"


class StockIntradayNumoer:
    """
        This class is not thread safe.
    """

    def __init__(self, config: StockIntradayNumoerConfig,
                 calculate_datetime=(lambda *e, **ee: now_in_nyc()),
                 serializer=str):
        self.c = config
        self.log_threashold_ts = 0
        self.log_fn = self.log_fh = None
        self.finished = False
        self.flush_count_down = config.flush_size
        self.file_countdown = config.file_flush_limit
        self.calculate_datum_datetime = calculate_datetime
        self.serializer = serializer
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.info(f"intraday data numoer started for {config.ticker}")

    def get_log_file(self, datum, batch_date=None):
        c = self.c
        if batch_date is not None:
            now = batch_date
        else:
            now = self.calculate_datum_datetime(datum)
        ts = datetime.timestamp(now)
        if self.flush_count_down <= 0:
            self.log_fh.flush()
            self.flush_count_down = c.flush_size
            self.file_countdown -= 1
        if ts > self.log_threashold_ts or self.file_countdown <= 0:
            self.log_threashold_ts = ts + self.c.directory_file_granularity_seconds
            fd = f"{c.directory}/{c.data_source}/{self.c.data_type}/{c.ticker}/{now.year}/{now.month}/{now.day}"
            fn = f"{fd}/{now.hour}+{ts}"
            os.makedirs(fd, exist_ok=True)
            self.log_fn = fn
            if self.log_fh is not None:
                try:
                    self.log_fh.close()
                except Exception as e:
                    print(f"Unable to close logfile {self.log_fn} with error {e}")
                    self.__logger.error(f"intraday data numoer started for {config.ticker}")
            self.__logger.info(f"Opening {fn} for appending to.")
            self.log_fh = open(fn, "a")
            self.flush_count_down = c.flush_size
            self.file_countdown = c.file_flush_limit
        self.flush_count_down -= 1

        return self.log_fh

    def _log2fh(self, fh, datum):
        return fh.writelines([self.serializer(datum), "\n"])

    def process_streaming_data(self, *data, batch_date=None):
        if not self.finished:
            if batch_date is not None:
                logfn = (lambda fh: lambda datum: fh.writelines([self.serializer(datum), "\n"]))(
                    self.get_log_file(None, batch_date))
            else:
                logfn = lambda datum: self._log2fh(fh=self.get_log_file(datum, batch_date), datum=datum)

            for datum in data:
                logfn(datum)

    def quit(self):
        self.finished = True
        try:
            self.log_fh.close()
        except Exception as e:
            print(f"Unable to close logfile {self.log_fn} while quiting with error {e}")
            pass
        raise NotImplementedError()


@singletonremote
class StockIntradayNumoerActor(StockIntradayNumoer):
    def __init__(self, *e, **ee):
        super().__init__(*e, **ee)


def wrap_streaming_data_numoer(data_numoer):
    def ret(*e, **ee):
        data_numoer.process_streaming_data.remote(*e, **ee)

    return ret


class StockIntradayNumoerFeeder:
    numoers: Final[List[StockIntradayNumoer]] = []

    ## This method helps feeders of intradaynumoer to start
    ## all the actors it needs. Should be called inside init
    def start_remote_numoers(self,
                             base_config: StockIntradayNumoer,
                             data_source=None,
                             feed_trade_numoer=None,
                             feed_quote_numoer=None):
        numoers = self.numoers
        init_numoer_remotely = lambda c: StockIntradayNumoerActor.remote(c, serializer=jsdump)
        for ticker in self.tickers:
            if feed_trade_numoer is not None:
                trade_numoer = init_numoer_remotely(base_config.update(
                    data_source=data_source, data_type='trade', ticker=ticker))
                feed_trade_numoer(wrap_streaming_data_numoer(trade_numoer), ticker)
                numoers.append(trade_numoer)
            if feed_quote_numoer is not None:
                quote_numoer = init_numoer_remotely(base_config.update(
                    data_source=data_source, data_type='quote', ticker=ticker))
                feed_quote_numoer(wrap_streaming_data_numoer(quote_numoer), ticker)
                numoers.append(quote_numoer)
