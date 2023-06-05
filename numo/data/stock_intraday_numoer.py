import os
from random import random
from datetime import datetime
from typing import Final, List, Optional

import msgpack

from numo.data.data_numoer import DataNumoerConfig
from numo.utils import configclass, singletonremote, jsdump, now_in_nyc, Logs


@configclass
class StockIntradayNumoerConfig(DataNumoerConfig):
    data_source: Final[str]
    ticker: Final[str]
    flush_size: Final[int] = 1000
    file_flush_limit: Final[int] = -1
    per_datum_datetime_extration: Final[str] = "now_in_nyc"
    serializer: Final[str] = "alpaca_json"
    file_per_second_modulus: Final[Optional[int]] = None  # use a new file for each this number of seconds


class StockIntradayNumoer(Logs):
    """
        This class is not thread safe.
    """

    def __init__(self, config: StockIntradayNumoerConfig,
                 calculate_datetime=(lambda *e, **ee: now_in_nyc()),
                 serializer=str):
        super().__init__()
        self.c = config
        self.current_day = None
        self.log_fn = self.log_fh = None
        self.finished = False
        self.flush_count_down = config.flush_size
        self.calculate_datum_datetime = calculate_datetime
        self.serializer = serializer
        self.loginfo(f"intraday data numoer started for {config.ticker}")

    def get_log_file(self, datum, batch_date=None):
        c = self.c
        if batch_date is not None:
            now = batch_date
        else:
            now = self.calculate_datum_datetime(datum)
        ts = datetime.timestamp(now)
        if self.flush_count_down <= 0:
            if self.log_fh:
                try:
                    self.log_fh.flush()
                except:
                    pass
            self.flush_count_down = c.flush_size
        nowaday = (now.year, now.month, now.day)
        if self.current_day != nowaday:
            self.log_threashold_ts = ts + self.c.directory_file_granularity_seconds
            fd = f"{c.directory}/{c.data_source}/{self.c.data_type}/{c.ticker}/{now.year:04d}/{now.month:02d}"
            additional = ''
            if c.file_per_second_modulus:
                additional = f'.{int(ts // c.file_per_second_modulus):d}'
            fn = f"{fd}/{now.day:02d}.{now.hour:02d}{additional}.{ts}{random() * 10}"
            os.makedirs(fd, exist_ok=True)
            self.log_fn = fn
            if self.log_fh is not None:
                try:
                    self.log_fh.close()
                except Exception as e:
                    print(f"Unable to close logfile {self.log_fn} with error {e}")
                    self.logerr(f"intraday data numoer started for {self.c.ticker}")
            self.loginfo(f"Opening {fn} for appending to.")
            self.log_fh = open(fn, "a")
            self.flush_count_down -= 1
            self.current_day = nowaday

        return self.log_fh

    def _log2fh(self, fh, datum):
        return fh.writelines([self.serializer(datum), "\n"])

    def process_streaming_data(self, *data, batch_date=None):
        if not self.finished:
            if batch_date is not None:
                batch_fh = self.get_log_file(None, batch_date)
                logfn = lambda datum: batch_fh.writelines([self.serializer(datum), "\n"])
            else:
                logfn = lambda datum: self._log2fh(fh=self.get_log_file(datum, batch_date), datum=datum)

            for datum in data:
                if isinstance(datum, list):
                    for datum_datum in datum:
                        logfn(datum_datum)
                else:
                    logfn(datum)

        return True

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


class StockIntradayNumoerStoreInTime(StockIntradayNumoer):
    def get_log_file(self, datum, batch_date=None):
        t = datum['t']
        if isinstance(t, str):
            batch_date = datetime.fromisoformat(datum['t'])
        elif isinstance(t, msgpack.Timestamp):
            batch_date = t.to_datetime()
        else:
            raise ValueError(f"ERROR: Could not parse time field {datum['t']=} from {datum}")
        return super().get_log_file(datum, batch_date=batch_date)


@singletonremote
class StockIntradayNumoerStoreInTimeActor(StockIntradayNumoerStoreInTime):
    def __init__(self, *e, **ee):
        super().__init__(*e, **ee)


def wrap_streaming_data_numoer(data_numoer):
    async def ret(*e, **ee):
        r = await data_numoer.process_streaming_data.remote(*e, **ee)
        return r

    return ret


class StockIntradayNumoerFeeder(Logs):
    numoers: Final[List[StockIntradayNumoer]] = []

    ## This method helps feeders of intradaynumoer to start
    ## all the actors it needs. Should be called inside init
    def start_remote_numoers(self,
                             base_config: StockIntradayNumoer,
                             data_source=None,
                             feed_trade_numoer=None,
                             feed_quote_numoer=None):
        numoers = self.numoers
        init_numoer_remotely = lambda c, name: StockIntradayNumoerStoreInTimeActor.options(name=name).remote(c,
                                                                                                             serializer=jsdump)
        for ticker in self.tickers:
            if feed_trade_numoer is not None:
                trade_numoer = init_numoer_remotely(
                    base_config.update(
                        data_source=data_source, data_type='trade', ticker=ticker),
                    name=f"{ticker}_trades_numoer")
                feed_trade_numoer(wrap_streaming_data_numoer(trade_numoer), ticker)
                numoers.append(trade_numoer)
            if feed_quote_numoer is not None:
                quote_numoer = init_numoer_remotely(
                    base_config.update(
                        data_source=data_source, data_type='quote', ticker=ticker),
                    name=f"{ticker}_quotes_numoer")
                feed_quote_numoer(wrap_streaming_data_numoer(quote_numoer), ticker)
                numoers.append(quote_numoer)
