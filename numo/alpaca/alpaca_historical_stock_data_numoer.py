from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
from typing import Final, List, Union, Optional, Generator
import logging
import ray
from alpaca.common import SupportedCurrencies, DATA_V2_MAX_LIMIT, RawData
from alpaca.data import StockQuotesRequest, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.historical.utils import get_data_from_response

from numo.alpaca import AlpacaAuthConfig
from numo.data.stock_intraday_numoer import StockIntradayNumoer, StockIntradayNumoerActor, StockIntradayNumoerFeeder
from numo.utils import jsdump, doubletonremote, Logs


def wrap_historical_data_numoer(data_numoer):
    async def ret(*e, **ee):
        data_numoer.process_historical_data.remote(*e, **ee)

    return ret


class CustomizedStockHistoricalDataClient(StockHistoricalDataClient, Logs):
    def __init__(self, *e, return_lists=True, **ee):
        self.return_lists = return_lists
        Logs.__init__(self)
        super().__init__(*e, **ee)

    def _data_stream(
            self,
            endpoint_asset_class: str,
            endpoint_data_type: str,
            api_version: str,
            symbol_or_symbols: str | List[str],
            limit: Optional[int] = None,
            page_limit: int = DATA_V2_MAX_LIMIT,
            **kwargs,
    ) -> Generator[RawData, None, None]:
        """Same as _data_get except returns generator of pages

            Returns: an iterator of tuples (ticker, datum)
        """
        # params contains the payload data
        params = kwargs

        # stocks, crypto, etc
        path = f"/{endpoint_asset_class}"

        params["symbols"] = ",".join(symbol_or_symbols)

        # TODO: Improve this mess if possible
        path += f"/{endpoint_data_type}"

        # data_by_symbol is in format of
        #    {
        #       "symbol1": [ "data1", "data2", ... ],
        #       "symbol2": [ "data1", "data2", ... ],
        #                ....
        #    }
        total_items = 0
        page_token = None

        self.loginfo("Beginning to stream data.")
        while True:

            actual_limit = None

            # adjusts the limit parameter value if it is over the page_limit
            if limit:
                # actual_limit is the adjusted total number of items to query per request
                actual_limit = min(int(limit) - total_items, page_limit)
                if actual_limit < 1:
                    break

            params["limit"] = actual_limit
            params["page_token"] = page_token

            response = self.get(path=path, data=params, api_version=api_version)

            # TODO: Merge parsing if possible

            for symbol, data in get_data_from_response(response).items():
                if not self.return_lists and isinstance(data, list):
                    for datum in data:
                        yield symbol, datum
                else:
                    yield symbol, data
            # if we've sent a request with a limit, increment count
            if actual_limit:
                total_items += actual_limit

            page_token = response.get("next_page_token", None)
            if page_token is None:
                break
        self.loginfo("Finished to stream data.")

    def stream_trades(self, request_params: StockQuotesRequest):
        return self._data_stream(
            endpoint_data_type="trades",
            endpoint_asset_class="stocks",
            api_version="v2",
            **request_params.to_request_fields(),
        )

    def stream_quotes(self, request_params: StockQuotesRequest):
        return self._data_stream(
            endpoint_data_type="quotes",
            endpoint_asset_class="stocks",
            api_version="v2",
            **request_params.to_request_fields(),
        )


@ray.remote(concurrency_groups={"quotes": 1, "trades": 1})
class AlpacaHistoricalRetrievingActor(StockIntradayNumoerFeeder):

    def __init__(self,
                 tickers,
                 auth_config: AlpacaAuthConfig,
                 config: StockIntradayNumoer,
                 start_date: datetime = datetime.now() - timedelta(weeks=50 * 52.1429),
                 end_date: datetime = datetime.now() + timedelta(weeks=50 * 52.1429)):
        super().__init__()
        self.c = config
        self.ac = auth_config
        self.start_date, self.end_date = start_date, end_date
        self.tickers = tickers
        self.quote_handlers = dict()
        self.trade_handlers = dict()
        self.loginfo(f"Starting remote numoers for {self.tickers} from {start_date} to {end_date}.")
        self.start_remote_numoers(
            base_config=config,
            data_source='alpaca',
            feed_trade_numoer=lambda numoer_call, ticker: self.trade_handlers.__setitem__(ticker, numoer_call),
            feed_quote_numoer=lambda numoer_call, ticker: self.quote_handlers.__setitem__(ticker, numoer_call),
        )
        self.quit = False

    @ray.method(concurrency_group="quotes")
    async def run_quotes(self) -> bool:
        self.loginfo(f"Starting to run quotes for {self.tickers}")
        cshdc = CustomizedStockHistoricalDataClient(self.ac.key_id, self.ac.secret_key,
                                                    raw_data=True, return_lists=True)
        stream = cshdc.stream_quotes(StockQuotesRequest(
            feed=DataFeed.IEX,
            # These are for BaseTimeseriesDataRequest
            symbol_or_symbols=self.tickers,
            start=self.start_date,
            end=self.end_date,
            limit=None,
            currency=SupportedCurrencies.USD
        ))
        for ticker, datum in stream:
            await self.quote_handlers[ticker](datum)
            if self.quit:
                break
        self.loginfo(f"Finished running quotes for {self.tickers}")
        return True

    @ray.method(concurrency_group="trades")
    async def run_trades(self) -> bool:
        self.loginfo(f"Starting to run trades for {self.tickers}")
        cshdc = CustomizedStockHistoricalDataClient(self.ac.key_id, self.ac.secret_key, raw_data=True, )
        stream = cshdc.stream_trades(StockQuotesRequest(
            feed=DataFeed.IEX,
            # These are for BaseTimeseriesDataRequest
            symbol_or_symbols=self.tickers,
            start=self.start_date,
            end=self.end_date,
            limit=None,
            currency=SupportedCurrencies.USD
        ))
        for ticker, datum in stream:
            if self.quit:
                break
            await self.trade_handlers[ticker](datum)
        self.loginfo(f"Finished running trades for {self.tickers}")
        return True


class AlpacaHistoricalRetriever(Logs):
    def __init__(self,
                 tickers: List[str],
                 auth_config: AlpacaAuthConfig,
                 config: StockIntradayNumoer):
        super().__init__()
        self.c = config
        self.ac = auth_config
        self.tickers = tickers
        self.actors = [AlpacaHistoricalRetrievingActor.remote(
            [ticker, ],
            auth_config=auth_config,
            config=config,
        ) for ticker in tickers]
        self.loginfo(f"Beginning to stream trades and quotes for {tickers}")
        self.thread = None

    def run(self):
        return ray.get(
            list(
                chain(
                    *((actor.run_quotes.remote(),
                       actor.run_trades.remote())
                      for actor in self.actors)
                )
            )
        )

    def run_in_thread(self, daemon=True):
        thread = self.thread = Thread(target=self.run, daemon=daemon)
        thread.start()
        return thread

    def quit(self):
        self.quit = True
        waits = []
        for numoer in self.numoers:
            waits.append(numoer.quit.remote())  # closeup the files and stop reacting to streamed data
        ray.get(waits)
        if self.thread is not None:
            self.thread.join()
        raise NotImplementedError()
