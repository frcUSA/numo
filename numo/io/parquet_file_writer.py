from datetime import datetime
from decimal import Decimal, getcontext
from functools import partial
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq

from .io import FileWriter, IO_REGISTRY_SOURCE_DATA_FORMAT
from ..utils import NYCTZ_NAME


class ParquetFileWriter(FileWriter):
    def __init__(self, file_name: str, schema: pa.Schema, buffer_size: int = 2 * 14, pqw_options={},
                 conversions: dict = None):
        self.schema = schema
        self.file_name = file_name
        self.pqw_options = pqw_options
        self.buffer_size = buffer_size

        self.pqtwtr = pq.ParquetWriter(
            where=self.file_name,
            schema=self.schema,
            **pqw_options
        )
        self.field_names = [str(schema.field(i).name) for i in range(len(schema))]
        self.buffer = [[] for _ in range(len(schema))]
        self.conversions = conversions or {}

    def _close(self):
        self.pqtwtr.close()
        self.pqtwtr = None

    def __enter__(self):
        if self.pqtwtr is None:
            raise IOError("Cannot reopen a parquet writer once it is closed")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __bool__(self):
        return self.pqtwtr is not None and self.pqtwtr.is_open

    def flush(self):
        buffer = self.buffer
        self.buffer = [[] for _ in range(len(self.schema))]
        self.pqtwtr.write_batch(pa.RecordBatch.from_arrays(buffer, schema=self.schema))

    def close(self):
        self.flush()
        self._close()

    def add_datum(self, datum: Dict):
        for idx, fnm in enumerate(self.field_names):
            converter = self.conversions.get(fnm, None)
            subdatum = datum.get(fnm, None)
            if converter:
                subdatum = converter(subdatum)
            self.buffer[idx].append(subdatum)
        if len(self.buffer[0]) >= self.buffer_size:
            self.flush()


def _ROUND_PRICE_DECIMAL(d, ndigits=6):
    return round(Decimal(d), ndigits=ndigits)


ALPACA_CONVERSIONS = {
    't': datetime.fromisoformat,
    'p': _ROUND_PRICE_DECIMAL,
    'ap': _ROUND_PRICE_DECIMAL,
    'bp': _ROUND_PRICE_DECIMAL,
    'u': lambda s: s or ''  # Convert null to empty string.
}
PRICE_TYPE = pa.decimal128(28,6)
getcontext().prec = 28
TRADE_ID_TYPE = pa.int64()
ALPACA_HISTORICAL_TRADE_SCHEMA = pa.schema([
    ('t', pa.timestamp('ns', NYCTZ_NAME)),  # time
    ('x', pa.string()),  # exchange
    ('p', PRICE_TYPE),  # price
    ('s', pa.int64()),  # shares
    ('c', pa.large_list(pa.string())),  # trade condition
    ('i', TRADE_ID_TYPE),  # trade id
    ('z', pa.string()),  # tape
    ('u', pa.string()),  # updates
])
ALPACA_HISTORICAL_QUOTE_SCHEMA = pa.schema([
    ('t', pa.timestamp('ns', NYCTZ_NAME)),  # time
    ('ax', pa.string()),  # ask exchange
    ('ap', PRICE_TYPE),  # ask price
    ('as', pa.int64()),  # ask size
    ('bx', pa.string()),  # bid exchange
    ('bp', PRICE_TYPE),  # bid price
    ('bs', pa.int64()),  # bid size
    ('c', pa.large_list(pa.string())),  # quote condition
    ('z', pa.string()),  # tape
])

ALPACA_STREAMING_TRADE_SCHEMA = pa.schema([
    ('T', pa.string()),  # message type is 't' for trades
    # ('S', pa.string()), ## We skip the symbol
    ('t', pa.timestamp('ns', NYCTZ_NAME)),
    ('x', pa.string()),  # exchange
    ('p', PRICE_TYPE),  # price of trade
    ('s', pa.int64()),  # size of trade
    ('c', pa.large_list(pa.string())),  # trade conditions
    ('i', TRADE_ID_TYPE),  # trade id
    ('z', pa.string()),  # tape
])
ALPACA_STREAMING_QUOTE_SCHEMA = pa.schema([
    ('T', pa.string()),  # message type is 'q' for quotes
    # ('S', pa.string()), ## We skip the symbol
    ('ax', pa.string()),  # ask exchange
    ('ap', PRICE_TYPE),  # ask price
    ('as', pa.int64()),  # ask size
    ('bx', pa.string()),  # bid exchange
    ('bp', PRICE_TYPE),  # bid price
    ('bs', pa.int64()),  # bid size
    ('c', pa.large_list(pa.string())),  # quote condition
    ('t', pa.timestamp('ns', NYCTZ_NAME)),  # time
    ('z', pa.string()),  # tape
])
# TODO, handle message types: 'c' for correction and 'x' for cancelation
IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_historical']['trade']['parquet'] = partial(
    ParquetFileWriter,
    schema=ALPACA_HISTORICAL_TRADE_SCHEMA,
    conversions=ALPACA_CONVERSIONS,
)
IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_historical']['quote']['parquet'] = partial(
    ParquetFileWriter,
    schema=ALPACA_HISTORICAL_QUOTE_SCHEMA,
    conversions=ALPACA_CONVERSIONS,
)
IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_streaming']['trade']['parquet'] = partial(
    ParquetFileWriter,
    schema=ALPACA_STREAMING_TRADE_SCHEMA,
    conversions=ALPACA_CONVERSIONS,
)
IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_streaming']['quote']['parquet'] = partial(
    ParquetFileWriter,
    schema=ALPACA_STREAMING_QUOTE_SCHEMA,
    conversions=ALPACA_CONVERSIONS,
)
