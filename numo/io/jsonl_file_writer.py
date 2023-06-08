from typing import Dict

from .io import FileWriter, IO_REGISTRY_SOURCE_DATA_FORMAT
from ..utils import jsdump


class JsonlFileWriter(FileWriter):
    def __init__(self, file_name: str, jsdump=jsdump):
        self.file_name = file_name
        self.jsdump = jsdump

        self.fh = open(file_name, 'w')

    def _close(self):
        self.fh.close()
        self.fh = None

    def __enter__(self):
        if self.fh is None:
            raise IOError("Cannot reopen a parquet writer once it is closed")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __bool__(self):
        return self.fh is not None and not self.fh.closed

    def flush(self):
        self.fh.flush()

    def close(self):
        self.flush()
        self._close()

    def add_datum(self, datum: Dict):
        self.fh.writelines([self.jsdump(datum)])


IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_historical']['trade']['jsonl'] = \
    IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_historical']['quote']['jsonl'] = \
    IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_streaming']['trade']['jsonl'] = \
    IO_REGISTRY_SOURCE_DATA_FORMAT['alpaca_streaming']['quote']['jsonl'] = JsonlFileWriter
