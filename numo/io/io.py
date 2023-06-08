from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict


class FileWriter(ABC):
    @abstractmethod
    def add_datum(self, datum: Dict):
        pass

    @abstractmethod
    def add_data(self, data):
        pass

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def close(self):
        pass

    def add_data(self, data):
        for datum in data:
            self.add_datum(datum)


IO_REGISTRY_SOURCE_DATA_FORMAT = defaultdict(lambda: defaultdict(defaultdict))
