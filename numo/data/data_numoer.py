from typing import Final

from numo.utils import configclass, BaseConfigClass


@configclass
class DataNumoerConfig(BaseConfigClass):
    data_type: Final[str]
    data_source: Final[str]
    directory: Final[str]
    directory_file_granularity_seconds: Final[float] = 24 * 60 * 60  # defaults to a new file every 24 hours
