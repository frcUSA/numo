import json
import logging
from abc import ABC
from dataclasses import dataclass, replace
from datetime import datetime, date

import msgpack
import ray
from pytz import timezone


def configclass(cls):
    result = dataclass(cls,
                       kw_only=True,
                       frozen=True,
                       )
    return result


@configclass
class BaseConfigClass:
    def __post_init__(self):
        object.__setattr__(self, "update", lambda **ee: replace(self, **ee))


def singletonremote(*e, **ee):
    return ray.remote(max_concurrency=1)(*e, **ee)


def doubletonremote(*e, **ee):
    return ray.remote(max_concurrency=2)(*e, **ee)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, msgpack.ext.Timestamp):
        return datetime.timestamp(obj.to_datetime())
    elif isinstance(obj, (datetime, date)):
        return obj.timestamp()
    raise TypeError("Type %s not serializable" % type(obj))


def jsdump(item):
    return json.dumps(item, default=json_serial)


NYCTZ_NAME = 'US/Eastern'
NYCTZ = timezone(NYCTZ_NAME)


def now_in_nyc(indt=None):
    if indt is None:
        return datetime.now(NYCTZ)
    else:
        return indt.astimezone(NYCTZ)


class Logs(ABC):
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def loginfo(self, *e, **ee):
        self._logger.info(*e, **ee)

    def logerr(self, *e, **ee):
        self._logger.error(*e, **ee)

    def _logwarn(self, *e, **ee):
        self._logger.warning(*e, **ee)

    def _logbug(self, *e, **ee):
        self._logger.debug(*e, **ee)
