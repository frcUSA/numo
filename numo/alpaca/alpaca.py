from typing import Final

from numo.utils import configclass, BaseConfigClass


@configclass
class AlpacaAuthConfig(BaseConfigClass):
    key_id: Final[str] = None
    secret_key: Final[str] = None
    paper_trading: Final[bool] = False

