from pydantic import BaseModel
from typing import Optional


class Coin(BaseModel):
    symbol: str
    name: str
    price_usd: Optional[float]
    market_cap: Optional[float]
    source: str
