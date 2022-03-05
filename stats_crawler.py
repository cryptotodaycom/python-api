from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import List

import redis
import requests

REDIS_HOST = "localhost"
REDIS_PORT = "6379"
COINAPI_OHLCV = "https://rest.coinapi.io/v1/ohlcv"
HEADERS = {"X-CoinAPI-Key": "FCD1EB52-4C81-45C5-8D10-FDF276D80BA8"}

CACHE = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
CACHE_TIMEOUT = 5 * 60


@dataclass
class Exchange:
    name: str
    pair: str


@dataclass
class CoinData:
    symbol: str
    total_supply: float
    max_supply: float
    exchange_list: List[Exchange]


@dataclass
class Coin:
    data: CoinData


COINS = [
    Coin(CoinData(
        symbol="BTC",
        total_supply=521521,
        max_supply=12512521521,
        exchange_list=[
            Exchange(name="Binance", pair="BTC/USDT"),
            Exchange(name="Kraken", pair="BTC/USDT")
        ]))
]


@dataclass
class OHLCV:  # coinapi format
    price_open: float = 0
    price_high: float = 0
    price_low: float = 0
    price_close: float = 0
    volume_traded: float = 0
    trades_count: int = 0
    time_period_start: str = None
    time_period_end: str = None
    time_open: str = None
    time_close: str = None

    # after one is needed for 14days...
    def price_increase(self, after: OHLCV = None) -> float:
        if not after:
            return 100 * (self.price_close - self.price_open) / self.price_open
        return 100 * ((after.price_close / self.price_open) * (self.volume_traded / after.volume_traded) - 1)


@dataclass
class Percentages:
    h1: float
    h24: float
    d7: float
    d14: float
    d30: float
    y1: float


@dataclass
class Stats:
    price: float
    market_cap: float
    fully_diluted_market_cap: float
    trading_volume: float
    h24_price_low: float
    h24_price_high: float
    d7_price_low: float
    d7_price_high: float
    percentages: Percentages


def _create_stats(coin_data: CoinData,
                  price_ohlcv: OHLCV,
                  h1_ohlcv: OHLCV,
                  h24_ohlcv: OHLCV,
                  d7_ohlcv: OHLCV,
                  d14_ohlcv: OHLCV,
                  d30_ohlcv: OHLCV,
                  y1_ohlcv: OHLCV) -> Stats:
    price = price_ohlcv.price_close / price_ohlcv.volume_traded
    market_cap = price * coin_data.total_supply
    fully_diluted_market_cap = price * coin_data.max_supply
    h24_price_low = h24_ohlcv.price_low / h24_ohlcv.volume_traded
    h24_price_high = h24_ohlcv.price_high / h24_ohlcv.volume_traded
    d7_price_low = d7_ohlcv.price_low / d7_ohlcv.volume_traded
    d7_price_high = d7_ohlcv.price_high / d7_ohlcv.volume_traded
    return Stats(
        price,
        market_cap,
        fully_diluted_market_cap,
        h24_ohlcv.volume_traded,
        h24_price_low,
        h24_price_high,
        d7_price_low,
        d7_price_high,
        Percentages(
            h1=h1_ohlcv.price_increase(),
            h24=h24_ohlcv.price_increase(),
            d7=d7_ohlcv.price_increase(),
            d14=d14_ohlcv.price_increase(d7_ohlcv),
            d30=d30_ohlcv.price_increase(),
            y1=y1_ohlcv.price_increase()
        ))


def _ohlcv_sums(ohlcv: OHLCV, ohlcv_rcv: OHLCV) -> None:
    volume = ohlcv_rcv.volume_traded
    ohlcv.price_open += ohlcv_rcv.price_open * volume
    ohlcv.price_high += ohlcv_rcv.price_high * volume
    ohlcv.price_low += ohlcv_rcv.price_low * volume
    ohlcv.price_close += ohlcv_rcv.price_close * volume
    ohlcv.volume_traded += volume


def _create_coinapi_url(symbol_id: str, period_id: str, limit: int) -> str:
    return f"{COINAPI_OHLCV}/{symbol_id}/latest?period_id={period_id}&limit={limit}"


def _process_data(symbol_id: str, period_id: str, limit: int, ohlcv1: OHLCV, ohlcv2: OHLCV = None) -> None:
    url = _create_coinapi_url(symbol_id, period_id, limit)
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise SystemExit(f"Response code for '{url}' was {response.status_code}!")

    data = response.json()
    if len(data) < 1 or len(data) > 2:  # 1 or 2 is expected
        raise SystemExit(f"Data returned for '{url}' has length {len(data)}!")

    _ohlcv_sums(ohlcv1, OHLCV(**data[0]))
    if ohlcv2:  # necessary when limit is set to 2 for 14 day
        _ohlcv_sums(ohlcv2, OHLCV(**data[1]))


def _create_coinapi_symbol_id(exchange: Exchange) -> str:
    asset_id_base, asset_id_quote = exchange.pair.split("/")
    return f"{exchange.name}_SPOT_{asset_id_base}_{asset_id_quote}".upper()


def main() -> None:
    print("Starting to crawl...")

    for coin in COINS:
        print(f"Processing coin '{coin.data.symbol}'...")

        price_ohlcv = OHLCV()
        h1_ohlcv = OHLCV()
        h24_ohlcv = OHLCV()
        d7_ohlcv = OHLCV()
        d14_ohlcv = OHLCV()
        d30_ohlcv = OHLCV()
        y1_ohlcv = OHLCV()

        for exchange in coin.data.exchange_list:
            symbol_id = _create_coinapi_symbol_id(exchange)
            print(f"  Processing symbol_id '{symbol_id}'...")

            # Current price
            _process_data(symbol_id, "5SEC", 1, price_ohlcv, None)
            # 1h
            _process_data(symbol_id, "1HRS", 1, h1_ohlcv, None)
            # 24h
            _process_data(symbol_id, "1DAY", 1, h24_ohlcv, None)
            # 7d + 14d
            _process_data(symbol_id, "7DAY", 2, d7_ohlcv, d14_ohlcv)
            # 30d
            _process_data(symbol_id, "1MTH", 1, d30_ohlcv, None)
            # 1y
            _process_data(symbol_id, "1YRS", 1, y1_ohlcv, None)

        print("  Calculating stats...")
        stats = _create_stats(coin.data, price_ohlcv, h1_ohlcv, h24_ohlcv, d7_ohlcv, d14_ohlcv, d30_ohlcv, y1_ohlcv)

        print(f"  Saving stats into cache under name '{coin.data.symbol}'...")
        CACHE.setex(name=coin.data.symbol, time=CACHE_TIMEOUT, value=json.dumps(asdict(stats)))

    print("Crawling ended!")


if __name__ == "__main__":
    raise SystemExit(main())
