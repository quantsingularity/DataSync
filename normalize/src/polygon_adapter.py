"""
DataSync - Polygon.io Feed Adapter
Converts raw Polygon WebSocket messages into NormalizedTick / NormalizedBar.

Polygon message types:
  T.*  - trade
  Q.*  - quote
  A.*  - aggregate (per-second bar)
  AM.* - aggregate (per-minute bar)
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

from .models import AssetClass, DataSource, NormalizedBar, NormalizedTick, Timeframe


def _ts(ms: int) -> datetime:
    """Convert millisecond epoch to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def polygon_trade_to_tick(msg: Dict[str, Any]) -> NormalizedTick:
    """Convert a Polygon trade event (ev=T) to a NormalizedTick."""
    return NormalizedTick(
        time=(
            _ts(msg["t"])
            if isinstance(msg.get("t"), (int, float))
            else datetime.now(timezone.utc)
        ),
        symbol=msg["sym"],
        asset_class=AssetClass.EQUITY,
        source=DataSource.POLYGON,
        price=Decimal(str(msg["p"])),
        size=Decimal(str(msg.get("s", 0))),
        conditions=msg.get("c", []),
        exchange=str(msg.get("x", "")),
        tape=str(msg.get("z", "")),
        extra={
            "sequence_number": msg.get("q"),
            "correction": msg.get("e"),
        },
    )


def polygon_quote_to_tick(msg: Dict[str, Any]) -> NormalizedTick:
    """Convert a Polygon quote event (ev=Q) to a NormalizedTick."""
    bid = Decimal(str(msg["bp"])) if "bp" in msg else None
    ask = Decimal(str(msg["ap"])) if "ap" in msg else None
    mid = ((bid + ask) / 2) if bid and ask else (bid or ask or Decimal("0"))
    return NormalizedTick(
        time=(
            _ts(msg["t"])
            if isinstance(msg.get("t"), (int, float))
            else datetime.now(timezone.utc)
        ),
        symbol=msg["sym"],
        asset_class=AssetClass.EQUITY,
        source=DataSource.POLYGON,
        price=mid,
        bid=bid,
        ask=ask,
        bid_size=Decimal(str(msg["bs"])) if "bs" in msg else None,
        ask_size=Decimal(str(msg["as"])) if "as" in msg else None,
        conditions=msg.get("c", []),
        extra={},
    )


def polygon_agg_to_bar(
    msg: Dict[str, Any], timeframe: Timeframe = Timeframe.ONE_MIN
) -> NormalizedBar:
    """Convert a Polygon aggregate event (ev=A or AM) to a NormalizedBar."""
    return NormalizedBar(
        time=(
            _ts(msg["s"])
            if isinstance(msg.get("s"), (int, float))
            else datetime.now(timezone.utc)
        ),
        symbol=msg["sym"],
        asset_class=AssetClass.EQUITY,
        timeframe=timeframe,
        source=DataSource.POLYGON,
        open=Decimal(str(msg["o"])),
        high=Decimal(str(msg["h"])),
        low=Decimal(str(msg["l"])),
        close=Decimal(str(msg["c"])),
        volume=Decimal(str(msg["v"])) if "v" in msg else None,
        vwap=Decimal(str(msg["av"])) if "av" in msg else None,
    )
