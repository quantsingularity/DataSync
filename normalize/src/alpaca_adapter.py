"""
DataSync - Alpaca Feed Adapter
Converts raw Alpaca WebSocket messages into NormalizedTick / NormalizedBar.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

from .models import AssetClass, DataSource, NormalizedBar, NormalizedTick, Timeframe


def alpaca_trade_to_tick(
    msg: Dict[str, Any], asset_class: AssetClass
) -> NormalizedTick:
    """Convert an Alpaca trade message to a NormalizedTick."""
    return NormalizedTick(
        time=msg.get("t") or datetime.now(timezone.utc),
        symbol=msg["S"],
        asset_class=asset_class,
        source=DataSource.ALPACA,
        price=Decimal(str(msg["p"])),
        size=Decimal(str(msg.get("s", 0))),
        conditions=msg.get("c", []),
        exchange=msg.get("x"),
        tape=msg.get("z"),
        extra={"id": msg.get("i"), "taker_side": msg.get("tks")},
    )


def alpaca_quote_to_tick(
    msg: Dict[str, Any], asset_class: AssetClass
) -> NormalizedTick:
    """Convert an Alpaca quote message to a NormalizedTick."""
    bid = Decimal(str(msg["bp"])) if "bp" in msg else None
    ask = Decimal(str(msg["ap"])) if "ap" in msg else None
    mid = ((bid + ask) / 2) if bid and ask else (bid or ask or Decimal("0"))
    return NormalizedTick(
        time=msg.get("t") or datetime.now(timezone.utc),
        symbol=msg["S"],
        asset_class=asset_class,
        source=DataSource.ALPACA,
        price=mid,
        bid=bid,
        ask=ask,
        bid_size=Decimal(str(msg["bs"])) if "bs" in msg else None,
        ask_size=Decimal(str(msg["as"])) if "as" in msg else None,
        exchange=msg.get("ax"),
        conditions=msg.get("c", []),
        extra={},
    )


def alpaca_bar_to_bar(
    msg: Dict[str, Any],
    asset_class: AssetClass,
    timeframe: Timeframe = Timeframe.ONE_MIN,
) -> NormalizedBar:
    """Convert an Alpaca bar message to a NormalizedBar."""
    return NormalizedBar(
        time=msg.get("t") or datetime.now(timezone.utc),
        symbol=msg["S"],
        asset_class=asset_class,
        timeframe=timeframe,
        source=DataSource.ALPACA,
        open=Decimal(str(msg["o"])),
        high=Decimal(str(msg["h"])),
        low=Decimal(str(msg["l"])),
        close=Decimal(str(msg["c"])),
        volume=Decimal(str(msg["v"])) if "v" in msg else None,
        trade_count=msg.get("n"),
        vwap=Decimal(str(msg["vw"])) if "vw" in msg else None,
    )
