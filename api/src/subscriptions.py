"""
DataSync - Subscription Manager
Allows downstream services to register which symbols they need.
Subscriptions drive which symbols are actively streamed and stored.
"""

import logging
from typing import List, Optional

from normalize.src.models import SubscriptionRequest
from store.src.timescale import get_pool

logger = logging.getLogger("datasync.subscriptions")


class Subscription:
    """Simple dataclass for subscription records."""

    def __init__(self, row: dict):
        self.id = row["id"]
        self.service_name = row["service_name"]
        self.symbol = row["symbol"]
        self.asset_class = row["asset_class"]
        self.is_active = row["is_active"]
        self.subscribed_at = row["subscribed_at"]
        self.__dict__.update(row)


async def create_subscription(body: SubscriptionRequest) -> dict:
    """Register a subscription, updating if already exists."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO subscriptions (service_name, symbol, asset_class, is_active)
        VALUES ($1, $2, $3, TRUE)
        ON CONFLICT (service_name, symbol) DO UPDATE
          SET is_active = TRUE, asset_class = EXCLUDED.asset_class
        RETURNING id, service_name, symbol, asset_class, is_active, subscribed_at
        """,
        body.service_name,
        body.symbol.upper(),
        str(body.asset_class),
    )
    logger.info(f"Subscription created/updated: {body.service_name} -> {body.symbol}")
    return dict(row)


async def list_subscriptions(active_only: bool = True) -> List[dict]:
    """List all registered subscriptions."""
    pool = await get_pool()
    q = "SELECT id, service_name, symbol, asset_class, is_active, subscribed_at FROM subscriptions"
    if active_only:
        q += " WHERE is_active = TRUE"
    q += " ORDER BY symbol, service_name"
    rows = await pool.fetch(q)
    return [dict(r) for r in rows]


async def delete_subscription(sub_id: int) -> None:
    """Deactivate a subscription (soft delete)."""
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE subscriptions SET is_active = FALSE WHERE id = $1",
        sub_id,
    )
    if result == "UPDATE 0":
        from fastapi import HTTPException

        raise HTTPException(404, f"Subscription #{sub_id} not found")
    logger.info(f"Subscription #{sub_id} deactivated")


async def get_active_symbols(asset_class: Optional[str] = None) -> List[str]:
    """Return unique active symbols, optionally filtered by asset class."""
    pool = await get_pool()
    if asset_class:
        rows = await pool.fetch(
            "SELECT DISTINCT symbol FROM subscriptions WHERE is_active = TRUE AND asset_class = $1",
            asset_class,
        )
    else:
        rows = await pool.fetch(
            "SELECT DISTINCT symbol FROM subscriptions WHERE is_active = TRUE"
        )
    return [r["symbol"] for r in rows]
