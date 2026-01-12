from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session


def backfill_market_p_no(conn: Connection | Session) -> int:
    result = conn.execute(
        text(
            """
            UPDATE market_snapshots
            SET market_p_no = 1 - market_p_yes,
                market_p_no_derived = TRUE
            WHERE market_p_no IS NULL
              AND market_p_yes IS NOT NULL
              AND is_yesno IS TRUE
            """
        )
    )
    return int(getattr(result, "rowcount", 0) or 0)
