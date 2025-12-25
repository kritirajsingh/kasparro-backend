from sqlalchemy.sql import text
from core.database import SessionLocal
from schemas.coin import Coin
from services.checkpoints import get_last_checkpoint, update_checkpoint
from services.etl_run_tracker import start_etl_run, finish_etl_run


def normalize_coinpaprika():
    SOURCE = "coinpaprika"
    session = SessionLocal()

    run_id = start_etl_run(SOURCE)
    processed = 0

    try:
        last_ts = get_last_checkpoint(SOURCE)

        if last_ts:
            rows = session.execute(
                text("""
                    SELECT id, payload, fetched_at
                    FROM raw_coinpaprika
                    WHERE fetched_at > :last_ts
                    ORDER BY fetched_at
                """),
                {"last_ts": last_ts}
            ).fetchall()
        else:
            rows = session.execute(
                text("""
                    SELECT id, payload, fetched_at
                    FROM raw_coinpaprika
                    ORDER BY fetched_at
                """)
            ).fetchall()

        latest_ts = None

        for row in rows:
            payload = row.payload
            latest_ts = row.fetched_at

            coin = Coin(
                symbol=payload.get("symbol"),
                name=payload.get("name"),
                price_usd=payload.get("quotes", {}).get("USD", {}).get("price"),
                market_cap=payload.get("quotes", {}).get("USD", {}).get("market_cap"),
                source=SOURCE
            )

            session.execute(
                text("""
                    INSERT INTO coins (symbol, name, price_usd, market_cap, source)
                    VALUES (:symbol, :name, :price_usd, :market_cap, :source)
                    ON CONFLICT (symbol, source)
                    DO UPDATE SET
                        price_usd = EXCLUDED.price_usd,
                        market_cap = EXCLUDED.market_cap,
                        updated_at = NOW()
                """),
                coin.model_dump()
            )

            processed += 1

        session.commit()

        if latest_ts:
            update_checkpoint(SOURCE, latest_ts)

        finish_etl_run(run_id, processed, "success")

    except Exception as e:
        session.rollback()
        finish_etl_run(run_id, processed, "failure", str(e))
        raise

    finally:
        session.close()
