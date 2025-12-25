import time
import psycopg2
from core.config import settings

MAX_RETRIES = 15
SLEEP_SECONDS = 2

def wait_for_db():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(settings.DATABASE_URL)
            conn.close()
            print("✅ Database is ready")
            return
        except Exception as e:
            print(
                f"⏳ Waiting for database "
                f"({attempt}/{MAX_RETRIES})..."
            )
            time.sleep(SLEEP_SECONDS)

    raise RuntimeError("❌ Database not ready after retries")

if __name__ == "__main__":
    wait_for_db()
