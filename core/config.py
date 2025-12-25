from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    COINGECKO_URL: str
    COINPAPRIKA_URL: str

    class Config:
        env_file = ".env"
        extra = "forbid"   # GOOD PRACTICE

settings = Settings()
