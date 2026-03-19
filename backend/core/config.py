"""
Application configuration — loaded from .env file.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    odds_api_key: str = ""
    redis_url: Optional[str] = None
    odds_refresh_interval: int = 3600   # 1 hour default — free tier is 500 req/month
    min_arb_profit: float = 0.5   # minimum profit % to surface
    host: str = "0.0.0.0"
    port: int = 8000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
