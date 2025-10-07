# Внешние зависимости
from typing import Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv
import os
import logging
# Внутренние модули
from app.settings.logger import setup_logger

load_dotenv()


@dataclass
class Config:
    TABLE_URL: str = field(default_factory=lambda: os.getenv("TABLE_URL"))
    GOOGLE_KEYS_PATH: str = field(default_factory=lambda: os.getenv("GOOGLE_KEYS_PATH"))
    
    COOKIES_FOR_PARSER_PATH: str = field(default_factory=lambda: os.getenv("COOKIES_FOR_PARSER_PATH"))
    
    ADMIN_ID: int = field(default_factory=lambda: int(os.getenv("ADMIN_ID")))
    BOT_TOKEN: str = field(default_factory=lambda: os.getenv("BOT_TOKEN"))
    
    TIMEOUT_WORK_MINUTES: int = field(default_factory=lambda: int(os.getenv("TIMEOUT_WORK_MINUTES", 300)))
    RANGE_DAYS_WORK: int = field(default_factory=lambda: int(os.getenv("RANGE_DAYS_WORK", 7)))
    DELTA_DAYS_WORK: int = field(default_factory=lambda: int(os.getenv("DELTA_DAYS_WORK", 7)))
    WORKSHEET_NUM: int = field(default_factory=lambda: int(os.getenv("WORKSHEET_NUM", 0)))
    
    PROXY: Optional[str] = field(default_factory=lambda: os.getenv("PROXY"))

    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.logger = setup_logger(
            name=os.getenv("LOG_NAME", "main"),
            level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", None)
        )

        self.logger.info("Конфигурация инициализирована")


    def __str__(self) -> str:
        return f"Config(table_url={self.TABLE_URL}, log_level={self.logger.level})"


_instance = None


def get_config() -> Config:
    global _instance
    if _instance is None:
        _instance = Config()

    return _instance