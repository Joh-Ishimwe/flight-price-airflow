"""Central configuration. The only module that reads environment variables.

Everything else imports from here, so no other file contains a password,
hostname, or port.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Walk up from this file to the project root: settings.py -> utils -> src -> root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load .env into the environment. Values already set in the real environment
# win, because in production (inside Airflow) there is no .env file - the
# variables are injected by the container.
load_dotenv(PROJECT_ROOT / ".env", override=False)


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


def _required(name: str) -> str:
    """Read an environment variable, failing loudly if it is absent.

    Fail fast at startup with a clear message, rather than deep inside the
    pipeline with a confusing one.
    """
    value = os.getenv(name)
    if not value:
        raise ConfigError(
            f"Required environment variable {name!r} is not set. "
            f"Copy .env.example to .env and fill it in."
        )
    return value


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection details for one database."""

    host: str
    port: int
    user: str
    password: str
    database: str
    driver: str          # "mysql+pymysql" or "postgresql+psycopg2"
    connect_timeout: int = 10

    @property
    def url(self) -> str:
        """SQLAlchemy connection URL."""
        return (
            f"{self.driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def __repr__(self) -> str:
        # Never let a password reach a log file or a traceback.
        return (
            f"DatabaseConfig(host={self.host!r}, port={self.port}, "
            f"user={self.user!r}, database={self.database!r}, password='***')"
        )


def get_mysql_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3307")),
        user=_required("MYSQL_USER"),
        password=_required("MYSQL_PASSWORD"),
        database=_required("MYSQL_DATABASE"),
        driver="mysql+pymysql",
    )


def get_postgres_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("POSTGRES_ANALYTICS_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_ANALYTICS_PORT", "5433")),
        user=_required("POSTGRES_ANALYTICS_USER"),
        password=_required("POSTGRES_ANALYTICS_PASSWORD"),
        database=_required("POSTGRES_ANALYTICS_DB"),
        driver="postgresql+psycopg2",
    )