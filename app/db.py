from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from .settings import settings

def _supports_statement_timeout(database_url: str) -> bool:
    try:
        return make_url(database_url).get_backend_name() == "postgresql"
    except Exception:
        return database_url.startswith("postgres")


connect_args: dict[str, object] = {}
if settings.DB_STATEMENT_TIMEOUT_SECONDS > 0 and _supports_statement_timeout(settings.DATABASE_URL):
    timeout_ms = int(settings.DB_STATEMENT_TIMEOUT_SECONDS * 1000)
    connect_args["options"] = f"-c statement_timeout={timeout_ms}"

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
