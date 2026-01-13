from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from .settings import settings

connect_args: dict[str, str] = {}
if settings.DB_STATEMENT_TIMEOUT_SECONDS > 0:
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
