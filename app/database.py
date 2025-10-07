from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, declarative_base

DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/teste"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

Base = declarative_base()