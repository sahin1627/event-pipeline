import pandas as pd
from sqlalchemy import create_engine


def load_parquet_to_staging():
    # Create SQLAlchemy engine
    engine = create_engine(
        "postgresql+psycopg2://admin:admin@localhost:5433/warehouse"
    )

    # source
    PARQUET_PATH = "../data/raw/product_events"
    # target table in postgres
    TARGET_SCHEMA = "db_staging"
    TARGET_TABLE = "product_events_raw"

    parquet_df = pd.read_parquet(PARQUET_PATH)

    parquet_df.to_sql(TARGET_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="delete_rows", index=False)

load_parquet_to_staging()
