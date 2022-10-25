from utils.statements import bills, date_range

from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime, timedelta
import uuid

@task(name="Extract Bills")
def extract_bills(type):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()

        date_tup = date_range(type)
        model = bills["get_bills"].format(date_tup[0], date_tup[1])
        df = read_sql(sql=model, con=conn)

        logger.info(f"Extracted User DF {df.shape}:" )
        logger.info(df.head())
        return df
    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()