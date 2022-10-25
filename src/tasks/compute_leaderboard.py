from utils.statements import bills, date_range

from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql, DataFrame
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
        logger.info(model)
        df = read_sql(sql=model, con=conn)

        logger.info(f"Extracted User DF {df.shape}:" )
        logger.info(df.head())
        return (df, date_tup[0])

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()

@task(name="Process Leaderboard")
def process_leaderboard(tw, sdt):
    try: 
        logger = get_run_logger()

        # Generate and rename id
        df = DataFrame(data={'utility_type': [1, 2, 3, 4]})

        # Generate id
        ids = [uuid.uuid4() for _ in range(len(df.index))]
        df["id"] = ids

        # Insert stored_date_time and time_window
        df["time_window"] = tw
        df["stored_date_time"] = sdt

        # Process date
        current_datetime = datetime.now()
        df["date_created"] = current_datetime
        df["last_updated"] = current_datetime

        df = df[['id', 'utility_type', 'time_window', 'stored_date_time', 'date_created', 'last_updated']]
        logger.info(f"Process Leaderboard DF {df.shape}:")
        logger.info(df.head())
        return (df, ids)

    except Exception as e:
        logger.error("Data process error: " + str(e))
        raise ValueError()

