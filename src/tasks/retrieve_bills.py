from utils.statements import users
from utils.bill_formula import calc_usage, hour_dict

from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime, timedelta, timezone
import uuid

@task(name="Extract Users")
def extract_users():
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()
        df = read_sql(sql=users["get_users"], con=conn)

        logger.info(f"Extracted User DF {df.shape}:" )
        logger.info(df.head())

        return df
    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()

@task
def process_bills(df):
    try: 
        logger = get_run_logger()

        # Process usage data with hourly multiplier
        hour = (datetime.now(timezone.utc) + timedelta(hours=8)).hour
        df["electricity_used"] = df.apply(lambda row: calc_usage(row, 1, hour), axis=1).astype('int')
        df["gas_used"] = df.apply(lambda row: calc_usage(row, 2, hour), axis=1).astype('int')
        df["water_used"] = df.apply(lambda row: calc_usage(row, 3, hour), axis=1).astype('int')

        # Remove housing_type and household_members
        df.drop(['housing_type', 'household_members'], axis=1, inplace=True)

        # Insert for average user
        avg_user = ['2c7778a3-eb59-4403-9046-600d3e0725c3', df["electricity_used"].mean(), df["gas_used"].mean(), df["water_used"].mean()]
        df.loc[-1] = avg_user
        df.index = df.index + 1
        df = df.sort_index()

        # Generate id
        df["id"] = [uuid.uuid4() for _ in range(len(df.index))]

        # Process cost data
        df["electricity_cost"] = (df["electricity_used"] * 2.2).astype('int')
        df["gas_cost"] = (df["gas_used"] * 2).astype('int')
        df["water_cost"] = (df["water_used"] * 0.025).astype('int')
        df["total_cost"] = df["electricity_cost"] + df["gas_cost"] + df["water_cost"]

        # Process date
        current_datetime = (datetime.now(timezone.utc) + timedelta(hours=8)).replace(tzinfo=timezone(timedelta(hours=8)))
        prev_hour = current_datetime.replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)

        logger.info(f"Actual SDT: {prev_hour}")
        df["stored_date_time"] = prev_hour
        df["date_created"] = current_datetime
        df["last_updated"] = current_datetime

        logger.info(f"Process Bill DF {df.shape}:")
        logger.info(df.head(20))
        return df

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()

@task
def load_bills(df):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()
        rows_imported = df.to_sql(
            name="bills",
            con=conn,
            schema="public",
            if_exists='append',
            index=False)

        logger.info(f"Imported {rows_imported} rows into bills")

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()