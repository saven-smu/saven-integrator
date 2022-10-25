from utils.statements import users
from utils.bill_formula import calc_usage, hour_dict

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime, timedelta
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

        # Generate and rename id
        df.rename(columns={ 'id':'user_id'}, inplace=True)
        df["id"] = [uuid.uuid4() for _ in range(len(df.index))]

        # Process usage data with hourly multiplier
        hm = hour_dict[datetime.now().hour]
        df["electricity_used"] = df.apply(lambda row: calc_usage(row, 1) * hm, axis=1)
        df["gas_used"] = df.apply(lambda row: calc_usage(row, 2) * hm, axis=1)
        df["water_used"] = df.apply(lambda row: calc_usage(row, 3) * hm, axis=1)

        # Process cost data
        df["electricity_cost"] = (df["electricity_used"] * 2.2).astype('int')
        df["gas_cost"] = (df["gas_used"] * 2).astype('int')
        df["water_cost"] = (df["water_used"] * 0.025).astype('int')
        df["total_cost"] = df["electricity_cost"] + df["gas_cost"] + df["water_cost"]

        # Process date
        current_datetime = datetime.utcnow()
        prev_hour = current_datetime.replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)

        df["stored_date_time"] = prev_hour
        df["date_created"] = current_datetime
        df["last_updated"] = current_datetime

        # Remove housing_type and household_members
        df.drop(['housing_type', 'household_members'], axis=1, inplace=True)

        logger.info(f"Process Bill DF {df.shape}:")
        logger.info(df.head())
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

@flow(name="Retrieve Bills")
def retrieve_bills():
    # Extract user from database
    df = extract_users()
    # Process bills for given user
    proc_df = process_bills(df)
    # Load bills into database
    load_bills(proc_df)

if __name__ == "__main__":
    retrieve_bills()