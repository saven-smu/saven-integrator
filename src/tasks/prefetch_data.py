from utils.statements import bills, users
from utils.bill_formula import calc_usage, hour_dict

from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, concat
from datetime import datetime, timezone, timedelta
import uuid

@task(name="Drop User Bills")
def drop_user_bills(uid):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()
        
        with conn.begin():
            model = bills["delete_bills_id"].format(uid)

            res = conn.execute(model)
            logger.info(f"Drop user bills {uid}: {res}")

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()

@task(name="Extract Single User")
def extract_single_user(uid):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()

        model = users["get_user_id"].format(uid)
        df = read_sql(sql=model, con=conn)

        logger.info(f"Extracted User DF {df.shape}:" )
        logger.info(df.head())

        return df.head(1)
    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()

@task(name="Process Single Bill")
def process_single_bills(dfo):
    try: 
        logger = get_run_logger()
        df = concat([dfo]*24,ignore_index=True)
        
        # Process date
        current_datetime = (datetime.now(timezone.utc) + timedelta(hours=8)).replace(tzinfo=timezone(timedelta(hours=8)))
        prev_hour = current_datetime.replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)
        hour_list = [prev_hour - timedelta(hours=x) for x in range(24)]

        logger.info(f"Actual SDT: {prev_hour}")
        df["stored_date_time"] = hour_list
        df["date_created"] = current_datetime
        df["last_updated"] = current_datetime

        # Process usage data with hourly multiplier
        df["electricity_used"] = df.apply(lambda row: calc_usage(row, 1, row['stored_date_time'].hour), axis=1).astype('int')
        df["gas_used"] = df.apply(lambda row: calc_usage(row, 2, row['stored_date_time'].hour), axis=1).astype('int')
        df["water_used"] = df.apply(lambda row: calc_usage(row, 3, row['stored_date_time'].hour), axis=1).astype('int')

        # Remove housing_type and household_members
        df.drop(['housing_type', 'household_members'], axis=1, inplace=True)

        # Generate id
        df["id"] = [uuid.uuid4() for _ in range(len(df.index))]

        # Process cost data
        df["electricity_cost"] = (df["electricity_used"] * 2.2).astype('int')
        df["gas_cost"] = (df["gas_used"] * 2).astype('int')
        df["water_cost"] = (df["water_used"] * 0.025).astype('int')
        df["total_cost"] = df["electricity_cost"] + df["gas_cost"] + df["water_cost"]

        logger.info(f"Process Bill DF {df.shape}:")
        logger.info(df.head())
        return df

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
