from utils.statements import bills, date_range
from utils.compute_score import htd_dict

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

@task
def process_userleaderboard(df, ids):
    try: 
        logger = get_run_logger()

        # Balance usage
        df['balancer'] = df.apply(lambda row: htd_dict[row['housing_type']] / row['household_members'], axis=1)
        df['bal_electric'] = df['balancer'] * df['total_electric']
        df['bal_water'] = df['balancer'] * df['total_water']
        df['bal_gas'] = df['balancer'] * df['total_gas']

        # Reverse Normalize cols
        df['norm_electric'] = (1 - (df['bal_electric']-df['bal_electric'].mean())/df['bal_electric'].std())
        df['norm_water'] = (1 - (df['bal_water']-df['bal_water'].mean())/df['bal_water'].std())
        df['norm_gas'] = (1 - (df['bal_gas']-df['bal_gas'].mean())/df['bal_gas'].std())
        df['norm_total'] = (df['norm_electric']+df['norm_water']+df['norm_gas'])/3

        # Splitting df and Scoring df
        electric_df = proc_df(df[['user_id', 'norm_electric']].copy().rename(columns={ 'norm_electric':'norm_usage'}, inplace=True), ids[0])
        water_df = proc_df(df[['user_id', 'norm_water']].copy().rename(columns={ 'norm_water':'norm_usage'}, inplace=True), ids[1])
        gas_df = proc_df(df[['user_id', 'norm_gas']].copy().rename(columns={ 'norm_gas':'norm_usage'}, inplace=True), ids[2])
        total_df = proc_df(df[['user_id', 'norm_total']].copy().rename(columns={ 'norm_total':'norm_usage'}, inplace=True), ids[3])
        
        # Concat results
        proc_df = (pd.concat([electric_df, water_df, gas_df, total_df]))
        proc_df["id"] = [uuid.uuid4() for _ in range(len(proc_df.index))]

        # Process date
        current_datetime = datetime.now()
        proc_df["date_created"] = current_datetime
        proc_df["last_updated"] = current_datetime

        logger.info(f"Process UserLeaderboard DF {proc_df.shape}:")
        logger.info(proc_df.head())
        return proc_df

    except Exception as e:
        logger.error("Data process error: " + str(e))
        raise ValueError()