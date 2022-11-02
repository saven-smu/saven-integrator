from utils.statements import bills, users, date_range
from utils.compute_score import htd_dict, proc_df, reverse_mm_normalize

from random import randint
from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, concat
from datetime import datetime, timezone, timedelta
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

        logger.info(f"Extracted Bills DF {df.shape}:" )
        return (df, date_tup[0])

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()

@task(name="Extract User Credits")
def extract_user_creds():
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()

        df = read_sql(sql=users["get_users_credits"], con=conn)

        logger.info(f"Extracted User Credits DF {df.shape}:" )
        return df

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

        actl_sdt = sdt.replace(tzinfo=timezone(timedelta(hours=8)))
        logger.info(f"Actual SDT: {actl_sdt}")
        df["stored_date_time"] = actl_sdt

        # Process date
        current_datetime = (datetime.now(timezone.utc) + timedelta(hours=8)).replace(tzinfo=timezone(timedelta(hours=8)))
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
        df['balancer'] = df.apply(lambda row: htd_dict[row['housing_type'].lower()] / row['household_members'], axis=1)
        df['bal_electric'] = df['balancer'] * df['total_electric']
        df['bal_water'] = df['balancer'] * df['total_water']
        df['bal_gas'] = df['balancer'] * df['total_gas']

        # Reverse MinMax Normalization on cols
        df['norm_electric'] = reverse_mm_normalize(df['bal_electric'])
        df['norm_water'] = reverse_mm_normalize(df['bal_water'])
        df['norm_gas'] = reverse_mm_normalize(df['bal_gas'])
        df['norm_total'] = (df['norm_electric']+df['norm_water']+df['norm_gas'])/3

        # Splitting df and Scoring df
        electric_df = proc_df(df[['user_id', 'norm_electric']].copy().rename(columns={ 'norm_electric':'norm_usage'}), ids[0])
        water_df = proc_df(df[['user_id', 'norm_water']].copy().rename(columns={ 'norm_water':'norm_usage'}), ids[1])
        gas_df = proc_df(df[['user_id', 'norm_gas']].copy().rename(columns={ 'norm_gas':'norm_usage'}), ids[2])
        total_df = proc_df(df[['user_id', 'norm_total']].copy().rename(columns={ 'norm_total':'norm_usage'}), ids[3])

        # Concat results
        ulb_df = (concat([electric_df, water_df, gas_df, total_df]))
        ulb_df["id"] = [uuid.uuid4() for _ in range(len(ulb_df.index))]

        # Process date
        current_datetime = (datetime.now(timezone.utc) + timedelta(hours=8)).replace(tzinfo=timezone(timedelta(hours=8)))
        ulb_df["date_created"] = current_datetime
        ulb_df["last_updated"] = current_datetime

        logger.info(f"Process UserLeaderboard DF {ulb_df.shape}:")
        logger.info(ulb_df.head())
        return (ulb_df, total_df)

    except Exception as e:
        logger.error("Data process error: " + str(e))
        raise ValueError()

@task
def load_df(df, tbl_name):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()
        rows_imported = df.to_sql(
            name=tbl_name,
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

@task
def reward_credits(df, cred_df):
    try:
        logger = get_run_logger()
        rds_engine = create_engine(Secret.load("saven-rds").get())
        conn = rds_engine.connect()

        top_user = df.sort_values(by=["computed_score"], ascending=False).head(5)["user_id"].tolist()
        with conn.begin():
            for idx, uid in enumerate(top_user):
                new_credit = cred_df[cred_df['user_id'] == uid]['credits'].values[0] + int(randint(500, 1000)/(idx+1));
                model = users["upd_users"].format(new_credit, uid)
                logger.info(model)
                res = conn.execute(model)
                logger.info(f"Updated User {uid}: {res}")

    except Exception as e:
        logger.error("Data extract error: " + str(e))
        raise ValueError()
    finally:
        if conn:
            conn.close()
            