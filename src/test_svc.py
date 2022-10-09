from statements import test
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import create_engine
from pandas import read_sql

@task
def test_db():
    try:
        logger = get_run_logger()
        pg_engine = create_engine(Secret.load("saven-rds").get())
        conn = pg_engine.connect()
        df = read_sql(sql=test["test_conn"], con=conn)

        logger.info("Connection from RDS Successful")
        logger.info(df.head())
    except Exception as e:
        logger.error("Data extract error: " + str(e))
    finally:
        if conn:
            conn.close()

@task
def print_version(version):
    # load current saven version
    get_run_logger().info(f"Running Saven-Integrator {version}")

@flow(name="Test Flow")
def test_flow(version):
    print_version(version)
    test_db()

if __name__ == "__main__":
    test_flow("v0.0.1")