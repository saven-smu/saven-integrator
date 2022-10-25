from tasks.retrieve_bills import extract_users, process_bills, load_bills
from tasks.compute_leaderboard import extract_bills

from prefect import flow

@flow(name="Retrieve Bills")
def retrieve_bills():
    # Extract user from database
    df = extract_users()
    # Process bills for given user
    proc_df = process_bills(df)
    # Load bills into database
    load_bills(proc_df)

@flow(name="Compute Leaderboard")
def compute_leaderboard(type):
    # Extract bills from database
    df = extract_bills(type)
    # Process bills for given user
    # proc_df = process_bills(df)
    # Load bills into database
    # load_bills(proc_df)

if __name__ == "__main__":
    # Test Flows
    # retrieve_bills()
    compute_leaderboard(1)
    pass