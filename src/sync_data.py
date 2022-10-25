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

if __name__ == "__main__":
    retrieve_bills()