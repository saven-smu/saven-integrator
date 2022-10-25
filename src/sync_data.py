from tasks.retrieve_bills import extract_users, process_bills, load_bills
from tasks.compute_leaderboard import extract_bills, process_leaderboard

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
def compute_leaderboard(time_window):
    # Extract bills from database
    bill_res = extract_bills(time_window)
    # Process bills for given user
    lb_res = process_leaderboard(time_window, bill_res[1])
    # Load bills into database
    ulb_df = process_userleaderboard(bill_res[0], lb_res[1])

if __name__ == "__main__":
    # Test Flows
    # retrieve_bills()
    compute_leaderboard(1)
    pass