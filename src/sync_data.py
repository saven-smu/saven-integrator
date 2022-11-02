from tasks.retrieve_bills import extract_users, process_bills, load_bills
from tasks.compute_leaderboard import *

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
    (bdf, sdt) = extract_bills(time_window)
    # Extract user credits from database
    user_cred_df = extract_user_creds()
    # Process leaderboard for given user
    (ldf, ids) = process_leaderboard(time_window, sdt)
    # Load leaderboard into database
    load_df(ldf, "leaderboards")
    # Process user leaderboard for given user 
    (ulb_df, tot_df) = process_userleaderboard(bdf, ids)
    # Load user leaderboard into database
    load_df(ulb_df, "userleaderboards")
    # Reward user credits
    reward_credits(tot_df, user_cred_df)

if __name__ == "__main__":
    # Test Flows
    # retrieve_bills()
    compute_leaderboard(1)
    pass