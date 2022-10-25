from datetime import datetime, timedelta

users = {
    "get_users": """
        SELECT  id, housing_type, household_members
        FROM USERS;
    """
}

bills = {
    "get_bills": """
        SELECT usr.id as user_id, usr.housing_type, usr.household_members, sum(bil.electricity_used) as total_electric, sum(bil.water_used) as total_water, sum(bil.gas_used) as total_gas
        FROM BILLS as bil
        INNER JOIN USERS as usr
        ON usr.id = bil.user_id
        WHERE bil.stored_date_time >= '{0}' AND bil.stored_date_time < '{1}'
        GROUP BY usr.id, usr.housing_type, usr.household_members;
    """
}

test = {
    "test_conn": """
        SELECT 1;
    """
}

def date_range(type):
    tdy = datetime.utcnow().replace(microsecond=0, second=0, minute=0, hour=0)
    prev = None;
    if type == 1:
        prev = tdy - timedelta(days=1)
    elif type == 2:
        prev = tdy - timedelta(days=6)
    else:
        prev = tdy.replace(month=tdy.month - 1)
    return (prev, tdy)