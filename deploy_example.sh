prefect deployment build ./src/sync_data.py:retrieve_bills -n data-sync -q etl
prefect deployment build ./src/test_svc.py:test_flow -n test-svc -q test

prefect agent start -q test