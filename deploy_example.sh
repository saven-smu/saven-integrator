# Build deployment file
prefect deployment build ./src/test_svc.py:test_flow -n test-svc -q test
# Apply deployment to prefect cloud
prefect deployment apply test_flow-deployment.yaml
# Start local agent
prefect agent start -q test