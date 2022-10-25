FROM prefecthq/prefect:2.2.0-python3.9

RUN pip install --upgrade pip
RUN pip install sqlalchemy pandas psycopg2-binary

WORKDIR /app
COPY . /app/

ENTRYPOINT [ "prefect", "agent", "start", "-q", "etl" ]