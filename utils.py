from kubernetes import config
from clients.postgres import PostgresClient
import os

def postgres_client_from_env():
    PSQL_HOST = os.getenv("PSQL_HOST")
    PSQL_USERNAME = os.getenv("PSQL_USERNAME")
    PSQL_PASSWORD = os.getenv("PSQL_PASSWORD")
    PSQL_DB = os.getenv("PSQL_DB")

    client = PostgresClient(db=PSQL_DB, username=PSQL_USERNAME, password=PSQL_PASSWORD, host=PSQL_HOST)
    client.connect_if_not_connected()
    return client

def initialize_kube():
    DEV = os.getenv('DEV')
    if DEV:
        print ("Loading from local kube config")
        home = os.path.expanduser("~")
        kube_config_path = os.getenv("KUBE_CONFIG", home+"/.kube/config")
        config.load_kube_config(config_file=kube_config_path)
    else:
        print ("Loading In-cluster config")
        config.load_incluster_config()