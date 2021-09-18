import psycopg2
import kopf
from kubernetes import client, config
import os

def initialize_db():

    PSQL_HOST = os.getenv("PSQL_HOST", "184.73.10.62")
    PSQL_USERNAME = os.getenv("PSQL_USERNAME", "postgres")
    PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "password")
    PSQL_DB = os.getenv("PSQL_DB", "postgres")

    return psycopg2.connect(database=PSQL_DB, user=PSQL_USERNAME, password=PSQL_PASSWORD, host=PSQL_HOST)

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

db_connection = initialize_db()
initialize_kube()

def insert_data(table, primary_id, name, age, country):
    insert_query = """INSERT INTO {table_name} (id, name, age, country) VALUES ('{id}', '{name}', {age}, '{country}')""".format(
        table_name=table, 
        id=primary_id, 
        name=name, 
        age=age, 
        country=country
    )
    
    with db_connection.cursor() as cursor:
        cursor.execute(insert_query)
        db_connection.commit()

def delete_data(table, primary_id):
    delete_query = """DELETE FROM {table_name} WHERE id='{id}'""".format(
        table_name=table, 
        id=primary_id
    )

    with db_connection.cursor() as cursor:
        cursor.execute(delete_query)
        db_connection.commit()

@kopf.on.create("demo.yash.com", "v1", "postgres-writer")
def create_fn(spec, **kwargs):
    resource_namespace = kwargs["body"]["metadata"]["namespace"]
    resource_name = kwargs["body"]["metadata"]["name"]
    spec = kwargs["body"]["spec"]
    
    primary_id = resource_namespace + "/" + resource_name
    table, name, age, country = spec["table"], spec["name"], spec["age"], spec["country"]

    insert_data(table, primary_id, name, age, country)
    
    return "Successfully wrote data corresponding to id: {id}, name: {name}, age: {age}, country: {country}".format(
        id=primary_id, 
        name=name, 
        age=age, 
        country=country
    )

@kopf.on.delete("demo.yash.com", "v1", "postgres-writer")
def delete_fn(spec, **kwargs):
    resource_namespace = kwargs["body"]["metadata"]["namespace"]
    resource_name = kwargs["body"]["metadata"]["name"]
    spec = kwargs["body"]["spec"]
    
    primary_id = resource_namespace + "/" + resource_name
    table = spec["table"]

    delete_data(table, primary_id)

    return "Successfully delete data corresponding to id: {id}".format(id=primary_id)