from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from adf_creation import *
from create_cosmos_db_account import CreateCosmosDBAccount
from Database_And_Container_Creation.create_mongodb_cosmos_dbs_cols import CreateMongoDBCosmosCollections
from Database_And_Container_Creation.create_sql_cosmos_dbs_cols import CreateSqlCosmosContainers
import pyodbc
import subprocess
from pymongo import MongoClient
from sql_queries import SqlQueries
import logging
from config import Config, SourceSink


logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return "Welcome to data migration app"


@app.route('/signup', methods=['POST'])
@cross_origin()
def signup():

    try:
        data = request.get_json()
        user_signup = SqlQueries()
        check_username = user_signup.check_user_existence(data['username'], data['password'])

        if not check_username:
            user_signup.insert_user_details(data['firstname'], data['lastname'], data['username'], data['password'])
            user_signup.close_connection()
            return "Account created successfully"

    except Exception as ex:
        logging.error("Exception occurred in signup function of MigrationMain {}".format(ex))
    return "Account already exists. Login with your username and password"


@app.route('/login', methods=['POST'])
@cross_origin()
def login():

    try:
        username = request.json['username']
        password = request.json['password']

        user_login = SqlQueries()
        check_username = user_login.check_user_existence(username, password)
        user_login.close_connection()

        if not check_username:
            return "Account does not exists. Signup to create an account"
        return "Login successful"

    except Exception as ex:
        logging.error("Exception occurred in signup function of MigrationMain {}".format(ex))
    return "Invalid login"


@app.route('/create_uri', methods=['POST'])
@cross_origin()
def create_uri():

    try:
        user_uri = request.json['uri']
        username = request.json['username']
        account_type = request.json['account_type']
        driver_name = request.json['driver_name']
        uri_creation = SqlQueries()

        if account_type == 'mongodb':
            if not uri_creation.check_mongodb_uri_existence(username, user_uri):
                client = MongoClient(user_uri)
                client.server_info()
                uri_creation.insert_mongodb_credentials(username, user_uri)
                return "MongoDB connection string is validated"

        else:
            if not uri_creation.check_sql_uri_existence(username, user_uri):
                sql_uri = "Driver=" + driver_name + "};" + user_uri
                connection = pyodbc.connect(sql_uri)
                cursor = connection.cursor()
                uri_creation.insert_sql_credentials(username, user_uri)
                cursor.close()
                connection.close()
            return "SQL connection string is validated"

        uri_creation.close_connection()
        return "Select your connection string from dropdown"

    except Exception as ex:
        logging.error("Invalid connection string", ex)
        return "Invalid Connection String\nMongoDB connection string should be of version 3.3 or 3.4\nSQL connection" \
               " string should not contain hyphens or special characters"


@app.route('/fetch_uris', methods=['GET'])
@cross_origin()
def fetch_uris():

    try:
        username = request.args['username']
        account_type = request.args['account_type']
        get_uris = SqlQueries()

        if account_type == 'mongodb':
            uris = get_uris.get_mongodb_uris_with_username(username)
        else:
            uris = get_uris.get_sql_uris_with_username(username)
        return jsonify(uris)

    except Exception as ex:
        logging.error("Unable to fetch URIs", ex)
    return ""


@app.route('/fetch_tables', methods=['POST'])
@cross_origin()
def fetch_tables():

    try:
        user_uri = request.json['uri']
        account_type = request.json['account_type']
        driver_name = request.json['driver_name']

        if account_type == 'mongodb':
            client = MongoClient(user_uri)
            databases = {}

            # creating nested dictionary of databases, collection and shard keys
            for db in client.list_database_names():
                if db == 'local':
                    continue
                temp = {}  # for storing collection name as key and column names as its value in the dictionary

                for col in client[db].list_collection_names():
                    fields = []  # for storing field or column names
                    query = client[db].get_collection(col).find_one()  # searching or finding random query

                    for field, value in query.items():  # traversing the query
                        fields.append(field)  # adding columns or field in to the list

                    if fields:
                        temp[col] = fields  # storing collection name as key and list of columns as its value
                if temp:
                    databases[db] = temp
                    # storing database as key and dictionary of collection name and columns as its value
            return jsonify(databases)

        else:
            sql_uri = "Driver=" + driver_name + "};" + user_uri
            conn = pyodbc.connect(sql_uri)
            cursor = conn.cursor()
            cursor.execute("SELECT db_name()")
            result0 = cursor.fetchall()

            cursor.execute("SELECT name FROM sys.tables")
            result1 = cursor.fetchall()
            column_names = {}

            for i in range(len(result1)):
                table_names = []
                table_name = result1[i][0]
                cursor.execute("SELECT column_name from INFORMATION_SCHEMA.columns where table_name = '{}'".format(
                    table_name))
                result2 = cursor.fetchall()

                for j in range(len(result2)):
                    table_names.append(result2[j][0])
                column_names[table_name] = table_names

            cursor.close()
            conn.close()
            return jsonify({result0[0][0]: column_names})

    except Exception as ex:
        logging.error("Exception occurred in fetching databases and collections {}".format(ex))
    return jsonify("Not able to fetch the databases and collections")


@app.route('/create_cosmos_resource', methods=['POST'])
@cross_origin()
def create_cosmos_resource():

    try:
        username = request.json['username']
        account_type = request.json['account_type']
        cosmos_cli = CreateCosmosDBAccount()

        if cosmos_cli.create_cosmos_resource(Config['resource_group'], Config['SUB_ID'], account_type):
            cosmos_creation = SqlQueries()

            if account_type == 'mongodb':
                cosmos_creation.update_mongodb_cosmos_name(username, SourceSink[account_type]['cosmos_group'])
            else:
                cosmos_creation.update_sql_cosmos_name(username, SourceSink[account_type]['cosmos_group'])

        return "Created cosmos {} resource successfully".format(SourceSink[account_type]['cosmos_group'])

    except Exception as ex:
        logging.error("Exception occurred in call_migration function of MigrateMain {}".format(ex))
    return "Cosmos resource creation is unsuccessful"


@app.route('/create_databases', methods=['POST'])
@cross_origin()
def create_databases():

    try:
        migration_dbs_cols = request.get_json()
        account_type = migration_dbs_cols['account_type']

        if account_type == "mongodb":
            create_cosmos_client = CreateMongoDBCosmosCollections()
        else:
            create_cosmos_client = CreateSqlCosmosContainers()

        if create_cosmos_client.create_all_databases(migration_dbs_cols["form_value"]) and\
                create_cosmos_client.create_all_containers(migration_dbs_cols["form_value"]):
            return "Created databases and collections successfully"

    except Exception as ex:
        logging.error("Exception occurred in create_databases function of MigrateMain {}".format(ex))
    return "Databases and collections creation is unsuccessful"


@app.route('/create_data_factory', methods=['POST'])
@cross_origin()
def create_data_factory():

    try:
        username = request.json['username']
        account_type = request.json['account_type']
        data_factory_name = SourceSink[account_type]['data_factory_name']
        create_adf = CreateAdfResource()

        if create_adf.create_df(Config['resource_group'], data_factory_name, Config['AZURE_CLIENT_ID'],
                                Config['AZURE_CLIENT_SECRET'], Config['AZURE_TENANT_ID'], Config['SUB_ID']):
            df_creation = SqlQueries()

            if account_type == "mongodb":
                df_creation.update_mongodb_adf_name(username, data_factory_name)
            else:
                df_creation.update_sql_adf_name(username, data_factory_name)

        return "Created data factory {} successfully".format(data_factory_name)

    except Exception as ex:
        logging.error("Exception occurred in create_data_factory function of MigrateMain {}".format(ex))
    return "Data factory creation is unsuccessful"


@app.route('/create_pipeline', methods=['POST'])
@cross_origin()
def create_pipeline():

    try:
        dbs_cols = request.get_json()
        create_adf_pipeline = CreateAdfResource()
        credentials = ClientSecretCredential(client_id=Config['AZURE_CLIENT_ID'],
                                             client_secret=Config['AZURE_CLIENT_SECRET'],
                                             tenant_id=Config['AZURE_TENANT_ID'])

        adf_client = DataFactoryManagementClient(credentials, Config['SUB_ID'])
        migration_dbs_cols = dbs_cols["form_value"]
        account_type = dbs_cols['account_type']

        for i in range(len(migration_dbs_cols)):
            database_name = migration_dbs_cols[i]['database_name']
            collection = migration_dbs_cols[i]['table']

            if not create_adf_pipeline.run_all(dbs_cols['uri'], adf_client, account_type, database_name, collection,
                                               SourceSink[account_type]['cosmos_group'],
                                               SourceSink[account_type]['data_factory_name'],
                                               SourceSink[account_type]['source_linked_service'],
                                               SourceSink[account_type]['sink_linked_service'],
                                               SourceSink[account_type]['source_dataset'],
                                               SourceSink[account_type]['sink_dataset']):
                return "Pipeline creation is unsuccessful"
        return "Created pipeline successfully"

    except Exception as ex:
        logging.error("Exception occurred in create_pipeline function of MigrateMain {}".format(ex))
    return "Pipeline creation is unsuccessful"


@app.route('/run_pipeline', methods=['POST'])
@cross_origin()
def run_pipeline():

    try:
        migration_dbs_cols = request.get_json()
        dbs_cols = migration_dbs_cols["form_value"]
        account_type = migration_dbs_cols['account_type']

        for i in range(len(migration_dbs_cols)):
            collection = dbs_cols[i]['table']
            pipeline_execution = CreateAdfResource()

            if not pipeline_execution.run_monitor_pipeline(collection, Config['resource_group'],
                                                           SourceSink[account_type]['data_factory_name'],
                                                           Config['AZURE_CLIENT_ID'], Config['AZURE_CLIENT_SECRET'],
                                                           Config['AZURE_TENANT_ID'], Config['SUB_ID'],
                                                           SourceSink['pipeline_name']):
                return "Data migration is unsuccessful"
        return "Data migrated successfully"

    except Exception as ex:
        logging.error("Exception occurred in run_pipeline function of MigrateMain {}".format(ex))
    return "Data migration is unsuccessful"


if __name__ == '__main__':
    app.run(debug=True)
