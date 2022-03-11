import subprocess
import logging
from config import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


class CreateSqlCosmosContainers:

    def create_all_databases(self, migration_dbs_cols):

        try:
            resource_group = Config['resource_group']
            sql_cosmos_group = SourceSink['azure_sql_database']['cosmos_group']
            # iterating through all the databases for creating cosmos DB using single db creation approach
            created = []

            for i in range(len(migration_dbs_cols)):
                database_name = migration_dbs_cols[i]['database_name']
                if database_name not in created:

                    logging.info("{dbname}".format(dbname=database_name))
                    # using azure cli command and storing it in variable create_app_command
                    create_app_command = "az cosmosdb sql database create --account-name " + sql_cosmos_group + \
                                         " --name " + database_name + " --resource-group " + resource_group + \
                                         " --subscription " + Config['SUB_ID']
                    logging.info("{cac}".format(cac=create_app_command))

                    # running the azure cli command using subprocess
                    create_app = subprocess.run(create_app_command, shell=True, stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
                    # reading output and error
                    create_app_stdout = create_app.stdout.decode("utf-8")
                    create_app_stderr = create_app.stderr.decode("utf-8")
                    logging.info("Stdout log for create_all_databases " + create_app_stderr)
                    logging.info("Stderr log for create_all_databases " + create_app_stdout)
                    logging.info("{db} Database updated or created".format(db=database_name))
                    created.append(database_name)

            return True

        except Exception as e:
            logging.error("Unexpected error because of create_all_databases function {ex}".format(ex=e))
        return False

    def create_all_containers(self, migration_dbs_cols):

        try:
            # iterating through dbname, col name and shard key for creating collections using single collection
            # creation approach
            resource_group = Config['resource_group']
            sql_cosmos_group = SourceSink['azure_sql_database']['cosmos_group']

            for i in range(len(migration_dbs_cols)):
                database_name = migration_dbs_cols[i]['database_name']
                partition_key = migration_dbs_cols[i]['partition_key']
                container_name = migration_dbs_cols[i]['table_name']

                create_app_command = "az cosmosdb sql container create -a " + sql_cosmos_group + " -g " + \
                                     resource_group + " --subscription " + Config['SUB_ID'] + " -d " + database_name + \
                                     " -n " + container_name + " --partition-key-path '/" + partition_key + "'"

                logging.info("{cac}".format(cac=create_app_command))
                create_app = subprocess.run(create_app_command, shell=True, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)

                # reading output and error
                create_app_stdout = create_app.stdout.decode("utf-8")
                create_app_stderr = create_app.stderr.decode("utf-8")
                logging.info("Stderr logs for create_all_containers " + create_app_stderr)
                logging.info("Stdout logs for create_all_containers " + create_app_stdout)
                logging.info("{col} Container created or updated ".format(col=container_name))

            return True

        except Exception as e:
            logging.error("Unexpected error because of create_all_containers function : {ex}".format(ex=e))
        return False
