import subprocess
import logging
from config import SourceSink

logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


class CreateCosmosDBAccount:

    def create_cosmos_resource(self, resource_group, sub_id, account_type):

        try:
            mongodb_cosmos_group = SourceSink['mongodb']['cosmos_group']
            sql_cosmos_group = SourceSink['azure_sql_database']['cosmos_group']
            logging.info("INITIALIZING COSMOS RESOURCE DEPLOYMENT ==> \n\n\n")

            # storing the Azure CLI command in create_command
            if account_type == "mongodb":
                create_command = "az cosmosdb create --name " + mongodb_cosmos_group + " --resource-group " + \
                                 resource_group + " --subscription " + sub_id + " --kind MongoDB"
            else:
                create_command = "az cosmosdb create --name " + sql_cosmos_group + " --resource-group " + \
                                 resource_group + " --subscription " + sub_id
            logging.info(create_command)

            # running the AZURE CLI command
            create_app = subprocess.run(create_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            create_app_stdout = create_app.stdout.decode("utf-8")
            create_app_stderr = create_app.stderr.decode("utf-8")

            logging.info("Stdout logs for cosmos resource creation " + create_app_stdout)
            logging.info("Stderr logs for cosmos resource creation " + create_app_stderr)
            logging.info("COSMOS RESOURCE DEPLOYMENT DONE................\n\n")
            return True

        except Exception as e:
            logging.error("Unexpected error because of CosmosCLI create_cosmos function :{}".format(e))
        return False
