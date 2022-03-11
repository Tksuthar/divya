import logging
import subprocess


logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


class CosmosConnectionString:

    def get_connection_string(self, resource_group, cosmos_group, sub_id):

        try:
            logging.info("Fetching Cosmos Connection String")
            create_command = "az cosmosdb keys list -n " + cosmos_group + " -g " + resource_group + " --subscription " \
                             + sub_id + " --type connection-strings --query 'connectionStrings[0].connectionString'"
            logging.info(create_command)
            # running the AZURE CLI command
            create_app = subprocess.run(create_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # reading output and error
            create_app_stdout = create_app.stdout.decode("utf-8")
            create_app_stderr = create_app.stderr.decode("utf-8")
            logging.info("Stdout logs for cosmos resource creation " + create_app_stdout)
            logging.info("Stderr logs for cosmos resource creation " + create_app_stderr)
            logging.info("Cosmos Connection string fetched successfully................\n\n")
            return create_app_stdout

        except Exception as e:
            logging.error("Unexpected error because of CosmosCLI create_cosmos function :{}".format(e))
