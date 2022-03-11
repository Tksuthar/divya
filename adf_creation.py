from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time
from cosmos_connection_string import CosmosConnectionString
from config import *


class CreateAdfResource:

    @staticmethod
    def print_item(group):
        print("\tName: {}".format(group.name))
        print("\tId: {}".format(group.id))
        if hasattr(group, 'location'):
            print("\tLocation: {}".format(group.location))
        if hasattr(group, 'tags'):
            print("\tTags: {}".format(group.tags))
        if hasattr(group, 'properties'):
            CreateAdfResource.print_properties(group.properties)

    @staticmethod
    def print_properties(props):
        """Print a ResourceGroup properties instance."""
        if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
            print("\tProperties:")
            print("\t\tProvisioning State: {}".format(props.provisioning_state))
        print("\n\n")

    @staticmethod
    def print_activity_run_details(activity_run):
        """Print activity run details."""
        print("\n\tActivity run details\n")
        print("\tActivity run status: {}".format(activity_run.status))
        if activity_run.status == 'Succeeded':
            print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
            print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
            print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
        else:
            print("\tErrors: {}".format(activity_run.error['message']))

    def create_df(self, rg_name, df_name, azure_client_id, azure_client_secret, azure_tenant_id, sub_id):

        try:
            credentials = ClientSecretCredential(client_id=azure_client_id, client_secret=azure_client_secret,
                                                 tenant_id=azure_tenant_id)
            adf_client = DataFactoryManagementClient(credentials, sub_id)
            # Specify your Active Directory client ID, client secret, and tenant ID
            df_resource = Factory(location='westus')
            df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
            self.print_item(df)
            while df.provisioning_state != 'Succeeded':
                df = adf_client.factories.get(rg_name, df_name)
                time.sleep(1)

        except Exception as e:
            print("Unexpected error while creating adf resource in create_df function :", e)

    def source_linked_service(self, uri, adf_client, database_name, resource_group, data_factory_name,
                              source_linked_service):

        try:
            # Create an Azure Storage linked service
            storage_string = SecureString(value=uri)
            ls_name1 = database_name + "_sourceLinkedService"
            ls_azure_storage = LinkedServiceResource(properties=eval(source_linked_service)(
                connection_string=storage_string, database=database_name))
            ls = adf_client.linked_services.create_or_update(resource_group, data_factory_name, ls_name1,
                                                             ls_azure_storage)
            self.print_item(ls)

        except Exception as e:
            print("Unexpected error because of AdfMain source_linked_service function :", e)

    def sink_linked_service(self, adf_client, account_type, database_name, resource_group, cosmos_group,
                            data_factory_name, sink_linked_service, sub_id):

        try:
            connection = CosmosConnectionString()
            cosmos_string = connection.get_connection_string(resource_group, cosmos_group, sub_id)
            cosmos_string = cosmos_string.replace('"', '')

            if account_type == 'azure_sql_database':
                cosmos_string += "Database=" + database_name

            storage_string = SecureString(value=cosmos_string)
            ls_name2 = database_name + "_sinkLinkedService"
            ls_azure_storage = LinkedServiceResource(properties=eval(sink_linked_service)(
                connection_string=storage_string, database=database_name))

            if account_type == 'mongodb':
                ls_azure_storage = LinkedServiceResource(properties=eval(sink_linked_service)(
                    connection_string=storage_string, database=database_name, is_server_version_above32=True))

            ls = adf_client.linked_services.create_or_update(resource_group, data_factory_name, ls_name2,
                                                             ls_azure_storage)
            self.print_item(ls)

        except Exception as e:
            print("Unexpected error because of AdfMain sink_linked_service function :", e)

    def source_dataset(self, adf_client, account_type, database_name, col_name, resource_group, data_factory_name,
                       source_dataset):

        try:
            ls_name1 = database_name + "_sourceLinkedService"
            ds_ls1 = LinkedServiceReference(reference_name=ls_name1)
            ds_in_name = col_name + "_" + database_name + "_source_dataset"

            if account_type == 'mongodb':
                ds_in_azure_blob = DatasetResource(properties=eval(source_dataset)(linked_service_name=ds_ls1,
                                                                                   collection=col_name))
            else:
                ds_in_azure_blob = DatasetResource(properties=eval(source_dataset)(linked_service_name=ds_ls1,
                                                                                   table_name=col_name))

            ds_in = adf_client.datasets.create_or_update(resource_group, data_factory_name, ds_in_name,
                                                         ds_in_azure_blob)
            self.print_item(ds_in)

        except Exception as e:
            print("Unexpected error because of AdfMain source_dataset function :", e)

    def sink_dataset(self, adf_client, account_type, database_name, col_name, resource_group, data_factory_name,
                     sink_dataset):

        try:
            ls_name2 = database_name + "_sinkLinkedService"
            print(ls_name2)
            ds_ls2 = LinkedServiceReference(reference_name=ls_name2)
            print(ds_ls2)

            if account_type == "mongodb":
                ds_out_azure_blob = DatasetResource(properties=eval(sink_dataset)(linked_service_name=ds_ls2,
                                                                                  collection=col_name))
            else:
                ds_out_azure_blob = DatasetResource(properties=eval(sink_dataset)(linked_service_name=ds_ls2,
                                                                                  collection_name=col_name))

            print(ds_out_azure_blob)
            ds_out_name = col_name + "_" + database_name + "_sink_dataset"
            print(ds_out_name)
            ds_out = adf_client.datasets.create_or_update(resource_group, data_factory_name, ds_out_name,
                                                          ds_out_azure_blob)
            self.print_item(ds_out)

        except Exception as e:
            print("Unexpected error because of Create adf resource class sink_dataset function :", e)

    def create_copy_activity(self, database_name, col_name, copy_activity_name):

        try:
            act_name = copy_activity_name + "_" + col_name
            blob_source = BlobSource()
            blob_sink = BlobSink()
            ds_in_name = col_name + "_" + database_name + "_source_dataset"
            ds_out_name = col_name + "_" + database_name + "_sink_dataset"
            ds_in_ref = DatasetReference(reference_name=ds_in_name)
            ds_out_ref = DatasetReference(reference_name=ds_out_name)
            copy_activity = CopyActivity(name=act_name, inputs=[ds_in_ref], outputs=[ds_out_ref], source=blob_source,
                                         sink=blob_sink)
            return copy_activity

        except Exception as e:
            print("Unexpected error because of AdfMain create_copy_activity function :", e)

    def create_pipeline(self, copy_activity, adf_client, col_name, resource_group, data_factory_name, pipeline_name):

        try:
            p_name = pipeline_name + "_" + col_name
            p_obj = PipelineResource(activities=[copy_activity], parameters={})
            p = adf_client.pipelines.create_or_update(resource_group, data_factory_name, p_name, p_obj)
            self.print_item(p)

        except Exception as e:
            print("Unexpected error because of AdfMain create_pipeline function :", e)

    def run_all(self, uri, adf_client, account_type, database_name, col_name, cosmos_group, data_factory_name,
                source_linked_service, sink_linked_service, source_dataset, sink_dataset):

        try:
            copy_activity_name = SourceSink['copy_activity_name']
            pipeline_name = SourceSink['pipeline_name']
            resource_group = Config['resource_group']
            sub_id = Config['SUB_ID']

            self.source_linked_service(uri, adf_client, database_name, resource_group, data_factory_name,
                                       source_linked_service)
            self.sink_linked_service(adf_client, account_type, database_name, resource_group, cosmos_group,
                                     data_factory_name, sink_linked_service, sub_id)
            self.source_dataset(adf_client, account_type, database_name, col_name, resource_group, data_factory_name,
                                source_dataset)
            self.sink_dataset(adf_client, account_type, database_name, col_name, resource_group, data_factory_name,
                              sink_dataset)
            copy_activity_status = self.create_copy_activity(database_name, col_name, copy_activity_name)
            self.create_pipeline(copy_activity_status, adf_client, col_name, resource_group, data_factory_name,
                                 pipeline_name)
            return True

        except Exception as e:
            print("Unexpected error because of run_all function :", e)
        return False

    def run_monitor_pipeline(self, col_name, rg_name, df_name, azure_client_id, azure_client_secret, azure_tenant_id,
                             sub_id, pipeline_name):

        try:

            credentials = ClientSecretCredential(client_id=azure_client_id, client_secret=azure_client_secret,
                                                 tenant_id=azure_tenant_id)
            adf_client = DataFactoryManagementClient(credentials, sub_id)
            run_response = adf_client.pipelines.create_run(rg_name, df_name, pipeline_name + "_" + col_name)
            time.sleep(30)
            pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
            print("\n\tPipeline execution status: {}".format(pipeline_run.status))

            filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1),
                                                last_updated_before=datetime.now() + timedelta(1))
            query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id,
                                                                            filter_params)
            self.print_activity_run_details(query_response.value[0])
            return True

        except Exception as e:
            print("Unexpected error because of AdfMain run_monitor_pipeline function :", e)
        return False
