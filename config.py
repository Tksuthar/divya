from sql_queries import SqlQueries
sql_config = SqlQueries()

Config = {
    "AZURE_CLIENT_ID": "062fa868-320a-45fe-9b91-e262e4b25043",
    "AZURE_CLIENT_SECRET": "n5j7Q~F~hGI-dg~gmEv.L3cHbVK5QXxHyAKIo",
    "AZURE_TENANT_ID": "e4e34038-ea1f-4882-b6e8-ccd776459ca0",
    "SUB_ID": "124f17c5-172f-49a9-b2f1-850e55e52339",
    "resource_group": "migration_to_cosmos",
}

SourceSink = {
    "copy_activity_name": "copy-data-activity",
    "pipeline_name": "copy-pipeline",
    "mongodb": {
        "source_dataset": "MongoDbAtlasCollectionDataset",
        "sink_dataset": "CosmosDbMongoDbApiCollectionDataset",
        "source_linked_service": "MongoDbAtlasLinkedService",
        "sink_linked_service": "CosmosDbMongoDbApiLinkedService",
        "cosmos_group": "mongodb-cosmos-migration" + str(sql_config.get_last_mongodb_id() + 1),
        "data_factory_name": "mongodb-migration-adf" + str(sql_config.get_last_mongodb_id() + 1),
    },
    "azure_sql_database": {
        "source_dataset": "AzureSqlTableDataset",
        "sink_dataset": "CosmosDbSqlApiCollectionDataset",
        "source_linked_service": "AzureSqlDatabaseLinkedService",
        "sink_linked_service": "CosmosDbLinkedService",
        "cosmos_group": "sql-cosmos-migration" + str(sql_config.get_last_mongodb_id() + 1),
        "data_factory_name": "sql-migration-adf" + str(sql_config.get_last_mongodb_id() + 1)
    }
}
