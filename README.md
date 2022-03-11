# Offline-Migration-Product-To-Cosmos
Product which migrates data from MongoDB and Azure SQL database to Azure Cosmos DB using APIs supported by Azure. 


## Steps to Excute the MigrationMain File are as follows :

  # Step - 1 :
    Install python using "pip install python" command.
    
  # Steps-2 :
    Setup your virtual environment.
      for settuping the environment follow these commands in command prompt :
        1.) python -m venv env    (for creating virtual environment named as env)
        2.) absolute path to this folder\env\Scripts\activate    (for activating venv)
        3.) pip install requirement.txt (i.e. requirement.txt file)
   
  # Now for runnning the file use command :
    python ./filename.extension
    
    

# Details of MigrationMain File :  
  
    #1.) User Credentials :
    
        (i)   Create URI API ( @app.route('/create_uri',methods=['POST']) )
              Request parameters : username, URI, account type (type of source database), driver name of Azure SQL database
              Purpose : To save the connection string (URI) of users in the database 

        (ii)  Fetch tables API (  @app.route('/fetch_tables',methods=['POST']) )
              Request parameters : URI, account type (type of source database), driver name of Azure SQL database
              Purpose : To get the nested dictionary containing databases names, collection name and shard key / partition key.

        (iii) Fetch URIs API ( @app.route('/create_uri',methods=['GET']) )
              Request parameters : username, account type (type of source database)
              Purpose : Returns a list URIs corresponding to user
      
    #2.) Signup API ( @app.route('/signup',methods=['POST']) )
        
         Request parameters : firstname, lastname, username and password
         Response : Message that explains whether account already exists or created
         Purpose : To signup by passing username and password.
        
    #3.) Login API ( @app.route('/login', methods=['POST']) )
         
         Request parameters  : username, password
         Response : Message that shows whether login is successful or not
         Purpose : To verify the user by cross checking the password to the corresponding username in the database
         
    #4.) Migration process     

	       1) Create cosmos resource API based on the database type to be migrated ( @app.route('/create_cosmos_resource', methods=['POST']))
	        	Request parameters : username and account type (type of source database)
            Response : Returns name of cosmos resource created 
	        	Purpose : To create a azure cosmosdb resource

	       2) Create databases API( @app.route('/create_databases', methods=['POST']))
	        	Request parameters : account type (type of source database), database names and table names selected by user
            Response : Returns created databases and collections message
	        	Purpose : To create collections and databases in azure cosmosdb resource

	       3) Create data factory API ( @app.route('/create_data_factory', methods=['POST']))
	        	Request parameters : username, account type (type of source database)
            Response : Returns name of azure data factory resource created
	        	Purpose : To create an azure data factory resource 

	       4) Create pipeline API ( @app.route('/create_pipeline', methods=['POST']))
	        	Request parameters : account type (type of source database), URI(connection string), database names, table names selected by user
            Response : Returns pipeline created successfully message
	        	Purpose : To create linked services, datasets, copy activity and pipeline

	       5) Run pipeline API ( @app.route('/run_pipeline', methods=['POST']))
	        	Request parameters : account type (type of source database), database names, table names selected by user
            Response : Returns data migrated successfully message 
	        	Purpose : To run and monitor azure pipeline 
	        	