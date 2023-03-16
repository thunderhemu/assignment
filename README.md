# Farmtrace takassignmente
  
## Table of Contents
* [General info](#general-info)
* [Flow Chart](flow-chart)
* [Execution options](#execution-options)
* [Set up local env for tests](#set-up-local-env-for-tests)
* [Project structure](#project-structure)


## General info
   This application read the data from sql writes to datalake and posgres
### config yaml file 
    
   it take config as yaml files
        
     input:  #input section
           query_file_path: <path of input query>
           sql:
             host_name: <host name>
             port: <port>
             users_name: <user-name>
             password: <password>
             db_name: <database name>
             table_name: <table name>
         
         output:
           datalake:
             output_file_path: <file path>
             output_file_format: <output file format>  #csv/parquet/orc
           sql:
             host_name: <host name>
             port: <port>
             users_name: <user-name>
             password: <password>
             db_name: <database name>
             table_name: <table name>

## Flow Chart
   ![Alt text](/images/flow.jpeg "Flow Chart")
   
## Execution options
      python etl_job.py --conf <conf.yaml>
 
## Set up local env for tests  

   1. Install Docker Desktop by using the following link
      https://docs.docker.com/desktop/install/mac-install/
   2. install python 3.0 and above
   3. pip install -r <path>/requirements.txt

## Project structure 

       ├── job          # Source files
       |   
       └──  tests   # Unit tests
       |        └──   conf  # test resources required for test cases
       ├── .gitignore    # list of untracked files
       ├── conf.template.yaml input config template 
       ├── query.sql     # ANSI SQL query to get lineal – direct descendants
       ├── README.md   # list of untracked files
       └── requirements.txt libs list
