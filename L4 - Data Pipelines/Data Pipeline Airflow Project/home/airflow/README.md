# Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

![imgs/data-pipeline](imgs/dag-code.png)

## Project Setup
Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.
#Create the redshift cluster 
To go to the Airflow UI:
You can use the Project Workspace here and click on the blue Access Airflow button in the bottom right.
If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).

Click on the Admin tab and select Connections.
**AWS Connection**
on the create connection page, enter the following values:
1.Conn Id: Enter aws_credentials.
2.Conn Type: Enter Amazon Web Services.
3.Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
4.Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

**Redshift Connection**
1.Conn Id: Enter redshift.
2.Conn Type: Enter Postgres.
3.Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. 
4.Schema: This is the Redshift database you want to connect to.
5.Login: Enter awsuser.
6.Password: Enter the password created when launching the Redshift cluster.
7.Port: Enter 5439.

## Datasets 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Project Strucute
* README: Current file, holds instructions and documentation of the project
* dags/sparkify_udacity_dag.py: Define your Directed Acyclic Graph with the default params required. Also, list all the tasks and dependencies required for the data pipeline. In this DAG we will also use PostgressOperator to create the tables required for the data pipeline
* dags/sparkify_dimensions_subdag.py: This is the subdag of 'sparkify_udacity_day' to load all the dimensions in a group
* imgs/dag_representation.png: DAG Project Pipeline
* plugins/helpers/sql_queries.py: List of all Insert SQL statements
* dags/create_tables.sql: Contains SQL Table creations statements
* plugins/operators/stage_redshift.py: StageToRedshiftOperator that copies data from S3 buckets into redshift staging tables
* plugins/operators/load_dimension.py: LoadDimensionOperator that loads data from redshift staging tables into dimensional tables
* plugins/operators/load_fact.py: LoadFactOperator fetched data from staging tables and loads them into dimensional respective tables
* plugins/operators/data_quality.py: DataQualityOperator thats check data quality in redshift tables

![imgs/data-pipeline](imgs/dag-code.png)

### Data Pipeline Steps:
1.Define your parent DAG(sparkify_udacity_dag) with default params(owner,start_date,retries,retry_delay,catchup), provide a schedule interval to run the DAG every hour(schedule_interval='0 * * * *')
2.To Create all the staging, fact and dimensional tables specified in the create_tables.sql we will use the Postgres Operator to execute all the queries
'''
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)
'''
3. After all tables are created, we will define our own custom StageToRedshiftOperator to copy all the events and song json data from S3 to Redshift staging_events and staging_songs tables.
4. Now to load data into songplays fact table, we will degine the LoadFactOperator which inserts data from staging tables into songplays
5. All the dimesional tables share the same code and properties so will use the Subdag operator to group them in one and load the data into them
6. After data is inserted into respective columns, we need to check data quality of all the table using DataQualityCheck operator. Once all the test passed, we can assume our data pipeline is completed
7) Below is the list of all dependencies of tasks
'''
start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

'''




### Building the operators
To complete the project, we will build four different operators that will stage the data, transform the data, and run checks on data quality.

All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply to many kinds of data pipelines with Redshift and with other databases.

*Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
*Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.
*Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.
*Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.
