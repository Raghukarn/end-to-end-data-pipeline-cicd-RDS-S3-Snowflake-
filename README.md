# End-to-End Data Pipeline with CI/CD for Snowflake
This repository contains the code and documentation for an end-to-end data pipeline project using **MySQL, AWS DMS, Snowflake**, and **CI/CD for database objects**. This pipeline facilitates data ingestion, transformation, and CDC (Change Data Capture) using Snowflake streams, stored procedures, and tasks. CI/CD for database objects is managed with SchemaChange and Azure DevOps.

## Project Overview

- Data ingestion from MySQL to AWS S3 via AWS DMS.
- Loading data into Snowflake’s raw and stage layers with automated change data capture (CDC).
- CI/CD for database objects to streamline production deployment.

## Architecture Diagram


## Tech Stack
- **Database**: MySQL (local), Snowflake
- **Cloud Services**: AWS RDS, S3, AWS DMS, Snowflake
- **CI/CD**: Azure Repos, SchemaChange (for database change management)
 
## Project Workflow
# Part 1: Data Pipeline
1. **Source Database**: MySQL database on a local machine connected to an AWS RDS instance.
2. **Data Migration**:
- &emsp; Use **AWS DMS** to transfer data from MySQL to S3 in CSV format, with separate folders for each table.
3. **Snowflake Integration**:
- &emsp; Create an **external integration** in Snowflake to connect to the S3 bucket and define an **external stage**.
- &emsp; Define a ```raw_layer``` schema in Snowflake and create tables using schema inference from the external stage.
4. **Data Loading to Raw Layer**:
- &emsp; Create a stored procedure to convert all columns in ```raw_layer``` tables to ```TEXT``` data types to prevent data loss during the load.
- &emsp; Create **Snowpipe** jobs to load data from S3 to Snowflake tables in ```raw_layer``` using **SQS notification channels** in S3.
5. **Data Transformation and CDC in Stage Layer**:
- &emsp; Create a ```stage_layer``` schema with the same tables but with accurate data types.
- &emsp; Set up **Snowflake streams** on ```raw_layer``` tables to capture CDC changes.
- &emsp; Develop stored procedures in the ```stage_layer``` schema to handle ```INSERT```, ```UPDATE```, and ```DELETE``` operations on CDC data using **MERGE statements**.
- &emsp; Schedule **Snowflake tasks** to ```call``` these stored procedures and automate data load and transformation.


## Part 2: CI/CD for Database Objects
1. **CI/CD Setup**:
- &emsp; Use **SchemaChange**, a lightweight Python tool, for database change management (DCM) in Snowflake.
- &emsp; The tool follows an imperative-style approach to manage Snowflake objects, enabling automatic deployment of database objects in production.
2. **Pipeline Management**:
- &emsp; Use **Azure Repos** for version control and **Azure Pipelines** to automate deployment.
- &emsp; Configure pipeline jobs to create or update database objects such as databases, schemas, tables, pipes, tasks, streams, and stored procedures.

## Project Structure
The following is an example of the project structure in this repository:

```
.
├── Snowflake-DevOps-CICD
│   ├── Repeatation
│   │   ├── V1.0__snowpipe_objects.sql       # SQL script for Snowpipe objects
│   │   ├── V1.1__stage_objects.sql          # SQL script for stage objects
│   │   └── V1.2__snowtask_objects.sql       # SQL script for Snowflake tasks
│   ├── migrations
│   │   ├── V1.0__create_objects.sql         # Initial database objects creation
│   │   ├── V1.1__update_objects.sql         # Update existing database objects
│   │   └── V1.2__new_DB_objects.sql         # Add new database objects
│   ├── azure-pipelines.yml                  # CI/CD pipeline configuration file
│   └── README.md                            # Project documentation

```
- **src/stored_procedures**: Contains SQL scripts for stored procedures.
- **src/tasks**: SQL scripts for Snowflake tasks.
- **cicd/schemachange**: Configuration files and change scripts for SchemaChange.

Acknowledgments
[SchemaChange Documentation](https://github.com/Snowflake-Labs/schemachange) <br>
[Snowflake Documentation](https://docs.snowflake.com/)

![dashboard](https://github.com/user-attachments/assets/386acc8c-e181-48b2-859c-c66f35fe1b49)

