
CREATE OR REPLACE DATABASE SF_RetailSales1;

CREATE OR REPLACE SCHEMA SF_RetailSales1.raw_layer1;

---> create the storage itegration for s3
-- CREATE OR REPLACE STORAGE INTEGRATION s3_int1
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::211125616305:role/rds_s3_snowflake_role'
--   ENABLED = TRUE
--   STORAGE_ALLOWED_LOCATIONS = ('s3://raghu-dms-target/dms/RetailSales/');

--   ---> create the EXTERNAL STAGE using s3_int storage itegration for s3
CREATE OR REPLACE STAGE my_external_stage
  URL = 's3://raghu-dms-target/dms/RetailSales/'
  STORAGE_INTEGRATION = s3_int;

  -- Create a file format that sets the file type as CSV.
CREATE FILE FORMAT my_csv_format
  TYPE = csv
  SKIP_HEADER = 0
  PARSE_HEADER = TRUE;

-- ============================================================ START - CREATE TABLES using INFER SCHEMA ===============================================


USE SCHEMA SF_RetailSales1.raw_layer1;

-- Query the INFER_SCHEMA function.
CREATE OR REPLACE TABLE DimCustomer
                USING TEMPLATE(SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE
    (
    INFER_SCHEMA(
              LOCATION=>'@my_external_stage/DimCustomer/'
              , FILE_FORMAT=>'my_csv_format'
              )
    ))ENABLE_SCHEMA_EVOLUTION = TRUE;


-- DimDate.
CREATE OR REPLACE TABLE DimDate
                USING TEMPLATE(SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE
    (
    INFER_SCHEMA(
              LOCATION=>'@my_external_stage/DimDate/'
              , FILE_FORMAT=>'my_csv_format'
              )
    ))ENABLE_SCHEMA_EVOLUTION = TRUE;


-- DimProduct.
CREATE OR REPLACE TABLE DimProduct
                USING TEMPLATE(SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE
    (
    INFER_SCHEMA(
              LOCATION=>'@my_external_stage/DimProduct/'
              , FILE_FORMAT=>'my_csv_format'
              )
    ))ENABLE_SCHEMA_EVOLUTION = TRUE;

-- DimStore.
CREATE OR REPLACE TABLE DimStore
                USING TEMPLATE(SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE
    (
    INFER_SCHEMA(
              LOCATION=>'@my_external_stage/DimStore/'
              , FILE_FORMAT=>'my_csv_format'
              )
    ))ENABLE_SCHEMA_EVOLUTION = TRUE;

-- FactSales.
CREATE OR REPLACE TABLE FactSales
                USING TEMPLATE(SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE
    (
    INFER_SCHEMA(
              LOCATION=>'@my_external_stage/FactSales/'
              , FILE_FORMAT=>'my_csv_format'
              )
    ))ENABLE_SCHEMA_EVOLUTION = TRUE;


-- ============================================================ END - CREATE TABLES using INFER SCHEMA ===============================================
