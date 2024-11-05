
CREATE OR REPLACE PROCEDURE SP_convert_columns_to_text()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark as snowpark

def run(session):
    DB = 'SF_RetailSales1'
    schema = 'RAW_LAYER1'
    
    # Step 1: Get all tables in the schema
    query_tables = f"""
        SELECT table_name
        FROM {DB}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type = 'BASE TABLE'
    """
    tables = session.sql(query_tables).collect()
    
    # Track conversion summary
    conversion_summary = []
    
    # Step 2: Loop through each table and convert columns to text
    for table_row in tables:
        table_name = table_row['TABLE_NAME']
        
        # Step 3: Rename all columns to uppercase for case insensitivity
        query_all_columns = f"""
            SELECT column_name
            FROM {DB}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table_name}'
        """
        all_columns = session.sql(query_all_columns).collect()
        
        rename_statements = []
        for row in all_columns:
            column_name = row['COLUMN_NAME']
            if column_name != column_name.upper():  # Check if the column name needs to be uppercased
                rename_statements.append(f'ALTER TABLE {DB}.{schema}.{table_name} RENAME COLUMN "{column_name}" TO {column_name.upper()}')
        
        for statement in rename_statements:
            session.sql(statement).collect()
        
        # Step 4: Fetch all columns with data types not equal to TEXT
        query_non_text_columns = f"""
            SELECT column_name, data_type
            FROM {DB}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table_name}'
              AND data_type NOT LIKE '%TEXT%'
        """
        non_text_columns = session.sql(query_non_text_columns).collect()
        
        # Step 5: Convert non-text columns to TEXT using temporary columns
        temp_columns = []
        for row in non_text_columns:
            column_name = row['COLUMN_NAME'].upper()  # Column names are now uppercase
            temp_column_name = f"{column_name}_TEMP"
            # Add temporary column as VARCHAR
            session.sql(f"ALTER TABLE {DB}.{schema}.{table_name} ADD COLUMN {temp_column_name} TEXT").collect()
            # Copy data from original column to temp column as VARCHAR
            session.sql(f"UPDATE {DB}.{schema}.{table_name} SET {temp_column_name} = {column_name}::TEXT").collect()
            # Drop original column
            session.sql(f"ALTER TABLE {DB}.{schema}.{table_name} DROP COLUMN {column_name}").collect()
            # Rename temp column to original column name
            session.sql(f"ALTER TABLE {DB}.{schema}.{table_name} RENAME COLUMN {temp_column_name} TO {column_name}").collect()
            temp_columns.append(temp_column_name)
        
        conversion_summary.append(f"Table {DB}.{schema}.{table_name}: Renamed {len(rename_statements)} columns to uppercase and converted {len(temp_columns)} columns to TEXT.")
    
    return "Conversion Summary:\n" + "\n".join(conversion_summary)
$$;


CALL SP_convert_columns_to_text();

-- SELECT * FROM SF_RETAILSALES.INFORMATION_SCHEMA.columns WHERE table_schema != 'INFORMATION_SCHEMA';
CREATE OR REPLACE FILE FORMAT my_csv_format1
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

-- ============================================================ START - CREATE SNOW PIPE ===============================================  
  CREATE OR REPLACE PIPE snowpipe_DimCustomer
  AUTO_INGEST = TRUE
  AS
  COPY INTO SF_RETAILSALES1.RAW_LAYER1.DIMCUSTOMER (CustomerID, CustomerName, Gender, Age, City)
  FROM '@my_external_stage/DimCustomer/'
  FILE_FORMAT = (FORMAT_NAME = my_csv_format1)
  ON_ERROR = 'CONTINUE';

  
-- DROP PIPE IF EXISTS snowpipe_DimDate;
-- DimDate
  CREATE OR REPLACE PIPE snowpipe_DimDate
  AUTO_INGEST = TRUE
  AS
  COPY INTO SF_RETAILSALES1.RAW_LAYER1.DIMDATE (DateID, Date, Year, Month, DayOfWeek)
  FROM '@my_external_stage/DimDate/'
  FILE_FORMAT = (FORMAT_NAME = my_csv_format1)
  ON_ERROR = 'CONTINUE';

  
-- DimProduct
  CREATE OR REPLACE PIPE snowpipe_DimProduct
  AUTO_INGEST = TRUE
  AS
  COPY INTO SF_RETAILSALES1.RAW_LAYER1.DIMPRODUCT (ProductID, ProductName, Category, Brand, Price)
  FROM '@my_external_stage/DimProduct/'
  FILE_FORMAT = (FORMAT_NAME = my_csv_format1)
  ON_ERROR = 'CONTINUE';

  
-- DimStore
  CREATE OR REPLACE PIPE snowpipe_DimStore 
  AUTO_INGEST = TRUE
  AS
  COPY INTO SF_RETAILSALES1.RAW_LAYER1.DIMSTORE (StoreID, StoreName, City, Region)
  FROM '@my_external_stage/DimStore/'
  FILE_FORMAT = (FORMAT_NAME = my_csv_format1)
  ON_ERROR = 'CONTINUE';

  
-- FactSales
  CREATE OR REPLACE PIPE snowpipe_FactSales
  AUTO_INGEST = TRUE
  AS
  COPY INTO SF_RETAILSALES1.RAW_LAYER1.FACTSALES (SaleID, ProductID, CustomerID, StoreID, DateID, SalesAmount, Quantity)
  FROM '@my_external_stage/FactSales/'
  FILE_FORMAT = (FORMAT_NAME = my_csv_format1)
  ON_ERROR = 'CONTINUE';



-- ============================================================ END - CREATE SNOW PIPE ===============================================