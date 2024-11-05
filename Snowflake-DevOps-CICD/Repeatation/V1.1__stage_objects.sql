CREATE OR REPLACE SCHEMA SF_RetailSales1.stage_layer1;

USE SCHEMA SF_RetailSales1.stage_layer1;

CREATE TABLE DimProduct (
    ProductID INT,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Brand VARCHAR(50),
    Price DECIMAL(10,2)
);
CREATE TABLE DimCustomer (
    CustomerID INT,
    CustomerName VARCHAR(100),
    Gender VARCHAR(10),
    Age INT,
    City VARCHAR(100)
);

CREATE TABLE DimStore (
    StoreID INT,
    StoreName VARCHAR(100),
    City VARCHAR(100),
    Region VARCHAR(100)
);

CREATE TABLE DimDate (
    DateID INT,
    Date DATE,
    Year INT,
    Month INT,
    DayOfWeek VARCHAR(10)
);

CREATE TABLE FactSales (
    SaleID INT,
    ProductID INT,
    CustomerID INT,
    StoreID INT,
    DateID INT,
    SalesAmount DECIMAL(10,2),
    Quantity INT
);

--  ========================================== Snowflake STREAMS =================================

CREATE OR REPLACE STREAM SF_RETAILSALES1.RAW_LAYER1.STREAM_FACTSALES ON TABLE SF_RETAILSALES1.RAW_LAYER1.FACTSALES;
CREATE OR REPLACE STREAM SF_RETAILSALES1.RAW_LAYER1.STREAM_DIMCUSTOMER ON TABLE SF_RETAILSALES1.RAW_LAYER1.DIMCUSTOMER;
CREATE OR REPLACE STREAM SF_RETAILSALES1.RAW_LAYER1.STREAM_DIMDATE ON TABLE SF_RETAILSALES1.RAW_LAYER1.DIMDATE;
CREATE OR REPLACE STREAM SF_RETAILSALES1.RAW_LAYER1.STREAM_DIMPRODUCT ON TABLE SF_RETAILSALES1.RAW_LAYER1.DIMPRODUCT;
CREATE OR REPLACE STREAM SF_RETAILSALES1.RAW_LAYER1.STREAM_DIMSTORE ON TABLE SF_RETAILSALES1.RAW_LAYER1.DIMSTORE;

--  ========================================== SP for Dim Product=================================

CREATE OR REPLACE PROCEDURE SP_copy_to_DimProduct()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark

def run(session):
    try:
        # Define the source and target tables
        DB = 'SF_RETAILSALES11'
        source_schema = 'SF_RETAILSALES1.RAW_LAYER1'
        source_table = 'STREAM_DIMPRODUCT'
        target_schema = 'SF_RETAILSALES1.STAGE_LAYER1'
        target_table = 'DIMPRODUCT'

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE INTO {target_schema}.{target_table} AS target
            USING (
                SELECT PRODUCTID, PRODUCTNAME, CATEGORY, BRAND, PRICE, METADATA$ACTION, METADATA$ISUPDATE
                FROM {DB}.{source_schema}.{source_table}
            ) AS source
            ON target.PRODUCTID = source.PRODUCTID
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE') THEN
                UPDATE SET
                    target.PRODUCTNAME = source.PRODUCTNAME,
                    target.CATEGORY = source.CATEGORY,
                    target.BRAND = source.BRAND,
                    target.PRICE = source.PRICE
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE') THEN
                DELETE 
            
            WHEN NOT MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE') THEN
                INSERT (PRODUCTID, PRODUCTNAME, CATEGORY, BRAND, PRICE)
                VALUES (source.PRODUCTID, source.PRODUCTNAME, source.CATEGORY, source.BRAND, source.PRICE);
        """
        
        # Execute the MERGE statement
        session.sql(merge_statement).collect()

        return "Data merged successfully from schema1.table1 to schema2.table2 using record_type for INSERT, UPDATE, and DELETE."

    except Exception as e:
        # Handle any errors and return the error message
        return f"Error: {str(e)}"
$$;


--  ========================================== SP for Dim Customer =================================
CREATE OR REPLACE PROCEDURE SP_copy_to_DimCustomer()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark

def run(session):
    try:
        # Define the source and target tables
        DB = 'SF_RETAILSALES11'
        source_schema = 'SF_RETAILSALES1.RAW_LAYER1'
        source_table = 'STREAM_DIMCUSTOMER'
        target_schema = 'SF_RETAILSALES1.STAGE_LAYER1'
        target_table = 'DIMCUSTOMER'

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE INTO {target_schema}.{target_table} AS target
            USING (
                SELECT CustomerID, CustomerName, Gender, Age, City, METADATA$ACTION, METADATA$ISUPDATE
                FROM {DB}.{source_schema}.{source_table}
            ) AS source
            ON target.CustomerID = source.CustomerID
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE') THEN
                UPDATE SET
                    target.CustomerName = source.CustomerName,
                    target.Gender = source.Gender,
                    target.Age = source.Age,
                    target.City = source.City
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE') THEN
                DELETE 
            
            WHEN NOT MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE') THEN
                INSERT (CustomerID, CustomerName, Gender, Age, City)
                VALUES (source.CustomerID, source.CustomerName, source.Gender, source.Age, source.City);
        """
        
        # Execute the MERGE statement
        session.sql(merge_statement).collect()

        return "Data merged successfully from schema1.table1 to schema2.table2 using record_type for INSERT, UPDATE, and DELETE."

    except Exception as e:
        # Handle any errors and return the error message
        return f"Error: {str(e)}"
$$;

--  ========================================== SP for Dim Date =================================
CREATE OR REPLACE PROCEDURE SP_copy_to_DimDate()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark

def run(session):
    try:
        # Define the source and target tables
        DB = 'SF_RETAILSALES11'
        source_schema = 'SF_RETAILSALES1.RAW_LAYER1'
        source_table = 'STREAM_DIMDATE'
        target_schema = 'SF_RETAILSALES1.STAGE_LAYER1'
        target_table = 'DIMDATE'

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE INTO {target_schema}.{target_table} AS target
            USING (
                SELECT DateID, Date, Year, Month, DayOfWeek, METADATA$ACTION, METADATA$ISUPDATE
                FROM {DB}.{source_schema}.{source_table}
            ) AS source
            ON target.DateID = source.DateID
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE') THEN
                UPDATE SET
                    target.Date = source.Date,
                    target.Year = source.Year,
                    target.Month = source.Month,
                    target.DayOfWeek = source.DayOfWeek
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE') THEN
                DELETE 
            
            WHEN NOT MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE') THEN
                INSERT (DateID, Date, Year, Month, DayOfWeek)
                VALUES (source.DateID, source.Date, source.Year, source.Month, source.DayOfWeek);
        """
        
        # Execute the MERGE statement
        session.sql(merge_statement).collect()

        return "Data merged successfully from schema1.table1 to schema2.table2 using record_type for INSERT, UPDATE, and DELETE."

    except Exception as e:
        # Handle any errors and return the error message
        return f"Error: {str(e)}"
$$;


--  ========================================== SP for Dim Store =================================
CREATE OR REPLACE PROCEDURE SP_copy_to_DimStore()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark

def run(session):
    try:
        # Define the source and target tables
        DB = 'SF_RETAILSALES11'
        source_schema = 'SF_RETAILSALES1.RAW_LAYER1'
        source_table = 'STREAM_DIMSTORE'
        target_schema = 'SF_RETAILSALES1.STAGE_LAYER1'
        target_table = 'DIMSTORE'

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE INTO {target_schema}.{target_table} AS target
            USING (
                SELECT StoreID, StoreName, City, Region, METADATA$ACTION, METADATA$ISUPDATE
                FROM {DB}.{source_schema}.{source_table}
            ) AS source
            ON target.StoreID = source.StoreID
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE') THEN
                UPDATE SET
                    target.StoreName = source.StoreName,
                    target.City = source.City,
                    target.Region = source.Region
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE') THEN
                DELETE 
            
            WHEN NOT MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE') THEN
                INSERT (StoreID, StoreName, City, Region)
                VALUES (source.StoreID, source.StoreName, source.City, source.Region);
        """
        
        # Execute the MERGE statement
        session.sql(merge_statement).collect()

        return "Data merged successfully from schema1.table1 to schema2.table2 using record_type for INSERT, UPDATE, and DELETE."

    except Exception as e:
        # Handle any errors and return the error message
        return f"Error: {str(e)}"
$$;


--  ========================================== SP for FACTSALES =================================
CREATE OR REPLACE PROCEDURE SP_copy_to_FactSales()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
import snowflake.snowpark

def run(session):
    try:
        # Define the source and target tables
        DB = 'SF_RETAILSALES11'
        source_schema = 'SF_RETAILSALES1.RAW_LAYER1'
        source_table = 'STREAM_FACTSALES'
        target_schema = 'SF_RETAILSALES1.STAGE_LAYER1'
        target_table = 'FACTSALES'

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE INTO {target_schema}.{target_table} AS target
            USING (
                SELECT SaleID, ProductID, CustomerID, StoreID, DateID, SalesAmount, Quantity, METADATA$ACTION, METADATA$ISUPDATE
                FROM {DB}.{source_schema}.{source_table}
            ) AS source
            ON target.SaleID = source.SaleID
            AND target.ProductID = source.ProductID
            AND target.CustomerID = source.CustomerID
            AND target.StoreID = source.StoreID
            AND target.DateID = source.DateID
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE') THEN
                UPDATE SET
                    target.SalesAmount = source.SalesAmount,
                    target.Quantity = source.Quantity
            
            WHEN MATCHED AND (source.METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = 'FALSE') THEN
                DELETE 
            
            WHEN NOT MATCHED AND (source.METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE') THEN
                INSERT (SaleID, ProductID, CustomerID, StoreID, DateID, SalesAmount, Quantity)
                VALUES (source.SaleID, source.ProductID, source.CustomerID, source.StoreID, source.DateID, source.SalesAmount, source.Quantity);
        """
        
        # Execute the MERGE statement
        session.sql(merge_statement).collect()

        return "Data merged successfully from schema1.table1 to schema2.table2 using record_type for INSERT, UPDATE, and DELETE."

    except Exception as e:
        # Handle any errors and return the error message
        return f"Error: {str(e)}"
$$;
