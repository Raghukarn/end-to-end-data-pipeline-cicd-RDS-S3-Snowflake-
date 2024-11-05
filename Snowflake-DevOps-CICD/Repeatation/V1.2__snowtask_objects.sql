CREATE OR REPLACE TASK call_SP_DimCustomer
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CROn 1 * * * * UTC'  -- Runs at the top of every hour
  COMMENT = 'Task to execute SP_copy_to_DimCustomer'
  AS
    CALL SF_RETAILSALES1.STAGE_LAYER1.SP_COPY_TO_DIMCUSTOMER();


CREATE OR REPLACE TASK call_SP_DimDate
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CROn 1 * * * * UTC'  -- Runs at the top of every hour
  COMMENT = 'Task to execute SP_copy_to_DimCustomer'
  AS
    CALL SF_RETAILSALES1.STAGE_LAYER1.SP_COPY_TO_DIMDATE();

    
 CREATE OR REPLACE TASK call_SP_DimProduct
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CROn 1 * * * * UTC'  -- Runs at the top of every hour
  COMMENT = 'Task to execute SP_copy_to_DimCustomer'
  AS
    CALL SF_RETAILSALES1.STAGE_LAYER1.SP_COPY_TO_DIMPRODUCT();

CREATE OR REPLACE TASK call_SP_DimStore
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CROn 1 * * * * UTC'  -- Runs at the top of every hour
  COMMENT = 'Task to execute SP_copy_to_DimCustomer'
  AS
    CALL SF_RETAILSALES1.STAGE_LAYER1.SP_COPY_TO_DIMSTORE();    

CREATE OR REPLACE TASK call_SP_FactSales
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CROn 1 * * * * UTC'  -- Runs at the top of every hour
  COMMENT = 'Task to execute SP_copy_to_DimCustomer'
  AS
    CALL SF_RETAILSALES1.STAGE_LAYER1.SP_COPY_TO_FACTSALES(); 


SHOW TASKS;

-- RESUME tasks
ALTER TASK call_SP_DimCustomer RESUME;
ALTER TASK call_SP_DimDate RESUME;
ALTER TASK call_SP_DimProduct RESUME;
ALTER TASK call_SP_DimStore RESUME;
ALTER TASK call_SP_FactSales RESUME;

-- SUSPEND tasks
ALTER TASK call_SP_DimCustomer SUSPEND;
ALTER TASK call_SP_DimDate SUSPEND;
ALTER TASK call_SP_DimProduct SUSPEND;
ALTER TASK call_SP_DimStore SUSPEND;
ALTER TASK call_SP_FactSales SUSPEND;
