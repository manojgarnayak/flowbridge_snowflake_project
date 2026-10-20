------- Environment -------
use database flowbridge_dev_db;

-------- Storage integration  -- Run as accountadmin --------
use role accountadmin;

create storage integration if not exists flowbridge_adls_integration
    type = external_stage
    storage_provider = 'azure'
    enabled = true
    azure_tenant_id = 'de8db145-1130-4490-8232-10380d23aab8'
    storage_allowed_locations = (
        'azure://flowbridgeprojectadls.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://flowbridgeprojectadls.blob.core.windows.net/supply-chain-raw-prod/'
    );

describe integration flowbridge_adls_integration;

grant usage on integration flowbridge_adls_integration to role sysadmin;


------ Notification Integration ------
create or replace notification integration flowbridge_notification_integration
    enabled = true
    type = queue 
    notification_provider = azure_storage_queue
    azure_storage_queue_primary_uri = 'https://flowbridgeprojectadls.queue.core.windows.net/flowbridge-supply-chain-queue'
    azure_tenant_id = 'de8db145-1130-4490-8232-10380d23aab8';

describe integration flowbridge_notification_integration;

grant usage on integration flowbridge_notification_integration to role sysadmin;

---------  Switch to sysadmin -----
use role sysadmin;
use database flowbridge_dev_db;
use schema flowbridge_dev_db.bronze_sch;
use warehouse flowbridge_pipeline_wh;

---------- File Format ---------
create file format if not exists bronze_sch.json_file_format
    type = 'json'
    strip_outer_array = true
    comment = 'JSON File Format for Flowbridge Project';

describe file format bronze_sch.json_file_format;

------- External Stage ------
create stage if not exists bronze_sch.adls_raw_stage
    url = 'azure://flowbridgeprojectadls.blob.core.windows.net/supply-chain-raw-dev/'
    storage_integration = flowbridge_adls_integration
    file_format = bronze_sch.json_file_format
    comment = 'External Stage - ADLS Gen2 DEV container';

list @bronze_sch.adls_raw_stage;


----- Transient Table ------
create or replace transient table bronze_sch.raw_orders(
    raw_data variant,
    ingested_at timestamp_ntz default current_timestamp(),
    file_name string,
    file_row_number number,
    load_id string default uuid_string()
) comment = 'Bronze Layer - raw JSON supply chain orders for Flowbridge';

------ Snowpipe -----
CREATE PIPE IF NOT EXISTS bronze_sch.supply_chain_pipe
    AUTO_INGEST = TRUE
    INTEGRATION = 'FLOWBRIDGE_NOTIFICATION_INTEGRATION'
    COMMENT = 'Snowpipe - auto ingest JSON files from ADLS Gen2'
AS
COPY INTO bronze_sch.raw_orders (
    raw_data,
    file_name,
    file_row_number
)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @bronze_sch.adls_raw_stage
        (FILE_FORMAT => 'BRONZE_SCH.JSON_FILE_FORMAT')
);

------- VERIFY ---------
show pipes;

select system$pipe_status('bronze_sch.supply_chain_pipe');

alter pipe bronze_sch.supply_chain_pipe refresh;

select * from bronze_sch.raw_orders;

select * from table(information_schema.copy_history(
    table_name => 'raw_orders',
    start_time => dateadd(hours, -1, current_timestamp())
));