----- Set Environment -----
set env = 'dev';
set db = 'flowbridge_' || $env || '_db';

use database identifier($db);
use role sysadmin;

use schema silver_sch;
use warehouse flowbridge_pipeline_wh;

------ Dead Letter Table ------
------ Transient - stores bad/rejected records
----- created first so task can reference it

create or replace transient table silver_sch.dead_letter(
    raw_data variant,
    error_reason string,
    file_name string,
    file_row_number number,
    rejected_at timestamp_ntz default current_timestamp()
) comment = 'Dead letter table rejected/bad records from bronze';


---- stg_orders table ----
---- permanent - flattened, typed, cleaned json 
--- all columns from json schema included 
---- time travel enabled (default 1 day)

create or replace table silver_sch.stg_orders(
    order_id string,
    order_date timestamp_ntz,
    order_status string,

    customer_id string,
    customer_name string,
    customer_region string,
    customer_segment string,

    supplier_id string,
    supplier_name string,
    supplier_country string,
    lead_time_days number,
    performance_score float,

    shipment_id string,
    carrier string,
    ship_date timestamp_ntz,
    estimated_delivery date,
    delay_days number,

    product_id string,
    product_name string,
    category string,
    quantity number,
    unit_price float,

    total_amount float,
    payment_status string,

    warehouse_id string,
    warehouse_location string,
    inventory_level number,

    file_name string,
    file_row_number number,
    ingested_at timestamp_ntz,
    transformed_at timestamp_ntz default current_timestamp()
);


------ Create stream on raw_orders 
------ captures new records from bronze layer

create or replace stream silver_sch.raw_orders_stream
    on table flowbridge_dev_db.bronze_sch.raw_orders
    append_only = true
    show_initial_rows = true
    comment = "Stream on raw orders captures new data for silver processing";


------ Stored Procedures sp_bronze_to_silver
---- route bad records to dead_letter
----- merge clean records into stg_orders


CREATE OR REPLACE PROCEDURE silver_sch.sp_bronze_to_silver()
    RETURNS STRING
    LANGUAGE SQL
    COMMENT = 'Routes bad records to dead letter and merges clean records to stg_orders'
AS
$$
BEGIN

    CREATE OR REPLACE TEMPORARY TABLE silver_sch.stream_buffer AS
    SELECT *
    FROM silver_sch.raw_orders_stream
    WHERE METADATA$ACTION = 'INSERT';

    RETURN 'Stream buffer created successfully';

END;
$$;


----- Route bad data to dead letter ------
insert into silver_sch.dead_letter(
    raw_data,
    error_reason,
    file_name,
    file_row_number
)
select 
    s.raw_data,
    case
        when s.raw_data:order_id::string is null
            then 'Missing order_id'
        when try_to_timestamp_ntz(s.raw_data:order_date::STRING) is null
            and s.raw_data:order_date::string not like '__-__-____'
            then 'Invalid or missing order_date'
        when upper(trim(s.raw_data:order_status::string)) not in ('PENDING', 'PROCESSING', 'SHIPPED', 'IN TRANSIT', 'DELIVERED', 'CANCELLED')
            then 'Invalid order_status:' || coalesce(s.raw_data:order_status::string, 'null')

        when s.raw_data:customer.customer_id::string is null
            then 'Missing customer_id'

        when s.raw_data:supplier.supplier_id::string is null
            then 'Missing supplier_id'
        when s.raw_data:supplier.performance_score::float > 100
            then 'Invalid performance_score > 100: ' || s.raw_data:supplier.performance_score::string
        when s.raw_data:supplier.performance_score::float < 0
            then 'Invalid performance_score < 0: ' || s.raw_data:supplier.performance_score::string
        when s.raw_data:supplier.lead_time_days::number < 0
            then 'Invalid lead_time_days < 0: ' || s.raw_data:supplier.lead_time_days::string

        when s.raw_data:items[0].product_id::string is null 
            then 'Missing product_id'
        when s.raw_data:items[0].quantity::number is null
            or s.raw_data:items[0].quantity::number <= 0
            then 'Invalid quantity: ' || coalesce(s.raw_data:items[0].quantity::string, 'null')
        when s.raw_data:items[0].unit_price::float is null
            or s.raw_data:items[0].unit_price::float < 0
            then 'Invalid unit_price: ' || coalesce(s.raw_data:items[0].unit_price::string, 'null')

        when s.raw_data:financials.total_amount::float is null
            or s.raw_data:financials.total_amount::float < 0
            then 'Invalid total_amount: ' || coalesce(s.raw_data:financials.total_amount::string, 'null')

        when s.raw_data:warehouse.inventory_level::number < 0
            then 'Invalid inventory_level < 0: ' || s.raw_data:warehouse.inventory_level::string

        else 'Unknown'
    end as error_reason,
    s.file_name,
    s.file_row_number
from silver_sch.stream_buffer as s
where 
    (
        s.raw_data:order_id::string is null
        or(
            try_to_timestamp_ntz(s.raw_data:order_date::string) is null
            and s.raw_data:order_date::string not like'__-__-____'
        )
        or upper(trim(s.raw_data:order_status::string)) not in(
            'PENDING', 'PROCESSING', 'SHIPPED',
            'IN TRANSIT', 'DELIVERED', 'CANCELLED'
        )

        or s.raw_data:customer.customer_id::string is null

        or s.raw_data:supplier.supplier_id::string is null
        or s.raw_data:supplier.performance_score::float > 100
        or s.raw_data:supplier.performance_score::float < 0
        or s.raw_data:supplier.lead_time_days::number < 0

        or s.raw_data:items[0].product_id::string is null
        or s.raw_data:items[0].quantity::number is null
        or s.raw_data:items[0].quantity::number <= 0
        or s.raw_data:items[0].unit_price::float is null
        or s.raw_data:items[0].unit_price::float < 0

        or s.raw_data:financials.total_amount::float is null
        or s.raw_data:financials.total_amount::float < 0

        or s.raw_data:warehouse.inventory_level::number < 0
    );


----- Merge clean records into stg_orders
----- when matched - update changed fields only
----- when not matched - insert new order

