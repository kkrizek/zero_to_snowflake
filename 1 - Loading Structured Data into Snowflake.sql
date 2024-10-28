use schema <your_db>.public;

/* 
Create your compute warehouse; set the size to extra small.  
Make sure to replace <your initials> with your initials
*/
create warehouse if not exists <your_initials>_compute_wh with
warehouse_size = 'XSMALL'
auto_suspend = 120
auto_resume = TRUE;

use warehouse <your_initials>_compute_wh;

/*
Create a table called COMPANY_METADATA to use for loading the comma-delimited data. Instead 
of using the UI, we use the worksheet to run the DDL that creates the table. 
 */
CREATE OR REPLACE TABLE company_metadata
(cybersyn_company_id string,
company_name string,
permid_security_id string,
primary_ticker string,
security_name string,
asset_class string,
primary_exchange_code string,
primary_exchange_name string,
security_status string,
global_tickers variant,
exchange_code variant,
permid_quote_id variant);

/*
We are working with structured, comma-delimited data that has already been staged in a public, 
external S3 bucket. Before we can use this data, we first need to create a stage that specifies 
the location of our external bucket.
 */
CREATE STAGE <your_db>.public.cybersyn_company_metadata
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn-consumer-company-metadata-csv/';

LIST @cybersyn_company_metadata;

/*
 Before we can load the data into Snowflake, we have to create a file format that 
matches the data structure.
 */
 CREATE OR REPLACE FILE FORMAT csv
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'  -- Automatically determines the compression of files
    FIELD_DELIMITER = ','  -- Specifies comma as the field delimiter
    RECORD_DELIMITER = '\n'  -- Specifies newline as the record delimiter
    SKIP_HEADER = 1  -- Skip the first line
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  -- Fields are optionally enclosed by double quotes (ASCII code 34)
    TRIM_SPACE = FALSE  -- Spaces are not trimmed from fields
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Does not raise an error if the number of fields in the data file varies
    ESCAPE = 'NONE'  -- No escape character for special character escaping
    ESCAPE_UNENCLOSED_FIELD = '\134'  -- Backslash is the escape character for unenclosed fields
    DATE_FORMAT = 'AUTO'  -- Automatically detects the date format
    TIMESTAMP_FORMAT = 'AUTO'  -- Automatically detects the timestamp format
    NULL_IF = ('')  -- Treats empty strings as NULL values
    COMMENT = 'File format for ingesting data for zero to snowflake';

SHOW FILE FORMATS IN DATABASE <your_db>;



--Set the warehouse size to small
alter warehouse <your initials>_compute_wh set warehouse_size = 'SMALL';

--Use the COPY command to load the structured CSV data into your company metadata table
COPY INTO company_metadata FROM @cybersyn_company_metadata file_format=csv 
PATTERN = '.*csv.*' ON_ERROR = 'CONTINUE';

/*
Now let's reload the COMPANY_METADATA table with a larger warehouse to see the impact the
additional compute resources have on the loading time.
 */
 TRUNCATE TABLE company_metadata;

-- Verify that the table is empty by running the following command:
SELECT * FROM company_metadata LIMIT 10;

--Change the warehouse size to LARGE
ALTER WAREHOUSE <your_initials>_compute_wh SET warehouse_size='large';

-- Verify the change using the following SHOW WAREHOUSES:
SHOW WAREHOUSES;

--Use the same copy command
COPY INTO company_metadata FROM @cybersyn_company_metadata file_format=csv 
PATTERN = '.*csv.*' ON_ERROR = 'CONTINUE';

--Navigate to the Query History tab and compare the times of the two copy commands

/*
Let's assume our internal analytics team wants to eliminate resource contention between 
their data loading/ETL workloads and the analytical end users using BI tools to query 
Snowflake.

Since our company already has a warehouse for data loading, let's create a new warehouse 
for the end users running analytics.
 */
create warehouse if not exists <your_initials>_analytics_wh with
warehouse_size = 'LARGE'
auto_suspend = 120
auto_resume = TRUE
max_cluster_count = 5
min_cluster_count = 1
;