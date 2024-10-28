use schema cybersyn.public;

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
CREATE STAGE cybersyn.public.cybersyn_sec_filings
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

SHOW FILE FORMATS IN DATABASE cybersyn;



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
ALTER WAREHOUSE compute_wh SET warehouse_size='large';

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

--SEMI-STRUCTURED DATA
/*
Next, let's create two tables, SEC_FILINGS_INDEX and SEC_FILINGS_ATTRIBUTES to use for 
loading JSON data. 

Note that Snowflake has a special data type called VARIANT that allows storing the entire 
JSON object as a single row and querying the object directly.
 */
CREATE TABLE sec_filings_index (v variant);

CREATE TABLE sec_filings_attributes (v variant);

--create a stage that points to the bucket where the semi-structured JSON data is stored on AWS S3
CREATE STAGE cybersyn.public.cybersyn_sec_filings
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn_cpg_sec_filings/';

LIST @cybersyn_sec_filings;

/*
We will now use a warehouse to load the data from an S3 bucket into the tables we created earlier.

Note that you can specify a FILE FORMAT object inline in the command. In the previous section 
where we loaded structured data in CSV format, we had to define a file format to support 
the CSV structure. Because the JSON data here is well-formed, we are able to simply specify 
the JSON type and use all the default settings
 */
COPY INTO sec_filings_index
FROM @cybersyn_sec_filings/cybersyn_sec_report_index.json.gz
    file_format = (type = json strip_outer_array = true);

COPY INTO sec_filings_attributes
FROM @cybersyn_sec_filings/cybersyn_sec_report_attributes.json.gz
    file_format = (type = json strip_outer_array = true);

--Lets review the data that was loaded
SELECT * FROM sec_filings_index LIMIT 10;
SELECT * FROM sec_filings_attributes LIMIT 10;

/*
Next, let's look at how Snowflake allows us to create a view and also query the JSON data directly using SQL.

Snowflake also supports materialized views in which the query results are stored as though the results are a table. 
This allows faster access, but requires storage space. Materialized views can be created and queried if you are using Snowflake Enterprise Edition (or higher).

Run the following command to create a columnar view of the semi-structured JSON SEC filing data, so it is easier 
for analysts to understand and query.

SQL dot notation v:VARIABLE is used in this command to pull out values at lower levels within the JSON object hierarchy. This allows us to treat each 
field as if it were a column in a relational table.
 */
CREATE OR REPLACE VIEW sec_filings_index_view AS
SELECT
    v:CIK::string                   AS cik,
    v:COMPANY_NAME::string          AS company_name,
    v:EIN::int                      AS ein,
    v:ADSH::string                  AS adsh,
    v:TIMESTAMP_ACCEPTED::timestamp AS timestamp_accepted,
    v:FILED_DATE::date              AS filed_date,
    v:FORM_TYPE::string             AS form_type,
    v:FISCAL_PERIOD::string         AS fiscal_period,
    v:FISCAL_YEAR::string           AS fiscal_year
FROM sec_filings_index;

CREATE OR REPLACE VIEW sec_filings_attributes_view AS
SELECT
    v:VARIABLE::string            AS variable,
    v:CIK::string                 AS cik,
    v:ADSH::string                AS adsh,
    v:MEASURE_DESCRIPTION::string AS measure_description,
    v:TAG::string                 AS tag,
    v:TAG_VERSION::string         AS tag_version,
    v:UNIT_OF_MEASURE::string     AS unit_of_measure,
    v:VALUE::string               AS value,
    v:REPORT::int                 AS report,
    v:STATEMENT::string           AS statement,
    v:PERIOD_START_DATE::date     AS period_start_date,
    v:PERIOD_END_DATE::date       AS period_end_date,
    v:COVERED_QTRS::int           AS covered_qtrs,
    TRY_PARSE_JSON(v:METADATA)    AS metadata
FROM sec_filings_attributes;

--Note that the results look just like a regular structured source
SELECT *
FROM sec_filings_index_view
LIMIT 20;

--GETTING DATA FROM THE SNOWFLAKE MARKETPLACE
/*
Navigate to Data Products > Marketplace
Search for Stock Price
Select Finance & Economics from Cybersyn

You can learn more about the contents of the data listing, explore data dictionaries, and see some sample queries.  You
will also see links to documentation and the dataset's cloud region availbility.

Click Get
Within the Options section, name the database FINANCIAL__ECONOMIC_ESSENTIALS and make avilable to SYSADMIN
Click Get
 */

 --Querying, the results cache, and cloning
use role sysadmin;
use warehouse <your_initials>_analytics_wh;
use database cybersyn;
use schema public;

--run the following query to see a sample of the company_metadata
select * from company_metadata;

/*
Next, let's look at the performance of these companies in the stock market
- First, calculate the daily return of a stock and the 5-day moving average from closing prices

If you have defined a particular database in the worksheet and want to use a table from a different database, 
you must fully qualify the reference to the other table by providing its database and schema name.
 */
SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS post_market_close,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date))::DOUBLE AS daily_return,
    AVG(ts.value) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS five_day_moving_avg_price
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Post-Market Close';

/*
Calculate the trading volume change from one day to the next to see if there's an increase or decrease in trading activity
 */
SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS nasdaq_volume,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date))::DOUBLE AS volume_change
FROM cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Nasdaq Volume';

/*
Snowflake has a result cache that holds the results of every query executed in the past 24 hours. These are available 
across warehouses, so query results returned to one user are available to any other user on the system who executes 
the same query, provided the underlying data has not changed. Not only do these repeated queries return extremely 
fast, but they also use no compute credits.

Let's see the result cache in action by running the exact same query again.

In the query details pane on the right, note that the query runs significantly faster because the results have been cached.
 */
 SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS post_market_close,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY primary_ticker ORDER BY ts.date))::DOUBLE AS daily_return,
    AVG(ts.value) OVER (PARTITION BY primary_ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS five_day_moving_avg_price
FROM cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE variable_name = 'Post-Market Close';

/*
Snowflake allows you to create clones, also known as "zero-copy clones" of tables, schemas, and databases in seconds. When 
a clone is created, Snowflake takes a snapshot of data present in the source object and makes it available to the 
cloned object. The cloned object is writable and independent of the clone source. Therefore, changes made to either 
the source object or the clone object are not included in the other.

A massive benefit of zero-copy cloning is that the underlying data is not copied. Only the metadata and pointers to 
the underlying data change. Hence, clones are "zero-copy" and storage requirements are not doubled when the data is 
cloned. Most data warehouses cannot do this, but for Snowflake it is easy!
 */
CREATE TABLE company_metadata_dev CLONE company_metadata;

--JOINING TABLES
/*
We will now join the JSON SEC filing datasets together to investigate the revenue of one CPG company, Kraft Heinz. 
Run the query below to join SEC_FILINGS_INDEX to SEC_FILINGS_ATTRIBUTES to see how Kraft Heinz (KHC) business segments 
have performed over time
 */
WITH data_prep AS (
    SELECT 
        idx.cik,
        idx.company_name,
        idx.adsh,
        idx.form_type,
        att.measure_description,
        CAST(att.value AS DOUBLE) AS value,
        att.period_start_date,
        att.period_end_date,
        att.covered_qtrs,
        TRIM(att.metadata:"ProductOrService"::STRING) AS product
    FROM sec_filings_attributes_view att
    JOIN sec_filings_index_view idx
        ON idx.cik = att.cik AND idx.adsh = att.adsh
    WHERE idx.cik = '0001637459'
        AND idx.form_type IN ('10-K', '10-Q')
        AND LOWER(att.measure_description) = 'net sales'
        AND (att.metadata IS NULL OR OBJECT_KEYS(att.metadata) = ARRAY_CONSTRUCT('ProductOrService'))
        AND att.covered_qtrs IN (1, 4)
        AND value > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY idx.cik, idx.company_name, att.measure_description, att.period_start_date, att.period_end_date, att.covered_qtrs, product
        ORDER BY idx.filed_date DESC
    ) = 1
)

SELECT
    company_name,
    measure_description,
    product,
    period_end_date,
    CASE
        WHEN covered_qtrs = 1 THEN value
        WHEN covered_qtrs = 4 THEN value - SUM(value) OVER (
            PARTITION BY cik, measure_description, product, YEAR(period_end_date)
            ORDER BY period_end_date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        )
    END AS quarterly_value
FROM data_prep
ORDER BY product, period_end_date;

--USING TIME TRAVEL
/*
Snowflake's powerful Time Travel feature enables accessing historical data, as well as the objects storing the data, at 
any point within a period of time. The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can 
be increased up to 90 days
 */
DROP TABLE sec_filings_index;

-- Run a query on the table:
SELECT * FROM sec_filings_index LIMIT 10;

--Time travel allows you to easily restore the table
UNDROP TABLE sec_filings_index;

SELECT * FROM sec_filings_index LIMIT 10;

/*
Let's roll back the COMPANY_METADATA table in the CYBERSYN database to a previous state to fix an unintentional DML 
error that replaces all the company names in the table with the word "oops".
 */
USE ROLE sysadmin;
USE WAREHOUSE <your_initials>_compute_wh;
USE DATABASE cybersyn;
USE SCHEMA public;

--Run the following command to replace all of the company names in the table with the word "oops"
UPDATE company_metadata SET company_name = 'oops';

select * from company_metadata;

/*
Normally we would need to scramble and hope we have a backup lying around. In Snowflake, we can simply run a command 
to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.

Use Time Travel to recreate the table with the correct company names and verify the company names have been restored:
 */
-- Set the session variable for the query_id
SET query_id = (
  SELECT query_id
  FROM TABLE(information_schema.query_history_by_session(result_limit=>5))
  WHERE query_text LIKE 'UPDATE%'
  ORDER BY start_time DESC
  LIMIT 1
);

-- Use the session variable with the identifier syntax (e.g., $query_id)
CREATE OR REPLACE TABLE company_metadata AS
SELECT *
FROM company_metadata
BEFORE (STATEMENT => $query_id);

-- Verify the company names have been restored
SELECT *
FROM company_metadata;



