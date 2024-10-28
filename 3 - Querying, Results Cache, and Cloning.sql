 --Querying, the results cache, and cloning
use role sysadmin;
use warehouse <your_initials>_analytics_wh;
use database <your_db>;
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
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
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
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
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





