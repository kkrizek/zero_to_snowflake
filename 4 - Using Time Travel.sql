use database <your_db>;
use schema public;
use warehouse <your_initials>_compute_wh;

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
