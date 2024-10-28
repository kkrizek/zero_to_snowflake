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