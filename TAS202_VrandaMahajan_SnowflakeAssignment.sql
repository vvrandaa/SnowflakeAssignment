-- Create Roles
create role admin;
create role developer;
create role PII;


-- Create warehouse
CREATE WAREHOUSE assignment_wh
                WAREHOUSE_SIZE = 'MEDIUM'
                WAREHOUSE_TYPE = 'STANDARD'
                AUTO_SUSPEND = 600
                AUTO_RESUME = TRUE
                MIN_CLUSTER_COUNT = 1
                MAX_CLUSTER_COUNT = 2
                SCALING_POLICY = STANDARD
                COMMENT = 'Warehouse for assignment queries';
                
-- Create the database
CREATE OR REPLACE DATABASE assignment_db;


-- Switch to assignment_db
USE DATABASE assignment_db;


-- Create the schema
CREATE OR REPLACE SCHEMA my_schema;
                
-- Give privilages to admin role
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE admin;
GRANT ALL PRIVILEGES ON DATABASE assignment_db TO ROLE admin;
GRANT ALL PRIVILEGES ON SCHEMA assignment_db.my_schema TO ROLE admin;
GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA assignment_db.my_schema TO ROLE admin;


GRANT ROLE admin TO ROLE accountadmin;


-- For developer role
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE developer;
GRANT USAGE ON DATABASE assignment_db TO ROLE developer;
GRANT USAGE ON SCHEMA assignment_db.my_schema TO ROLE developer;
GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA assignment_db.my_schema TO ROLE developer;


GRANT ROLE developer TO ROLE admin;


-- For PII role
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE assignment_db TO ROLE PII;
GRANT USAGE ON SCHEMA assignment_db.my_schema TO ROLE PII;
GRANT SELECT ON ALL TABLES IN SCHEMA assignment_db.my_schema TO ROLE PII;
GRANT ROLE PII TO ROLE accountadmin;




-- Create a table to store the data
CREATE OR REPLACE TABLE employee_data (
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR DEFAULT 'Snowflake worksheet',
    file_name VARCHAR,
    id INT,
    name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    hiredate TIMESTAMP,
    age INT,
    salary FLOAT
);


-- Create variant version of the employee_data table
CREATE OR REPLACE TABLE employee_data_variant (
    id INT,
    name VARCHAR,
    employee_data VARIANT, 
    hiredate TIMESTAMP,
    age INT,
    salary FLOAT
);


-- INTERNAL STAGE
-- Create file format
CREATE OR REPLACE FILE FORMAT emp_file_format
                              type = csv
                              field_delimiter = ', '
                              skip_header = 1
                              Null_if = ('Null','null')
                              empty_field_as_null = true;                            


-- Create an internal stage
CREATE OR REPLACE STAGE internal_stage
                        FILE_FORMAT = emp_file_format;


-- Load data from the sample CSV into the internal stage using snowsql
-- PUT file:///Users/vrandamahajan/Downloads/snowflake_vendor/assignment/empData.csv @internal_stage;


-- Copy data from the internal_stage into the table employee_data
COPY INTO employee_data (file_name, id, name, phone, email, hiredate, age, salary)
FROM (SELECT 
       METADATA$FILENAME AS file_name,  
       $1, $2, $3, $4, $5, $6, $7 
     FROM @internal_stage)
FILE_FORMAT = emp_file_format
ON_ERROR = 'skip_file';




--EXTERNAL STAGE
-- Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int2
                  type = external_stage
                  storage_provider = s3
                  enabled = true
                  storage_aws_role_arn = 'arn:aws:iam::637423504940:role/de'
                  storage_allowed_locations = ('s3://assignment-buck');


-- Create external stage, 
CREATE OR REPLACE STAGE external_stage 
                  storage_integration = s3_int2
                  url = 's3://assignment-buck/empData.csv'
                  file_format = emp_file_format;


-- Integrtaion in aws
desc integration s3_int2;


--  Load data into the variant table from external_stage
COPY INTO employee_data_variant (id, name, employee_data, hiredate, age, salary )
FROM (SELECT $1, $2,
       PARSE_JSON('{
           "phone": "' || $3 || '",
           "email": "' || $4 || '"
       }'), $5, $6, $7
       FROM @external_stage)
FILE_FORMAT = emp_file_format
ON_ERROR = 'skip_file';


select * from employee_data_variant limit 10;


-- Upload parquet file to the stage location
-- Create file format
CREATE FILE FORMAT parquet_format
                   TYPE = parquet;


-- Create stage
CREATE OR REPLACE STAGE parquet_stage
                        FILE_FORMAT = parquet_format; 


-- load file into the stage using snowsql                        
-- PUT file:///Users/vrandamahajan/Downloads/snowflake_vendor/assignment/Flights.parquet @parquet_stage;        




-- Infer the schema of the file
SELECT * 
FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage/Flights.parquet',
      FILE_FORMAT=>'parquet_format'
      )
    );




-- Select query on the staged parquet file
SELECT *
FROM @parquet_stage/Flights.parquet 
limit 10;




-- Create masking policy for email
CREATE MASKING POLICY email_masking AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('PII') THEN VAL
    ELSE '**********'
  END;
  
-- Create masking policy for phone
CREATE MASKING POLICY phone_masking AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('PII') THEN VAL
    ELSE '**********'
  END;


-- alter table to apply masking policy on the email and phone column
alter table employee_data modify column email set masking policy email_masking;
alter table employee_data modify column phone set masking policy phone_masking;


-- switch role to developer, email and phone are masked
use role developer;
use role PII;


SELECT * FROM EMPLOYEE_DATA limit 5;