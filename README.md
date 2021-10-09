# snowflake-pipe

Step 1 :  CREATE FILE FORMAT

create or replace file format <file_format_name>
type = csv field_delimiter = ',' skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('NULL','null')
empty_filed_as_null = true;

Step 2 : CREATE STAGE

create or replace stage <stage_name>
file_format = <file_format_name>;

Step 3 : CREATE TABLE

create table <table_name> (
col1 string,
col2 string,
col3 string,
col4 string
)

Step 4 : CREATE PIPE

create pipe <pipe_name> as copy into <table_name> from <stage_name>

Step 5 : UPLOAD CSV TO SNOWFLAKE

Make a git pull from below location:
https://github.com/abhkpaul/snowflake-pipe

Make below changes for app-config.properties :

Add in your username,password,accountname and other details

Run sf_load_data.py
