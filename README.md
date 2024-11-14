# USS Transformer

This automated tool, developed in python, works on an architecture composed of the combination of MinIO as the storage 
layer and Trino as the computational layer. Following the medallion architecture approach, the raw data is stored as 
parquet files in the bronze layer. The purpose of this tool is to extract the schema from the raw data, perform the USS 
transformation, and store the transformed data in the silver layer.

## Tool Operations

### Setup (Optional)

This operation is optional and allows Trino to be set up from a schema stored in PostgreSQL. 

Given the name of a specific schema, the tool executes a dump command to obtain the SQL statements to completely 
recreate the schema. Afterward, the tool parses them to retrieve all the important information about the schema. Primary 
keys and foreign keys are not considered because not required for the creation of the schema in Trino. 

The tool uses a feature of the DuckDB python API to extract table data as parquet file. These files are uploaded to MinIO via API. The 
paths indicate that these files belong to the bronze layer. An example of path is 
"s3://bronze/\<SchemaName>/\<TableName>/". 

Using SQLGlot, an SQL transpiler for python, the tool can easily convert the data types used in PostgreSQL to the ones 
used in Trino. Finally, the tool creates and runs SQL statements for Trino to recreate the schema tables and associate 
them to the parquet files.

### Schema Extraction

Given the name of a specific schema to be transformed into USS, the tool connects to Trino via API to run several 
queries to retrieve all useful information about this schema. The first of them is to get all the table names of the 
specified schema. The following SQL statement returns the list of table names of the indicated schema.

> SHOW TABLES [ FROM schema_name ]

Queries are then performed for discovering the names and the data types of the columns in each table. The query shown 
below is to retrieve the SQL statement which creates the specified table.

> SHOW CREATE TABLE table_name
 
A general example of the CREATE TABLE statement is shown below, which is the result of the SHOW CREATE TABLE statement.

> CREATE [ OR REPLACE ] TABLE [ IF NOT EXISTS ]  
> table_name (  
> &nbsp; &nbsp; { column_name data_type [ NOT NULL ]  
> &nbsp; &nbsp; &nbsp; &nbsp; [ COMMENT comment ]  
> &nbsp; &nbsp; &nbsp; &nbsp; [ WITH ( property_name = expression [, ...] ) ]  
> &nbsp; &nbsp; | LIKE existing_table_name  
> &nbsp; &nbsp; &nbsp; &nbsp; [ { INCLUDING | EXCLUDING } PROPERTIES ]  
> &nbsp; &nbsp; }  
> &nbsp; &nbsp; [, ...]  
> )  
> [ COMMENT table_comment ]  
> [ WITH ( property_name = expression [, ...] ) ]

The tool parses the SQL statements which create all the tables in the schema to store all the important information 
such as table names, column names and data types, in an organized way to proceed with the USS transformation.

### Annotation Required

Since Trino does not maintain primary keys and foreign keys, the tool requires a special file in which to find all the 
information about them contained in the schema to be transformed. These annotations, written by users, must follow a 
simplified SQL syntax in order to be read by the tool. 

> PRIMARY KEY table_name(column_name [, ...]);

The annotation to declare which columns compose the primary key of the specified table is above. The annotation 
to declare which columns compose the foreign key of the specified table and the corresponding columns in the referenced 
table is below.

> FOREIGN KEY table_name(column_name [, ...]) REFERENCES referenced_table(column_name [, ...]);

### USS Transformation

Once the tool has appropriately stored all the information about the schema, such as table names, column names and data 
types, primary and foreign keys, the USS transformation can begin. 

First of all, the tool needs to create an important list named "links" for each table in the schema. Given a specific 
table, its list initially contains only the name of the tables referenced by its foreign keys. At this point, an initial 
iteration is performed on this list to check whether the stored tables have foreign keys which refers to tables not yet 
present in it. In case new referenced tables are not in the list, they are added into it and another iteration is 
executed, otherwise it means there are no new tables iteratively reachable through FKs. Therefore, if a table has no 
foreign keys, its list will be empty. 

For each table, the technical column named "_Key_\<TableName>" used as the unique PK is stored in the data structure with all the 
information about the schema. If the original primary key is single column, the data type of the new column is the same 
as the original data type. If the table has no PK or has a multi-column one, the data type is defined as binary. These 
new columns are also added as columns of the new table "bridge" along with the column "stage", whose data type is 
defined as variable-length character string. Since the USS structure is defined, the schema tables can be created and 
populated from the raw data using the SQL statement explained below.

### CREATE TABLE AS SELECT (CTAS)

The SQL statement used in Trino to create a new table containing the result of a SELECT query is shown below.

> CREATE [ OR REPLACE ] TABLE [ IF NOT EXISTS ] table_name [ ( column_alias, ... ) ]  
> [ COMMENT table_comment ]  
> [ WITH ( property_name = expression [, ...] ) ]  
> AS query  
> [ WITH [ NO ] DATA ]

The bridge table requires the data from the other tables to be created and populated, hence the other tables have 
priority. How create and populate a specific table using the CTAS is described below.

The "table_name" in the CREATE TABLE clause of CTAS will be replaced by "silver_\<OriginalSchemaName>.
\<OriginalTableName>", to indicate that the table belongs to the silver layer. Inside the round brackets there will only 
be column names, because their data types are directly taken from the data type of the columns listed in the SELECT 
query executed in the AS clause.

The first WITH clause can be used to set properties of the new table. Since the tool works on parquet files in the 
S3-compatible object storage, the property "format" can be set to the value "parquet" and the property 
"external_location" can be set to the path where the parquet file, which contains the table data, is stored in the 
object storage. The path must also indicate that the table belongs to the silver layer. An example of path is
"s3://silver/\<SchemaName>/\<TableName>/".

The AS clause is followed by a SELECT query, which retrieves all data from the original table belonging to the bronze 
layer. To populate the technical column "_Key_\<TableName>", the original PK is reselected if it consists of a single 
column. If the original table has no PK, the Trino function "uuid()" is applied to associate a universally unique 
identifier to each row.

Otherwise, a slightly complex step must be performed if the original PK is multi-column. Each column composing the 
original PK must be transformed into a variable character string using the function of Trino "cast()". Then, the 
concatenation of these columns is executed using the function "array_join()", which must subsequently be transformed as 
binary data. Finally, the function "sha256()" is applied to obtain a hash value to be used as a technical PK.

After executing the CTAS query for each table, the bridge can be created and populated. The columns listed in the CREATE 
TABLE clause are the column "stage" and the columns which refer to the technical PKs of each tables.

In the AS clause, UNION operations are performed between several SELECT queries. Each of these queries retrieves data to 
populate the stage table associate to a specific table. These queries must select the columns in the same order.

Starting from a specific table, left join operations are performed to connects all the tables contained in the list 
"links" related to the specific table. At this point, all technical PKs of joined tables can be retrieved in addition to 
the technical PK of the specific table. Instead, the columns which refer to the technical PKs of non-joined tables are 
set to null. The table name is inserted for each row in the column "stage". 

Once the CTAS statement for the bridge table 
is performed, the USS transformation is completed and the schema in the silver layer is ready to be transformed to 
create data overviews useful for business analysis to be stored in the gold layer.

### Test

The test to verify the correctness of the USS transformation performed by the tool consists of checking that the bridge 
table produced is the same as the one expected by the user. The test is passed if the number of rows in the produced 
bridge table is equal to the number of rows in the user-supplied one, and if every row provided by the user is found in 
the produced bridge table. The test is performed using SQL queries via Trino. Since the generation of the UUID is not 
deterministic, the primary key for tables without it is created through the Trino function "row_number()" which assigns 
the row position based on an order on a specific column chosen by the tool. It is not efficient to execute sorting 
operations when data is distributed, but in this way the user can provide an expected result for PKs in the bridge 
tables, which is not possible using UUIDs.

## Tool Execution Prerequisites

- Install the python packages written in "requirements.txt".  
- The setup function works only if there is an instance of Postgres.  
- The "config.py" file contains the parameters for connecting to it.  
- Do not use localhost:5432 to avoid conflicts with the dockerized instance of Postgres. 
- Create a sql file to specify the primary and foreign keys of the schema as the following instructions:

> PRIMARY KEY table(columns);

> FOREIGN KEY table(columns) REFERENCES referenced_table(referenced_columns); 

- The "samples" folder contains two schema dumps useful for creating two schemas in the local postgreSQL instance to use 
the "setup" function. Annotations for these schemas are stored in the "notes" folder.
- In the main.py file, you can choose whether to use the setup function via the boolean variable "SETUP".  
- In addition, you must specify the name of the schema to be transformed and the path of the file that contains the 
annotations on the primary and foreign keys.
- Choose if primary keys must have deterministic or indeterministic values. The tool is less efficient when 
deterministic values are chosen. See below on how to choose this.
- Once everything is configured, you can run the main.
- Before to run the test to verify the correctness of the bridge table of the "loops" schema, ensure that the lines 196 
and 258 are uncommented in the file "transformation.py" in "tools/functions" folder, and lines 197 and 259 are 
commented out.

#### Uncomment to use deterministic values as primary keys (comment lines 197 and 259):

> at line 196: datatype = 'smallint'

> at line 258: query += f'row_number() over (order by {list(columns.keys())[0]})'
 
#### Uncomment to use indeterministic values as primary keys (comment lines 196 and 258):

> at line 197: datatype = 'varbinary'

> at line 259: query = 'cast(uuid() as varbinary)'