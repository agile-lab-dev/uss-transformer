# To run the project:

#### Install the python packages written in "requirements.txt".

####  The setup function works only if there is an instance of Postgres. 

#### The "config.py" file contains the parameters for connecting to it. 

#### Do not use localhost:5432 to avoid conflicts with the dockerized instance of Postgres.

#### Create a sql file to specify the primary and foreign keys of the schema as the following instructions:

> PRIMARY KEY table(columns);

> FOREIGN KEY table(columns) REFERENCES referenced_table(referenced_columns);

#### In the main.py file, you can choose whether to use the setup function via the boolean variable "SETUP". 

#### In addition, you must specify the name of the schema to be transformed and the path of the file that contains the annotations on the primary and foreign keys.

#### Once everything is configured, you can run the main.