# Data Transformation
This is a test solution to meet technical requirements


# Installation
Run pip install -r requirements.txt

Edit the db_config.yml and add your database details.

## Create a virtual environment
	
	python -m venv your_virtualenv_name

## Activate the virtual environment (Windows)

Activate the virtual environment (Windows)

	your_virtualenv_name\Scripts\activate

Activate the virtual environment (Unix or MacOS)
	
	source your_virtualenv_name/bin/activate


## Securely setup your passwords

Setup a secure method of storing the database passwords into the environment variables: 

	export POSTGRES_PASSWORD="your_secure_password"
	export MONGO_PASSWORD="your_secure_password"
	export MYSQL_PASSWORD="your_secure_password"


# Usage
Run the following command:
	python tf_json.py --file_input [your_source_data_filepath] --format [your_output_format] --output [tablename_or_outputfile]

## Examples

Execute the following command for outputing values to a csv

	python tf_json.py --file_input ../data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json --format csv --output transformed.csv

The following will command will upload fhir data to a database (e.g. postgres/mysql/mongo)

	python tf_json.py --file_input ../data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json --format postgres --output your_table_name

