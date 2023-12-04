import os
import pandas as pd
import json
import logging as log
import argparse
import glob
import yaml
import psycopg2
import mysql.connector
from pymongo import MongoClient

log.basicConfig(filename='data_processing.log',
                    level=log.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

class ConfigHandler:
    @staticmethod
    def load_config(config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

class DataProcessor:
    def __init__(self):
        self.file_input = None
        self.format_type = None
        self.output = None
        self.db_config = None

    def flatten_json(self, data, parent_key='', sep='_'):
        for k, v in data.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                yield from self.flatten_json(v, new_key, sep=sep)
            elif isinstance(v, list):
                for i, val in enumerate(v):
                    new_list_key = new_key + sep + str(i)
                    if isinstance(val, dict):
                        yield from self.flatten_json(val, new_list_key, sep=sep)
                    else:
                        yield new_list_key, str(val).replace('\n', ' ')
            else:
                yield new_key, str(v).replace('\n', ' ')

    def load_data(self, file_path):
        log.info(f'Reading JSON contents from {file_path}...')
        with open(file_path, 'r') as file:
            data = json.load(file)
        log.info('Data loaded successfully.')
        return data

    def output_data(self, df, output_type, output=None):
        if output_type == 'csv':
            log.info('Writing to CSV...')
            df.to_csv(self.output, index=False) 
        else:
            self.write_to_database(df, output_type, output)

    def write_to_database(self, df, db_type, output_table):
        if db_type == 'postgres':
            log.info('Writing to PostgreSQL database...')
            conn = psycopg2.connect(**self.db_config['postgres'])
            cur = conn.cursor()
            df.to_sql(output_table, conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        elif db_type == 'mysql':
            log.info('Writing to MySQL database...')
            conn = mysql.connector.connect(**self.db_config['mysql'])
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {output_table} (...)")
            for _, row in df.iterrows():
                sql = f"INSERT INTO {output_table} (...) VALUES (...)"
                cursor.execute(sql, tuple(row))
            conn.commit()
            cursor.close()
            conn.close()

    def process(self):
        parser = argparse.ArgumentParser(description='Process JSON data and store in CSV or database')
        parser.add_argument('--file_input', help='Pattern of JSON file(s) in data folder, e.g., *.json')
        parser.add_argument('--format', choices=['csv','postgres','mysql','mongo'], help='Output format type (e.g csv, mysql, mongo, postgres)')
        parser.add_argument('--output', help='Filename for csv or table name for databases (for postgres, mysql, mongo)', required=True)
        args = parser.parse_args()

        self.file_input = args.file_input
        self.format_type = args.format
        self.output = args.output

        file_paths = glob.glob(self.file_input)
        for file_path in file_paths:
            if os.path.exists(file_path):
                data = self.load_data(file_path)
                self.db_config = ConfigHandler.load_config('db_config.yml')

                log.info('Flatten JSON to DataFrame')
                if isinstance(data, list):
                    flattened_data = (dict(self.flatten_json(record)) for record in data)
                else:
                    flattened_data = [dict(self.flatten_json(data))]

                df = pd.DataFrame(flattened_data)

                log.info(df.head())
                if "csv" in self.format_type:
                    self.output_data(df, self.format_type)
                else:
                    self.output_data(df, self.format_type, self.output)
            else:
                log.error(f'File not found: {file_path}')

if __name__ == "__main__":
    processor = DataProcessor()
    processor.process()
    log.info('Script execution ended.')
    print("Done")
