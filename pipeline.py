import mysql.connector as connection
import pandas as pd
import pandas_gbq as pdbq

import os
import ssl

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context




def extract_table_from_mysql(table_name, my_sql_connection):
    extraction_query = f'SELECT * FROM {table_name}'
    df_table_data = pd.read_sql(extraction_query, my_sql_connection)
    return df_table_data


def transform_data_from_table(df_table_data):
    return df_table_data


def load_data_into_bigquery(bq_project_id, dataset, table_name, df_table_data):
    full_table_name_bg = f'{dataset}.{table_name}'
    pdbq.to_gbq(df_table_data, full_table_name_bg,
                project_id=bq_project_id, if_exists='replace')


def data_pipeline_mysql_to_bq(mysql_host, mysql_database, mysql_user, mysql_password, bq_project_id, dataset):
    try:
        mydb = connection.connect(host=mysql_host, database=mysql_database,
                                  user=mysql_user, passwd=mysql_password, use_pure=True)
        all_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
        df_tables = pd.read_sql(all_tables_query, mydb,
                                params=[mysql_database])

        print(df_tables.columns)

        for table in df_tables['TABLE_NAME']:
            df_table_data = extract_table_from_mysql(table, mydb)
            df_table_data = transform_data_from_table(df_table_data)
            load_data_into_bigquery(
                bq_project_id, dataset, table, df_table_data)
            print(f"Ingested table {table}")

        mydb.close()
    except Exception as e:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()
        print(f"An error occurred: {e}")


# Parâmetros de Conexão
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '36629228'
mysql_database = 'omni_management'
bq_project_id = 'dbt-analytics-engineer-407714'
dataset = 'omnichannel_raw'

data_pipeline_mysql_to_bq(mysql_host, mysql_database,
                          mysql_user, mysql_password, bq_project_id, dataset)
