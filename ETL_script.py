import os
import pandas as pd
import argparse
from sqlalchemy import create_engine
from time import time

# Download data
def download(url):
    os.system(f'wget {url}')
    filename = os.path.basename(url)
    if url.endswith('csv.gz'):
        os.system(f'gunzip "{filename.strip()}"')
        output, extension = os.path.splitext(filename.strip())
    else:
        output = filename.strip()
    return output

# Create connection with postgres
def connection():
    db_path = 'postgresql://root:root@localhost:5432/ny_taxi'
    engine = create_engine(db_path)
    return engine

# Create database schema
def create_schema(output, engine, db_name):
    df_iter = pd.read_csv(output, iterator=True, chunksize=100000)
    df = next(df_iter).head(0).to_sql(db_name, con=engine, if_exists='replace', index=False)
    return df_iter

# Insert and process data
def ingestion(df_iter, engine):
    loop_start = time()
    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql('yellow_tripdata_2021', con=engine, if_exists='append', index=False)
            
            t_end = time()
            
            print(f'Inserted another 100,000 rows into DB, took {round((t_end - t_start),2)} secs')
        except StopIteration:
            print('Data ingestion completed.')
            print(f'Overall Data Ingestion time {round((loop_start -time()),2)} secs')
            break

# Main function 
def main(args):
    url = args.url
    db_name = args.db_name
    output = download(url)
    engine = connection()
    df_iter = create_schema(output, engine, db_name)
    ingestion(df_iter, engine)
    
if __name__ == '__main__':
    # Create required command line arguments when running this file
    # Create argparse object which help to parse the arguments of the command line
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # Specify argument needed
    parser.add_argument('--url', required=True, help='url to download data')
    parser.add_argument('--db_name', required=True, help='name of table in Postgres')
    # Parse the arguments
    args = parser.parse_args()
    
    main(args)
    