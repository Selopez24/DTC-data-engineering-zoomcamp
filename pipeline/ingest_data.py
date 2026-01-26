#!/usr/bin/env python

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import os

green_taxi_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "trip_type": "Int64",
    "congestion_surcharge": "float64"
}

zones_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


@click.group()
def cli():
    pass

def get_engine(pg_user, pg_pass, pg_host, pg_port, pg_db):
    return create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

@cli.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target-table', default='green_trips', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading parquet')
@click.option('--parquet-url', default=None, help='Custom URL for parquet file')
def ingest_green_trips(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize, parquet_url):
    if parquet_url is None:
        parquet_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    
    click.echo(f'Downloading green taxi data from: {parquet_url}')
    click.echo(f'Target table: {target_table}')
    
    engine = get_engine(pg_user, pg_pass, pg_host, pg_port, pg_db)
    

    click.echo('Downloading and reading parquet file...')
    df = pd.read_parquet(parquet_url)
    click.echo(f'Loaded {len(df)} rows from parquet file.')
    
    needed_columns = [col for col in (list(green_taxi_dtype.keys()) + parse_dates) if col in df.columns]
    df = df[needed_columns]
    

    for col, dtype_str in green_taxi_dtype.items():
        if col in df.columns:
            if dtype_str == 'Int64':
                df[col] = df[col].astype('float64').astype('Int64')
            else:
                df[col] = df[col].astype(dtype_str)
    
    n_chunks = max(1, len(df) // chunksize)
    click.echo(f'Inserting data in {n_chunks} chunks...')
    
    first = True
    total_rows = 0
    
    for i in tqdm(range(0, len(df), chunksize), desc='Loading green taxi data'):
        df_chunk = df.iloc[i:i+chunksize]
        if first:

            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace',
                index=False
            )
            first = False
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )
        total_rows += len(df_chunk)
    
    click.echo(f'Successfully loaded {total_rows} rows into {target_table} table.')

@cli.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='zones', help='Target table name')
@click.option('--csv-url', default=None, help='Custom URL for zones CSV file')
def ingest_zones(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, csv_url):
    if csv_url is None:
        csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    
    click.echo(f'Downloading taxi zones data from: {csv_url}')
    click.echo(f'Target table: {target_table}')
    
    engine = get_engine(pg_user, pg_pass, pg_host, pg_port, pg_db)
    
    df = pd.read_csv(csv_url, dtype=zones_dtype)
    
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )
    
    click.echo(f'Successfully loaded {len(df)} zones into {target_table} table.')

if __name__ == '__main__':
    cli()
