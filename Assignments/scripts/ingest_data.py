"""
Week 1 - Coding Assignment
Specification : To ingest data in DuckDB Database
Author : Antara Saha
"""

# Import Libraries
import pandas as pd
import os
import glob
import duckdb

conn = duckdb.connect(database='Main_DB_95797') # create a database

# Load all the csv files:
# 1. Loading central park weather data
filename = 'Dataset/central_park_weather.csv'
tmp_data = pd.read_csv(filename)
tmp_data["filename"] = filename

print("Loading Rows from File: central_park_weather.csv")
conn.execute("""
    CREATE TABLE central_park_weather AS SELECT * FROM tmp_data;
    """
)
print("Loading central_park_weather.csv done\n")

# 2. Loading daily_citi_bike_trip_counts_and_weather     
filename = 'Dataset/daily_citi_bike_trip_counts_and_weather.csv'
tmp_data = pd.read_csv(filename)
tmp_data["filename"] = filename

print("Loading Rows from File: daily_citi_bike_trip_counts_and_weather.csv")
conn.execute("""
    CREATE TABLE daily_citi_bike_trip_counts_and_weather AS SELECT * FROM tmp_data;
""")
print("Loading daily_citi_bike_trip_counts_and_weather.csv done\n")

# 3. Loading fhv_bases
filename = 'Dataset/fhv_bases.csv'
tmp_data = pd.read_csv(filename)
tmp_data["filename"] = filename

print("Loading Rows from File: fhv_bases.csv")
conn.execute("""
    CREATE TABLE fhv_bases AS SELECT * FROM tmp_data;
    """
)
print("Loading fhv_bases.csv done\n")

# 4. Loading Bike Data
# Get file path
folder_path = 'Dataset/bike'
file_list = os.listdir(folder_path)

print("Creating table bike_data")
# Create bike_data table in DuckDB
conn.execute("""
CREATE TABLE bike_data(tripduration BIGINT, starttime TIMESTAMP, stoptime TIMESTAMP, "start station id" BIGINT, 
"start station name" VARCHAR, "start station latitude" DOUBLE, "start station longitude" DOUBLE,"end station id" BIGINT,
 "end station name" VARCHAR, "end station latitude" DOUBLE, "end station longitude" DOUBLE, bikeid BIGINT, 
 usertype VARCHAR, "birth year" BIGINT, gender TINYINT,
 ride_id VARCHAR, rideable_type VARCHAR, started_at TIMESTAMP, ended_at TIMESTAMP, start_station_name VARCHAR, 
 start_station_id VARCHAR, end_station_name VARCHAR, end_station_id VARCHAR, start_lat DOUBLE, start_lng DOUBLE, 
 end_lat DOUBLE, end_lng DOUBLE, member_casual VARCHAR,filename VARCHAR);
""")

# Column name list
columns_lst =["tripduration","starttime","stoptime","start station id","start station name",
"start station latitude","start station longitude","end station id","end station name",
"end station latitude","end station longitude","bikeid","usertype","birth year","gender","ride_id",
"rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id",
"start_lat","start_lng","end_lat","end_lng","member_casual","filename"]

# Insert rows from csv.gz file to bike_data table
for file_name in file_list:
    if file_name.endswith('.csv.gz'):
        file_path = os.path.join(folder_path, file_name)
        print("Loading Rows from File: ",file_name)
        tmp_data = pd.read_csv(file_path,compression='gzip',low_memory=False)
        tmp_data["filename"] = file_path
        tmp_data = tmp_data.reindex(columns=columns_lst)
        conn.execute("""
            INSERT INTO bike_data SELECT * FROM tmp_data;
            """)
print("Loading bike_data done\n")
     
# 5. Loading fhv_tripdata
print("Loading Rows from File: fhv_tripdata_*.parquet")
conn.execute("""
    CREATE TABLE fhv_tripdata AS SELECT * FROM read_parquet('Dataset/taxi/fhv_tripdata_*.parquet', filename=true);
    """
)
print("Loading fhv_tripdata done\n")

# 6. Loading fhvhv_tripdata
print("Loading Rows from File: fhvhv_tripdata_*.parquet")
conn.execute("""
    CREATE TABLE fhvhv_tripdata AS SELECT * FROM read_parquet('Dataset/taxi/fhvhv_tripdata_*.parquet', filename=true);
    """
)
print("Loading fhvhv_tripdata done\n")

# 7. Loading green_tripdata
print("Loading Rows from File: green_tripdata_*.parquet")
conn.execute("""
    CREATE TABLE green_tripdata AS SELECT * FROM read_parquet('Dataset/taxi/green_tripdata_*.parquet', filename=true);
    """
)
print("Loading green_tripdata done\n")

# 8. Loading yellow_tripdata
print("Loading Rows from File: yellow_tripdata_*.parquet")
conn.execute("""
    CREATE TABLE yellow_tripdata AS SELECT * FROM read_parquet('Dataset/taxi/yellow_tripdata_*.parquet', filename=true);
    """
)
print("Loading yellow_tripdata done\n")

print ("All tables done!")
# End Script
