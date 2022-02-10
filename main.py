import etl as e
import argparse
import time
import sys
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import point


#DB_SCHEMA = "sa"
#TABLE = "rides"
#DOWNLOAD_DIR = "data/original"
#PROCESSED_DIR = "data/processed"
#STATIC_DIR = "data/static"


def extraction(config: dict) -> pd.DataFrame:
    """ Runs extraction

        Args:
            config (str): configuration dictionary
    """
    e.info("EXTRACTION: START DATA EXTRACTION")
    url = config["url"]

    e.info("EXTRACTION: CREATING DATA FRAME")
    df = e.read_json(url)
    #you can check if is better to create a geodataframe instead of dataframe
    print(df["date"])

    e.info("EXTRACTION: COMPLETED")
#     #sys.exit(0)
    return df


    e.info("TRANSFORMATION: DATA SUBSETTING BY VARIABLE")

   
def transformation(config: dict, df: pd.DataFrame) -> None:
    """Runs transformation

    Args:
        config (dict): [description]
    """
    e.info("TRANSFORMATION: START TRANSFORMATION")
    e.info("TRANSFORMATION: READING DATA")
    
    print(df['coordinates'])

    unnested_df = df.join(pd.DataFrame(df.coordinates.tolist(),index=df.index).add_prefix('coordinates_'))

    print(unnested_df.columns)
    print(unnested_df.head())

    temp_df = unnested_df[unnested_df['id'].str.contains("METEMP")==True]
    hum_df = unnested_df[unnested_df['id'].str.contains("ME00HR")==True]
    noise_df = unnested_df[unnested_df['id'].str.contains("RULAEQ")==True]
    #print(temp_df.head())
    print(temp_df[["id", "date", "value"]])
    
    
     #Task create dataframe for STATION and covert to geodataframe  

    stations_df = unnested_df[['id', 'coordinates_lat', 'coordinates_lng']]

    stations_df["id_sensor"] = stations_df["id"].str[-4:]

    del stations_df['id']

    stations_df_f = stations_df.drop_duplicates()

    print(stations_df_f.head())

    print(stations_df_f.shape)


#convert stations_df_f to geodataframe

# geometry = [point(xy) for xy in unnested_df[['id', 'coordinates_lat', 'coordinates_lng']] ]

# sens_stations = gpd.GeoDataFrame(stations_df_f, geometry = geomtry)

# stations_df = unnested_df[['id', 'coordinates_lat', 'coordinates_lng']]
stations_df_f = stations_df.drop_duplicates()
gdf = gpd.GeoDataFrame(
    stations_df_f, geometry=gpd.points_from_xy(stations_df_f.coordinates_lng, stations_df_f.coordinates_lat))
 
print(gdf.head())


      #e.info("TRANSFORMATION: DATA SUBSETTING BY VARIABLE")

    #temp_df = unnested_df.loc[unnested_df['id'][] == 'TEMP', ["id", "date", "value"]] 

   
    

    #df = pd.DataFrame(unnested_df, columns=["temperature", "humidity", "noise", "stations"])
    #df

    #e.info("TRANSFORMATION: DATA SUBSETTING BY VARIABLE")

    #convert stations_df to geodataframe

    

    # Select only the rows in the analysis period
    # What happens if there is no data in this period?
    # What should we do in that case?
    #start_date = config["period"]["start_date"]
    #end_date = config["period"]["end_date"]
    #df = df.loc[(start_date <= df["tpep_pickup_datetime"]) & (df["tpep_dropoff_datetime"] <= end_date)]
   

#    e.info("TRANSFORMATION: SUBSETTING DONE")
#    e.info("TRANSFORMATION: ADDING SPATIAL INFORMATION")
    ### here should be something firts in order to convert the dataframe into geodataframe

    #Add latitude and longitude columns from url
    #gdf = e.read_json(url)
    # gdf['latitude'] = gdf.centroid.y
    # gdf['longitude'] = gdf.centroid.x
    # locations = gdf[['id', 'original_id', 'coordinates']]
    # Rename columns to match the the join column and output columns
    # pu_locations = locations.rename(columns={
    #     "LocationID":"PULocationID",
    #     "latitude":"pickup_latitude",
    #     "longitude":"pickup_longitude"})
    # do_locations = locations.rename(columns={
    #     "LocationID":"DOLocationID",
    #     "latitude":"dropoff_latitude",
    #     "longitude":"dropoff_longitude"})
    # # Join the pickup and drop off latitude and longitude columns
    # df = df.merge(pu_locations, on='PULocationID')
    # df = df.merge(do_locations, on='DOLocationID')

#     # Rename columns to match the database model
#     df = df.rename(columns={
#         "tpep_pickup_datetime":"pickup_datetime",
#         "tpep_dropoff_datetime":"dropoff_datetime",
#         "RatecodeID":"rate_code"})
    
    # cols = config["columns"]

#     # This is a simple transformation, but simple transformations
#     # can be quite tricky. Consider what happens if there is
#     # a column in cols no present in df?
#     # How can we save that?
#    df = df[cols]
#    e.info("TRANSFORMATION: SAVING TRANSFORMED DATA")
#    e.write_json(df, fname=f"{PROCESSED_DIR}/{fname}", sep=",")
#    e.info("TRANSFORMATION: SAVED")
#    e.info("TRANSFORMATION: COMPLETED")


# def load(config: dict, chunksize: int=1000) -> None:
#     """Runs load

#     Args:
#         config (dict): configuration dictionary
#         chunksize (int): the number of rows to be inserted at one time
#     """
#     try:
#         fname = config["fname"]
#         db = e.DBController(**config["database"])
#         e.info("LOAD: READING DATA")
#         df = e.read_csv(f"{PROCESSED_DIR}/{fname}")
#         e.info("LOAD: DATA READ")
#         e.info("LOAD: INSERTING DATA INTO DATABASE")
#         db.insert_data(df, DB_SCHEMA, TABLE, chunksize=chunksize)
#         e.info("LOAD: DONE")
#     except Exception as err:
#         e.die(f"LOAD: {err}")


def parse_args() -> str:
    """ Reads command line arguments

        Returns:
            the name of the configuration file
    """
    parser = argparse.ArgumentParser(description="GPS: project")
    parser.add_argument("--config_file", required=False, help="The configuration file",default="./config/00.yml")
    args = parser.parse_args()
    return args.config_file

def time_this_function(func, **kwargs) -> str:
    """ Times function `func`

        Args:
            func (function): the function we want to time

        Returns:
            a string with the execution time
    """
    import time
    t0 = time.time()
    func(**kwargs)
    t1 = time.time()
    return f"'{func.__name__}' EXECUTED IN {t1-t0:.3f} SECONDS"

def main(config_file: str) -> None:
    """Main function for ETL

    Args:
        config_file (str): configuration file
    """
    config = e.read_config(config_file)
    df = extraction(config)
    msg = time_this_function(extraction, config=config)
    e.info(msg)
    transformation(config, df)
    msg = time_this_function(transformation, config=config)
    e.info(msg)
    #load(config, chunksize=10000)
    #msg = time_this_function(load, config=config, chunksize=1000)
    #e.info(msg)


if __name__ == "__main__":
    config_file = parse_args()
    main(config_file)

