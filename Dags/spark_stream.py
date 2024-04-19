import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather_forecast (
        id INT PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        timestamp TIMESTAMP,
        temperature_2m FLOAT,
        relative_humidity_2m FLOAT,
        dew_point_2m FLOAT,
        apparent_temperature FLOAT,
        precipitation_probability FLOAT,
        precipitation TEXT,
        rain FLOAT,
        showers FLOAT,
        snowfall FLOAT,
        snow_depth FLOAT,
        weather_code INT,
        cloud_cover FLOAT,
        cloud_cover_low FLOAT,
        cloud_cover_mid FLOAT,
        cloud_cover_high FLOAT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")

    id = kwargs.get('id')
    latitude = kwargs.get('latitude')
    longitude = kwargs.get('longitude')
    timestamp = kwargs.get('timestamp')
    temperature_2m = kwargs.get('temperature_2m')
    relative_humidity_2m = kwargs.get('relative_humidity_2m')
    dew_point_2m = kwargs.get('dew_point_2m')
    apparent_temperature = kwargs.get('apparent_temperature')
    precipitation_probability = kwargs.get('precipitation_probability')
    precipitation = kwargs.get('precipitation')
    rain = kwargs.get('rain')
    showers = kwargs.get('showers')
    snowfall = kwargs.get('snowfall')
    snow_depth = kwargs.get('snow_depth')
    weather_code = kwargs.get('weather_code')
    cloud_cover = kwargs.get('cloud_cover')
    cloud_cover_low = kwargs.get('cloud_cover_low')
    cloud_cover_mid = kwargs.get('cloud_cover_mid')
    cloud_cover_high = kwargs.get('cloud_cover_high')

    try:
        session.execute("""
            INSERT INTO spark_streams.weather_forecast(
                id, latitude, longitude, timestamp, temperature_2m, relative_humidity_2m, 
                dew_point_2m, apparent_temperature, precipitation_probability, precipitation, 
                rain, showers, snowfall, snow_depth, weather_code, cloud_cover, cloud_cover_low, 
                cloud_cover_mid, cloud_cover_high)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, latitude, longitude, timestamp, temperature_2m, relative_humidity_2m,
              dew_point_2m, apparent_temperature, precipitation_probability, precipitation,
              rain, showers, snowfall, snow_depth, weather_code, cloud_cover, cloud_cover_low,
              cloud_cover_mid, cloud_cover_high))
        logging.info(f"Data inserted with id {id}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'your_cassandra_host') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'your_kafka_host:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(['your_cassandra_host'])  # Replace with your Cassandra host
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to error {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("temperature_2m", StringType(), False),
        StructField("relative_humidity_2m", StringType(), False),
        StructField("dew_point_2m", StringType(), False),
        StructField("apparent_temperature", StringType(), False),
        StructField("precipitation_probability", StringType(), False),
        StructField("precipitation", StringType(), False),
        StructField("rain", StringType(), False),
        StructField("showers", StringType(), False),
        StructField("snowfall", StringType(), False),
        StructField("snow_depth", StringType(), False),
        StructField("weather_code", StringType(), False),
        StructField("cloud_cover", StringType(), False),
        StructField("cloud_cover_low", StringType(), False),
        StructField("cloud_cover_mid", StringType(), False),
        StructField("cloud_cover_high", StringType(), False)
    ])

    # Select the value column and cast it to string, then apply the schema
    # Finally, select all fields from the resulting DataFrame
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return sel

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # Define the streaming query
            streaming_query = (selection_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'weather_forecast')  # Assuming the table name is weather_forecast
                               .start())

            # Await termination of the streaming query
            streaming_query.awaitTermination()




