from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import uuid
import logging
import time
import sys

#Pre-processing the data
# Function to configure logging
def configure_logging():
    """Configures logging for the script."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to generate UUID
def generate_uuid():
    """Generates a UUID."""
    return str(uuid.uuid4())
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

# Function to wait for Cassandra to be ready
def wait_for_cassandra():
    """Waits for Cassandra to be ready."""
    retries = 5
    delay = 10  # seconds

def create_selection_df_from_kafka(spark_df):
    pass