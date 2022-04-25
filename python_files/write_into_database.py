# import the packages
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os
import requests
from datetime import datetime

# set up the spark envrionment
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"

# import spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark App").config("spark.jars", "postgresql-42.3.2.jar").getOrCreate()

os.system("PGPASSWORD=qwerty123 psql -h depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com -d haiyunzou21 -U haiyunzou21 -c '\i /project/MSIN0166_Data_Engineering_individual/NBA.sql'")

# load the data
teams_t_df = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/teams_t.parquet")
players_t_df = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/players_t.parquet")
current_season_result_df = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/current_season_result.parquet")
current_career_result_df = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/current_career_result_df.parquet")

# information for log into postgresql
postgres_uri = "jdbc:postgresql://depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com:5432/haiyunzou21"
user = "haiyunzou21"
password = "qwerty123"

# write the data into the database
teams_t_df.write.jdbc(url=postgres_uri, table="NBA.teams", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
players_t_df.write.jdbc(url=postgres_uri, table="NBA.players", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
current_season_result_df.write.jdbc(url=postgres_uri, table="NBA.current_season_result", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
current_career_result_df.write.jdbc(url=postgres_uri, table="NBA.current_career_result", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })