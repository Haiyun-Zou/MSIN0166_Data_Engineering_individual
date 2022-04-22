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


# load the data
teams = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/team.parquet").toPandas()
players = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/players.parquet").toPandas()

# get all the team_code and thier conference area
dict_team = {i:j for i,j in zip(players['team'], players['is_eastern'])}

# merge the area information with the team data frame
teams_t = teams.merge(pd.DataFrame({'team':dict_team.keys(), 'is_eastern': dict_team.values()}))

# convert the columns name
teams_t.columns = ['team_code', 'team_name', 'wins', 'loss', 'playoff', 'is_eastern']

# extract the information for players table in the database
players_t = players[['link', 'name', 'team', 'birth', 'age', 'No.', 'pos', 'guaranteed', 'height', 'weight', 'exp']]

# convert the columns name
players_t.columns = ['id', 'name', 'team_code', 'birth_year', 'age', 'number', 'possition', 'guaranteed', 'height', 'weight', 'exp']

# extract the current season and career results from players
current_season_result = players[['link', 'G_2122', 'GS_2122','MP_2122', 'FG%_2122', '3P%_2122', '2P%_2122', 'eFG%_2122', 'FT%_2122', 'ORB_2122', 'DRB_2122', 'AST_2122', 'STL_2122', 'BLK_2122', 'TOV_2122', 'PF_2122', 'PTS_2122']]
current_career_result = players[['link', 'G_career', 'GS_career','MP_career', 'FG%_career', '3P%_career', '2P%_career', 'eFG%_career', 'FT%_career', 'ORB_career', 'DRB_career', 'AST_career', 'STL_career', 'BLK_career', 'TOV_career', 'PF_career', 'PTS_career']]

# convert the columns name
current_season_result.columns = ['id', 'G', 'GS','MP', 'FGperc', '3Pperc', '2Pperc', 'eFGperc', 'FTperc', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
current_career_result.columns = ['id', 'G', 'GS','MP', 'FGperc', '3Pperc', '2Pperc', 'eFGperc', 'FTperc', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']

# convert all the data frame into spark data frame
teams_t_df = spark.createDataFrame(teams_t)
players_t_df = spark.createDataFrame(players_t)
current_season_result_df = spark.createDataFrame(current_season_result)
current_career_result_df = spark.createDataFrame(current_career_result)

# show the schema of the data frame
teams_t_df.printSchema()
players_t_df.printSchema()
current_season_result_df.printSchema()
current_career_result_df.printSchema()

# convert the data frame into parquet format
teams_t_df.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/teams_t.parquet", mode = 'overwrite')
players_t_df.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/players_t.parquet", mode = 'overwrite')
current_season_result_df.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/current_season_result.parquet", mode = 'overwrite')
current_career_result_df.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/current_career_result_df.parquet", mode = 'overwrite')