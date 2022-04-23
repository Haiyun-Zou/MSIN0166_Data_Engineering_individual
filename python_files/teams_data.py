# import the packages
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os
import requests
from datetime import datetime
np.random.seed(42)

# set up the spark envrionment
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"

# import spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark App").config("spark.jars", "postgresql-42.3.2.jar").getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

# using BeautifulSoup to get the web page information
URL ="https://www.basketball-reference.com/"
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
soup_body = str(soup.body)

# get all the team related data
list_info = re.findall(r'data-stat="payroll_text">(.*)</td></tr>', soup_body)

# set the team code and their full name into a dictionary
dict_info = {i[20:23]:[re.findall(r'title="(.*) Team Payroll', i)[0], int(re.findall(r'data-stat="wins">(.*)</', i)[0]), int(re.findall(r'data-stat="losses">(.*)', i)[0])] for i in list_info}

# check the playoff data
URL ="https://www.basketball-reference.com/playoffs/NBA_2022.html"
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
soup_body = str(soup.body)

# get the playoff team name
list_playoff = [i.split("html'>")[1] for i in re.findall(r'data-stat="team" >(.*?)</a></td><td', soup_body)]

# convert the team data into a data frame
team_df = pd.DataFrame({'team': dict_info.keys(), 'name': [i[0] for i in dict_info.values()], 'win': [int(i[1]) for i in dict_info.values()], 'loss': [int(i[2]) for i in dict_info.values()]})

# match all the playoff information with the data frame
list_in_playoff = [1 if i in list_playoff else 0 for i in team_df.name]

# add the column of  in_playoff into the data frame for team information
team_df['in_playoff'] = list_in_playoff

# convert the team information into spark data frame
df_team = spark.createDataFrame(team_df)

# show the schema of the data frame
df_team.printSchema()

# convert the data frame into parquet format
df_team.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/team.parquet", mode = 'overwrite')