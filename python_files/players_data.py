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


# create a data frame to store each player's information
players = pd.DataFrame(columns = ['name', 'link','twitter_account', 'No.', 'pos', 'height', 'weight', 'birth', 'age'])

# loop through each team page and get all the players in each team and their related information
for t in team_df.team:
    URL =f"https://www.basketball-reference.com/teams/{t}/2022.html"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    soup_body = str(soup.body)
    
    no = [i for i in re.findall(r'data-stat="number" scope="row">(.*)</th>', soup_body)]
    link_name = [i.split('</a>')[0].split('">') for i in re.findall(r'"><a href="(.*?)data-stat="pos"', soup_body)]
    pos = re.findall(r'data-stat="pos">(.*?)</td>', soup_body)
    height = [float(i.replace('-', '.')) for i in re.findall(r'data-stat="height">(.*?)</td>', soup_body)]
    weight = [int(i) for i in re.findall(r'data-stat="weight">(.*?)</td>', soup_body)]
    birth = re.findall(r'</td><td class="left" csk="(.*)" data-stat="birth_date">', soup_body)
    exp = re.findall(r'data-stat="years_experience">(.*)</td><td class', soup_body)
    list_team = [t for i in range(len(no))]
    playoff = team_df[team_df['team'] == t]['in_playoff'].values[0]
    list_playoff_t = [playoff for i in range(len(no))] 
    players = players.append(pd.DataFrame({'team': list_team,'playoff': list_playoff_t, 'name': [i[1] for i in link_name], 'link': [i[0] for i in link_name], 'No.': no, 'pos': pos, 'height': height, 'weight': weight, 'birth':birth, 'exp': exp})).reset_index(drop=True)

# convert the experience information with 'R' to 0.5 and convert the data type into float64
players['exp'] = players['exp'].replace('R', 0.5)
players['exp'] = players['exp'].astype(np.float64)

# temporary lists for all the players' information
temp_twitter_account = []
temp_in_season = []
temp_list_2122 = []
temp_list_career = []
# loop through each player's page and get their game information
for count,l in enumerate(players.link):
    URL =f"https://www.basketball-reference.com{l}"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    soup_body = str(soup.body)
    
    temp_twitter_account.append(re.findall(r'<a href="https://twitter.com/(.*)">', soup_body))
    
    temp_in_season.append('gamelog/2022' in soup_body)
    temp_list_2122.append(re.findall(fr'{l[1:-5]}/gamelog/2022">(.*?)</td></tr> ', soup_body))
    temp_list_career.append(re.findall(r'</tbody><tfoot><tr><th class="left" data-stat="season" scope="row">Career(.*?)</td></tr>', soup_body))
# add the columns into the players data frame
players['twitter_account'] = temp_twitter_account
players['in_2021_22_season'] = temp_in_season
players['2021_2022_season'] = temp_list_2122
players['career'] = temp_list_career

# get the age of each player by 2022-birth year
players['age'] = players['birth'].apply(lambda x: 2022 - int(x[:4]))

# fill the empty information 
players['twitter_account'] = players['twitter_account'].apply(lambda x: x[0] if len(x) > 0 else '')
players['2021_2022_season'] = players['2021_2022_season'].apply(lambda x: x[0] if len(x) > 0 else '')
players['career'] = players['career'].apply(lambda x: x[0] if len(x) > 0 else '')

def get_info(pattern, list_info):
    '''
    function to get information for different attributes
    '''
    temp_list = []
    for count, i in enumerate(list_info):
        if i!='':
            result = re.findall(pattern, i)[0]
            if 'strong' in result:
                result = result.replace('strong','').replace('/','').replace('<','').replace('>','')
            if result == '':
                result = 0
            
            temp_list.append(result)
        else:
            temp_list.append(0)
    return temp_list

# create new columns and get the corresponding information and convert the data type
players['G_2122'] = get_info((r'data-stat="g">(.*?)</td'), players['2021_2022_season'])
players['G_2122'] = players['G_2122'].astype(np.int64)
players['GS_2122'] = get_info((r'data-stat="gs">(.*?)</td'), players['2021_2022_season'])
players['GS_2122'] = players['GS_2122'].astype(np.int64)
players['MP_2122'] = get_info((r'data-stat="mp_per_g">(.*?)</td'), players['2021_2022_season'])
players['MP_2122'] = players['MP_2122'].astype(np.float64)
players['FG%_2122'] = get_info((r'data-stat="fg_pct">(.*?)</td'), players['2021_2022_season'])
players['FG%_2122'] = players['FG%_2122'].astype(np.float64)
players['3P%_2122'] = get_info((r'data-stat="fg3_pct">(.*?)</td'), players['2021_2022_season'])
players['3P%_2122'] = players['3P%_2122'].astype(np.float64)
players['2P%_2122'] = get_info((r'data-stat="fg2_pct">(.*?)</td'), players['2021_2022_season'])
players['2P%_2122'] = players['2P%_2122'].astype(np.float64)
players['eFG%_2122'] = get_info((r'data-stat="efg_pct">(.*?)</td'), players['2021_2022_season'])
players['eFG%_2122'] = players['eFG%_2122'].astype(np.float64)
players['FT%_2122'] = get_info((r'data-stat="ft_pct">(.*?)</td'), players['2021_2022_season'])
players['FT%_2122'] = players['FT%_2122'].astype(np.float64)
players['ORB_2122'] = get_info((r'data-stat="orb_per_g">(.*?)</td'), players['2021_2022_season'])
players['ORB_2122'] = players['ORB_2122'].astype(np.float64)
players['DRB_2122'] = get_info((r'data-stat="drb_per_g">(.*?)</td'), players['2021_2022_season'])
players['DRB_2122'] = players['DRB_2122'].astype(np.float64)
players['AST_2122'] = get_info((r'data-stat="ast_per_g">(.*?)</td'), players['2021_2022_season'])
players['AST_2122'] = players['AST_2122'].astype(np.float64)
players['STL_2122'] = get_info((r'data-stat="stl_per_g">(.*?)</td'), players['2021_2022_season'])
players['STL_2122'] = players['STL_2122'].astype(np.float64)
players['BLK_2122'] = get_info((r'data-stat="blk_per_g">(.*?)</td'), players['2021_2022_season'])
players['BLK_2122'] = players['BLK_2122'].astype(np.float64)
players['TOV_2122'] = get_info((r'data-stat="tov_per_g">(.*?)</td'), players['2021_2022_season'])
players['TOV_2122'] = players['TOV_2122'].astype(np.float64)
players['PF_2122'] = get_info((r'data-stat="pf_per_g">(.*?)</td'), players['2021_2022_season'])
players['PF_2122'] = players['PF_2122'].astype(np.float64)
players['PTS_2122'] = get_info((r'data-stat="pts_per_g">(.*)'), players['2021_2022_season'])
players['PTS_2122'] = players['PTS_2122'].astype(np.float64)

# create new columns and get the corresponding information and convert the data type
players['G_career'] = get_info((r'data-stat="g">(.*?)</td'), players['career'])
players['G_career'] = players['G_career'].astype(np.int64)
players['GS_career'] = get_info((r'data-stat="gs">(.*?)</td'), players['career'])
players['GS_career'] = players['GS_career'].astype(np.int64)
players['MP_career'] = get_info((r'data-stat="mp_per_g">(.*?)</td'), players['career'])
players['MP_career'] = players['MP_career'].astype(np.float64)
players['FG%_career'] = get_info((r'data-stat="fg_pct">(.*?)</td'), players['career'])
players['FG%_career'] = players['FG%_career'].astype(np.float64)
players['3P%_career'] = get_info((r'data-stat="fg3_pct">(.*?)</td'), players['career'])
players['3P%_career'] = players['3P%_career'].astype(np.float64)
players['2P%_career'] = get_info((r'data-stat="fg2_pct">(.*?)</td'), players['career'])
players['2P%_career'] = players['2P%_career'].astype(np.float64)
players['eFG%_career'] = get_info((r'data-stat="efg_pct">(.*?)</td'), players['career'])
players['eFG%_career'] = players['eFG%_career'].astype(np.float64)
players['FT%_career'] = get_info((r'data-stat="ft_pct">(.*?)</td'), players['career'])
players['FT%_career'] = players['FT%_career'].astype(np.float64)
players['ORB_career'] = get_info((r'data-stat="orb_per_g">(.*?)</td'), players['career'])
players['ORB_career'] = players['ORB_career'].astype(np.float64)
players['DRB_career'] = get_info((r'data-stat="drb_per_g">(.*?)</td'), players['career'])
players['DRB_career'] = players['DRB_career'].astype(np.float64)
players['AST_career'] = get_info((r'data-stat="ast_per_g">(.*?)</td'), players['career'])
players['AST_career'] = players['AST_career'].astype(np.float64)
players['STL_career'] = get_info((r'data-stat="stl_per_g">(.*?)</td'), players['career'])
players['STL_career'] = players['STL_career'].astype(np.float64)
players['BLK_career'] = get_info((r'data-stat="blk_per_g">(.*?)</td'), players['career'])
players['BLK_career'] = players['BLK_career'].astype(np.float64)
players['TOV_career'] = get_info((r'data-stat="tov_per_g">(.*?)</td'), players['career'])
players['TOV_career'] = players['TOV_career'].astype(np.float64)
players['PF_career'] = get_info((r'data-stat="pf_per_g">(.*?)</td'), players['career'])
players['PF_career'] = players['PF_career'].astype(np.float64)
players['PTS_career'] = get_info((r'data-stat="pts_per_g">(.*)'), players['career'])
players['PTS_career'] = players['PTS_career'].astype(np.float64)

# convert the players data into spark data frame
players_df = spark.createDataFrame(players)

# show the schema of the spark data frame
players_df.printSchema()

# convert the data frame into parquet format
players_df.write.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/players.parquet", mode = 'overwrite')