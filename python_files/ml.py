# import the packages
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os
import requests
from datetime import datetime
from IPython.display import Image
import matplotlib.pyplot as plt
np.random.seed(42)

# set up the spark envrionment
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"

# import spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark App").config("spark.jars", "postgresql-42.3.2.jar").getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

# load the data from parquet file
players = spark.read.parquet("/project/MSIN0166_Data_Engineering_individual/parquet_files/players.parquet").toPandas()

# convert the categorecal data into dummies
dummies_df = pd.get_dummies(players['pos'], prefix='is')

# add the columns into the data frame
for c in dummies_df.columns:
    players[c] = dummies_df[c]

# convert from True/False to 1/0
players['in_2021_22_season'] = players['in_2021_22_season'].replace({True:1, False:0})

players = players.sort_values('name')

# drop the columns that are not used for machine learning
players = players.drop([ 'team', 'name', 'link', 'pos', 'No.'], axis = 1)

import yaml
from sklearn.model_selection import train_test_split
# load the yaml document
params = yaml.safe_load(open("/project/MSIN0166_Data_Engineering_individual/params.yaml"))["ML"]

# split the train and test sets
players_train, players_test = train_test_split(players, test_size = params['split'], shuffle = params['shuffle'], random_state = params['seed'])

# extract the columns for transformation
players_train_trans = pd.DataFrame(players_train[players_train.columns[9:-5]])
players_test_trans = pd.DataFrame(players_test[players_test.columns[9:-5]])
X_train = np.array(players_train_trans)
X_test = np.array(players_test_trans)

from sklearn.preprocessing import StandardScaler
# standardise the data
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.fit_transform(X_test)

from sklearn.decomposition import PCA
# get the first 10 pca components
pca_train = PCA(n_components=params['n_components'])
pca_test = PCA(n_components = params ['n_components'])
pca_train.fit(X_train)
pca_test.fit(X_test)
players_pca_train= pca_train.transform(X_train)
players_pca_test= pca_test.transform(X_test)

sum(pca_train.explained_variance_ratio_)


# plot the PCA variation
plt.figure(figsize = (8,5))
plt.plot(np.cumsum(pca_train.explained_variance_ratio_),linestyle='solid', marker='o')
plt.xlabel('Number of Components')
plt.ylabel('Cumulative Explained Variance')
plt.title('Variation explained by PCA')

plt.show()

# convert the pca result into data frame
pca_players_train_df = pd.DataFrame(players_pca_train)
pca_players_test_df = pd.DataFrame(players_pca_test)

# rename the columns
pca_players_train_df.columns = [f'PCA{i+1}' for i in pca_players_train_df.columns]
pca_players_test_df.columns = [f'PCA{i+1}' for i in pca_players_test_df.columns]

# select the variables for building the mdel
players_train = pd.DataFrame(players_train[list(set(players_train.columns[:9]) | set(players_train.columns[-5:]))])
players_test = pd.DataFrame(players_test[list(set(players_test.columns[:9]) | set(players_test.columns[-5:]))])

# add the pca result columns into the data frame
for c in pca_players_train_df.columns:
    players_train[c] = list(pca_players_train_df[c])
for c in pca_players_test_df.columns:
    players_test[c] = list(pca_players_test_df[c])

# get the X variables
X_train = players_train.drop('playoff', axis = 1)
X_test = players_test.drop('playoff', axis = 1)

# get the y variables
y_train = players_train['playoff'].astype(np.int64)
y_test = players_test['playoff'].astype(np.int64)

from sklearn import tree
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier

import random
from sklearn.model_selection import cross_val_score
from sklearn.metrics import classification_report
from sklearn.ensemble import AdaBoostClassifier

# get default decision tree classifier
clf_dt = DecisionTreeClassifier().fit(X_train, y_train)
dt_cv_score = cross_val_score(clf_dt, X_test, y_test, cv=5)
dt_score = clf_dt.score(X_test, y_test)

# get default random forest classifier
np.random.seed(42)
clf_rf = RandomForestClassifier().fit(X_train, y_train)
rf_cv_score = cross_val_score(clf_rf, X_test, y_test, cv=5)
rf_score = clf_rf.score(X_test, y_test)

# get default gradient boosting classifier
gbc = GradientBoostingClassifier().fit(X_train, y_train)
gbc_cv_score = cross_val_score(gbc, X_test, y_test, cv=5)
gbc_score = gbc.score(X_test, y_test)

# get default ada boost classifier
ada = AdaBoostClassifier().fit(X_train, y_train)
ada_cv_score = cross_val_score(ada, X_test, y_test, cv=5)
ada_score = ada.score(X_test, y_test)

print('Decision Tree: ',dt_score, '\nRandomForest: ', rf_score, '\nGradientBoosting: ', gbc_score, '\nAdaBoost: ', ada_score)

import json
with open('/project/MSIN0166_Data_Engineering_individual/scores.json', "w") as fd:
    json.dump({"Decision_Tree_score": dt_score, 'Random_Forest_score':rf_score, 'Gradient_Boosting_score':gbc_score, 'Ada_Boost_score':ada_score}, fd, indent=4)

print(classification_report(y_test, ada.predict(X_test)))

from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.metrics import accuracy_score

# randomised search
params_rand ={
    'n_estimators' : np.random.randint(5,200,size = 1),
    'learning_rate': [0.01,0.02,0.03,0.04,0.05]
}

rand_ada = RandomizedSearchCV(AdaBoostClassifier(), 
                            params_rand, 
                            n_iter=20,
                            scoring='accuracy', 
                            n_jobs=1, 
                            cv=RepeatedStratifiedKFold(n_splits=5), 
                            random_state=1)

# train the search
final_ada = rand_ada.fit(X_train, y_train)

# show the best params
final_ada.best_params_

print(classification_report(y_test,final_ada.predict(X_test)))