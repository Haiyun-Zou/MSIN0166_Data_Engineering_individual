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
    players[c] = list(dummies_df[c])
    
# convert from True/False to 1/0
players['in_2021_22_season'] = players['in_2021_22_season'].replace({True:1, False:0})

players = players.sort_values('name').reset_index(drop=True)

# drop the columns that are not used for machine learning
players = players.drop([ 'team', 'name', 'link', 'pos', 'No.'], axis = 1)

import yaml
# load the yaml document
params = yaml.safe_load(open("/project/MSIN0166_Data_Engineering_individual/params.yaml"))["ML"]

print(params)

from sklearn.model_selection import train_test_split
# split the train and test sets
players_train, players_test = train_test_split(players, test_size = params['split'], shuffle = params['shuffle'], random_state = params['seed'])

# create spark df
players_train  = spark.createDataFrame(players_train)
players_test  = spark.createDataFrame(players_test)


# select columns for pca
players_train_trans = players_train.select(players_train.columns[9:-5])
players_test_trans = players_test.select(players_test.columns[9:-5])

print(players_train_trans.toPandas().shape)

from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# assemble the training data first
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in players_train_trans.columns]
# standerdise the data
scaler = [StandardScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in players_train_trans.columns]
pipeline = Pipeline(stages= assemblers + scaler)
X_train = pipeline.fit(players_train_trans)
X_train = X_train.transform(players_train_trans)


# assemble the testing data
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in players_test_trans.columns]
# standerdise the data
scaler = [StandardScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in players_test_trans.columns]
pipeline = Pipeline(stages= assemblers + scaler)
X_test = pipeline.fit(players_test_trans)
X_test = X_test.transform(players_test_trans)

# get the scaled columns
columns = [i for i in X_train.columns if 'scaled' in i]

from pyspark.ml.feature import PCA
# assemble the columns into features column
assemblers = VectorAssembler(inputCols= ['G_2122_scaled',
 'GS_2122_scaled',
 'MP_2122_scaled',
 'FG%_2122_scaled',
 '3P%_2122_scaled',
 '2P%_2122_scaled',
 'eFG%_2122_scaled',
 'FT%_2122_scaled',
 'ORB_2122_scaled',
 'DRB_2122_scaled',
 'AST_2122_scaled',
 'STL_2122_scaled',
 'BLK_2122_scaled',
 'TOV_2122_scaled',
 'PF_2122_scaled',
 'PTS_2122_scaled',
 'G_career_scaled',
 'GS_career_scaled',
 'MP_career_scaled',
 'FG%_career_scaled',
 '3P%_career_scaled',
 '2P%_career_scaled',
 'eFG%_career_scaled',
 'FT%_career_scaled',
 'ORB_career_scaled',
 'DRB_career_scaled',
 'AST_career_scaled',
 'STL_career_scaled',
 'BLK_career_scaled',
 'TOV_career_scaled',
 'PF_career_scaled',
 'PTS_career_scaled'], outputCol="features")

# transform the training data and return the PCA result in the column of PCA_features
X_train_v = assemblers.transform(X_train)
PCA_train = PCA(k = params['n_components'] , inputCol="features", outputCol = 'PCA_features')

X_train_model = PCA_train.fit(X_train_v)
X_train = X_train_model.transform(X_train_v)

# transform the testing data and return the PCA result in the column of PCA_features
X_test_v = assemblers.transform(X_test)
PCA_test = PCA(k = params['n_components'] , inputCol="features", outputCol = 'PCA_features')

X_test_model = PCA_test.fit(X_test_v)
X_test = X_test_model.transform(X_test_v)

print(X_train.toPandas().shape)

print(sum(X_train_model.explainedVariance))

# plot the PCA variation
plt.figure(figsize = (8,5))
plt.plot(np.cumsum(X_train_model.explainedVariance),linestyle='solid', marker='o')
plt.xlabel('Number of Components')
plt.ylabel('Cumulative Explained Variance')
plt.title('Variation explained by PCA')

plt.show()

# get the nonPCA columns
players_train_first = players_train.select(list(set(players_train.columns[:9]) | set(players_train.columns[-5:])))
players_test_first = players_test.select(list(set(players_test.columns[:9]) | set(players_test.columns[-5:])))

# convert the PCA result column into data frame
temp_train = X_train.select('PCA_features').rdd.map(lambda x: [float(i) for i in x['PCA_features']]).toDF(['PCA' + str(i+1) for i in range(params['n_components'])])
temp_test = X_test.select('PCA_features').rdd.map(lambda x: [float(i) for i in x['PCA_features']]).toDF(['PCA' + str(i+1) for i in range(params['n_components'])])])

# convert back to pandas
players_train_first_pd = players_train_first.toPandas()
players_test_first_pd = players_test_first.toPandas()
temp_train_pd = temp_train.toPandas()
temp_test_pd = temp_test.toPandas()

# merge the columns together
for c in temp_train_pd.columns:
    players_train_first_pd[c] = temp_train_pd[c]
for c in temp_test_pd.columns:
    players_test_first_pd[c] = temp_test_pd[c]
    
# convert back to spark df
players_train_final = spark.createDataFrame(players_train_first_pd)
players_test_final = spark.createDataFrame(players_test_first_pd)

from pyspark.ml.classification import DecisionTreeClassifier

# assemble all the X columns into features
assembler = VectorAssembler(inputCols =['is_PG',
 'is_SF',
 'is_SG',
 'is_eastern',
 'height',
 'guaranteed',
 'is_PF',
 'age',
 'birth',
 'is_C',
 'weight',
 'in_2021_22_season',
 'exp',
 'PCA1',
 'PCA2',
 'PCA3',
 'PCA4',
 'PCA5',
 'PCA6',
 'PCA7',
 'PCA8',
 'PCA9',
 'PCA10'] , outputCol='features')

# transform the train and test data
players_train_final = assembler.transform(players_train_final)
players_test_final= assembler.transform(players_test_final)

# rename the column playoff to label
players_train_final = players_train_final.withColumnRenamed('playoff', 'label')
players_test_final= players_test_final.withColumnRenamed('playoff', 'label')

print(players_train_final.toPandas().shape)

# build the model and make the prediction
clf_dt= DecisionTreeClassifier(featuresCol="features", labelCol="label")
clf_dt = clf_dt.fit(players_train_final)
pred_clf_dt = clf_dt.transform(players_test_final)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# evaluate the model
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
accuracy = evaluator.evaluate(pred_clf_dt)
print(accuracy)

from sklearn.metrics import confusion_matrix

y_p=pred_clf_dt.select("prediction").collect()
y =pred_clf_dt.select("label").collect()

cm = confusion_matrix(y, y_p)
tn, fp, fn, tp = cm.ravel()
print("Confusion Matrix:")
print(cm)

acc = (tn+tp)/(tp+tn+fp+fn)
precision = tp/(tp+fp)
recall = tp/(tp+fn)

import json
# write the metrics into the scores.json file
with open('/project/MSIN0166_Data_Engineering_individual/scores.json', "w") as fd:
    json.dump({"Accuract":acc , 'Precision':precision, 'Recall': recall, 'F1_Score':2*((precision * recall)/(precision + recall))}, fd, indent=4)