bash install_spark.sh
pip install dvc

dvc exp run -f team
dvc exp run -f players
dvc exp run -f ML
dvc exp run -f data_transformation
dvc exp run -f write_into_database

dvc metrics show

dvc exp show