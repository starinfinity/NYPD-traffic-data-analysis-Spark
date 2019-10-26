# Data-Analysis-Spark-
NYPD traffic Data analyses using Spark 


Files:
cleaner_spark.py- spark file to clean the data and process it using spark


Instructions:
Step 1 : run the cleaner spark file to clean the file and process the data


#this will clean the data present at /path/to/data/ and save it as cleaninputfile.csv
#this will also process that data in spark 
COMMAND USED:PYTHONSTARTUP=cleaner_spark.py pyspark


to view clean data 
COMMAND: hadoop fs -cat /user/6+2/cleaninputfile.csv

