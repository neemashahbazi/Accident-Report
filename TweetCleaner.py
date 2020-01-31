import os
import shutil
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

conf = SparkConf().setAppName("tweet cleaner").setMaster("local[4]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
file_directory = sys.argv[1]
cleaned_csv_file = sys.argv[2]
tmp_csv_file = sys.argv[3]

all_data = None

for i, file in enumerate(os.listdir(file_directory)):
    file_path = os.path.join(file_directory, file)
    lines = sc.textFile(file_path).map(lambda line: line.split(";")).toDF(["tweet_id", "tweet_text", "tweet_label"])
    lines = lines.withColumn("tweet_label", F.when(F.col("tweet_label") != "crash", "no").otherwise(F.col("tweet_label")))
    if i == 0:
        all_data = lines
    else:
        all_data = all_data.union(lines)

all_data.coalesce(1).write.csv(tmp_csv_file)
for file in os.listdir(tmp_csv_file):
    file_path = os.path.join(tmp_csv_file, file)
    if file_path.endswith('csv'):
        os.rename(file_path, cleaned_csv_file)

shutil.rmtree(tmp_csv_file)
