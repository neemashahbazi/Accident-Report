import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession
from utils import clean_text

# Assumes dataframe format: time_field, text, hashtags, geo, coordinates

all_data = sys.argv[1]
model_path = sys.argv[2]
spark = SparkSession.builder.appName('tweet-classifier').getOrCreate()
input_rdd = spark.read.csv(all_data, header=False, inferSchema=True).rdd

cleaned_rdd = input_rdd.map(lambda x: clean_text(x, train=True))
cleaned_df = cleaned_rdd.toDF()
cleaned_df = cleaned_df.withColumnRenamed("_1", "raw_label").withColumnRenamed("_2", "processed_text").withColumnRenamed("_3", "raw_text")

tokenizer = Tokenizer(inputCol="processed_text", outputCol="tokens")
w2v = Word2Vec(vectorSize=300, minCount=0, inputCol="tokens", outputCol="features")
si = StringIndexer(inputCol="raw_label", outputCol="label")
rf_classifier = LogisticRegression(labelCol="label", featuresCol="features")
rf_classifier_pipeline = Pipeline(stages=[tokenizer, w2v, si, rf_classifier])
pipeline_fit = rf_classifier_pipeline.fit(cleaned_df)
pipeline_fit.save(model_path)

"""

stats
              precision    recall  f1-score   support                           
           0     0.9696    0.9851    0.9773      3558
           1     0.8645    0.7545    0.8057       448
           
    accuracy                         0.9593      4006
   macro avg     0.9170    0.8698    0.8915      4006
weighted avg     0.9578    0.9593    0.9581      4006

"""