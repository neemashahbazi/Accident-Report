from pyspark.ml import PipelineModel
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from kafka import KafkaConsumer
import sys
from utils import clean_text, construct_parse_tree
import spacy

pipeline = sys.argv[3]
conf = SparkConf().setMaster("local[3]").setAppName("TweetClassifier")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
nlp = spacy.load("en_core_web_sm")

schema = StructType([
    StructField("processed_text", StringType(), True),
    StructField("username", StringType(), True),
    StructField("location", StringType(), True),
    StructField("geo_coordinates", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("label", StringType(), True),
    StructField("raw_text", StringType(), True)
])

topic_name = 'tweeter_feed'
model = PipelineModel.load(pipeline)
consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'])
for i, msg in enumerate(consumer):
    information = msg.value.decode("utf-8").split(" <-> ")
    processed, raw = clean_text(information[0])
    information[0] = processed
    information.append(raw)
    df = spark.createDataFrame(Row(information), schema)
    df = model.transform(df)
    if df.filter(df.prediction == 1.0) is not None:
        # YOUR CODE GOES HERE
