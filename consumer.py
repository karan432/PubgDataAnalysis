from pyspark.sql import SQLContext, Row, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

row_header = ["date",\
"game_size",\
"match_id",\
"match_mode",\
"party_size",\
"player_assists",\
"player_dbno",\
"player_dist_ride",\
"player_dist_walk",\
"player_dmg",\
"player_kills",\
"player_name",\
"player_survive_time",\
"team_id",\
"team_placement",\
"dateonly",\
"year",\
"month",\
"day"]

conf = SparkConf().setAppName("streamingPubg")
spark = SparkSession \
        .builder\
        .appName("streamingPubg") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/pubg") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/pubg") \
        .getOrCreate()
sc = spark.sparkContext

sql_context = SQLContext(sc)

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def toRow(record):
    return record.split(",") # Row()

def getKills(data):
    kills = data[0]
    knockdowns = data[1]
    return  kills-knockdowns if kills > knockdowns else 0

def getKnockdowns(data):
    kills = data[0]
    knockdowns = data[1]
    return knockdowns - kills if kills < knockdowns else 0

def getFullKills(data):
    kills = data[0]
    knockdowns = data[1]
    return knockdowns if kills > knockdowns else kills
    
def getRating(data):
    kills_only = data[0]
    knockdowns_only = data[1]
    full_kills = data[2]
    rel_dmg = data[3]
    rel_placement = data[4]
    rating = kills_only + (1.5 * knockdowns_only) + (3 * full_kills) + (3.5 * rel_dmg) + rel_placement
    return rating

rating_udf = udf(getRating, DoubleType())
kills_only_udf = udf(getKills, IntegerType())
knockdowns_only_udf = udf(getKnockdowns, IntegerType())
full_kills_udf = udf(getFullKills, IntegerType())

def write_to_mongo(df):
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database","pubg")\
        .option("collection", "players")\
        .save()

def calculate_rating(rdd):
    # cast columns to required type
    rdd.dropna(how = 'any')
    rdd = rdd.withColumn("game_size", rdd["game_size"].cast(IntegerType()))
    rdd = rdd.withColumn("party_size", rdd["party_size"].cast(IntegerType()))
    rdd = rdd.withColumn("player_assists", rdd["player_assists"].cast(IntegerType()))
    rdd = rdd.withColumn("player_dmg", rdd["player_dmg"].cast(IntegerType()))
    rdd = rdd.withColumn("player_kills", rdd["player_kills"].cast(IntegerType()))
    rdd = rdd.withColumn("player_dbno", rdd["player_dbno"].cast(IntegerType()))
    rdd = rdd.withColumn("team_placement", rdd["team_placement"].cast(IntegerType()))

    #calculate additional fields to calculate rating
    rdd = rdd.withColumn("total_players", rdd.game_size*rdd.party_size) #rdd["total_players"].cast(IntegerType())
    rdd = rdd.withColumn("kills_only", kills_only_udf(array("player_kills","player_dbno")))
    rdd = rdd.withColumn("knockdowns_only", knockdowns_only_udf(array("player_kills","player_dbno")))
    rdd = rdd.withColumn("full_kills", full_kills_udf(array("player_kills","player_dbno")))
    #calculate aggregate for each player
    agg = rdd.agg(avg(col("player_kills")),avg(col("player_dbno")),avg(col("player_dmg"))).collect()[0]
    avg_kills = agg["avg(player_kills)"]
    avg_kd = agg["avg(player_dbno)"]
    avg_kills_kd = (avg_kills + avg_kd) / 2
    avg_damage = agg["avg(player_dmg)"]
    rdd = rdd.withColumn("kills_only", rdd.kills_only/avg_kills)
    rdd = rdd.withColumn("knockdowns_only", rdd.knockdowns_only/avg_kd)
    rdd = rdd.withColumn("full_kills", rdd.full_kills/avg_kills_kd)
    rdd = rdd.withColumn("rel_dmg", rdd.player_dmg/avg_damage)
    rdd = rdd.withColumn("rel_placement", 1/rdd.team_placement)
    rdd = rdd.withColumn("rating", rating_udf(array("kills_only","knockdowns_only","full_kills","rel_dmg","rel_placement")))
    return rdd

def getMatchIds(df):
	matches = df.groupBy("match_id").count()
	ids = matches.select("match_id").collect()
	ids = [str(Id.match_id) for Id in ids]
	return ids

def process(rdd):
    try:
        if(not rdd.isEmpty()):
            rdd_row = rdd.map(toRow)
            rdd_row = rdd_row.toDF(row_header)
            ids = getMatchIds(rdd_row)
            for Id in ids:
                match_df = rdd_row.filter(rdd_row.match_id == Id)
                rating_rdd = calculate_rating(match_df)
                write_to_mongo(rating_rdd)
            print("************************************************************************")
            print("---------------------written to mongo-------------------")
            print("************************************************************************")
    except:
        print("error calculating rating......----------------********-------------------")

def sendRecord(record):
    print("************************************************************************")
    print("type: ", type(record))
    print(record)
    print("************************************************************************")

if __name__ == "__main__":
    stream = StreamingContext(sc, 10) # 10 second window

    print('================= ssc created ===================')

    kafka_stream = KafkaUtils.createStream(stream, \
                                        "localhost:2181", \
                                        "pubg_consumer",\
                                            {"test":1})
    matches = kafka_stream.map(lambda x: x[1])
    matches.foreachRDD(lambda match: process(match)) 
    stream.start()
    stream.awaitTermination()
