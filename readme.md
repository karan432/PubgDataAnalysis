## Dataset
https://www.kaggle.com/skihikingkevin/pubg-match-deaths#aggregate.zip

##start pyspark
pyspark --master local[*]

##start consumer
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.3.jar --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 consumer.py

##start producer
python producer.py

####### kafka ########

###start zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
###start producer
.\bin\windows\kafka-server-start.bat .\config\server.properties


.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

###consumer for testing
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic javainuse-topic --from-beginning

###satart producer
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test

###put file in queue
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test < trimmed_hd.csv