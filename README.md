# Create debs-topic
docker exec -it kafka kafka-topics --create --topic debs-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic debs-topic --from-beginning

./bin/spark-submit --master local --name write-debs --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./app/write_debs_data_24.py

./bin/spark-submit --master local --name debs-streaming --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./app/debs_data_streaming_24.py