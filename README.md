# Kafka producer for DEBS Grand Challenge 2024
## Table of contents
- [Kafka producer for DEBS Grand Challenge 2024](#kafka-producer-for-debs-grand-challenge-2024)
  - [Table of contents](#table-of-contents)
- [Prequisite](#prequisite)
- [How to run](#how-to-run)

# Prequisite
- [Docker](https://www.docker.com/)
- [Python](https://www.python.org/) >= 3.6
- [Pip](https://pypi.org/project/pip/) >= 20.0.2

# How to run
1. Clone the repository
```bash
git clone https://github.com/5IF-Cloud/kafka-streaming.git
```
2. Install the required packages
```bash
pip install -r requirements.txt
```
3. Run the docker-compose file
```bash
docker compose up -d
```
4. Create `debs-topic` in kafka
```bash
docker exec -it kafka kafka-topics --create --topic debs-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
5. Launch the producer
```bash
python3 write_debs_data_24.py
```

If you want to see the data in the topic, you can use the following command:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic debs-topic --from-beginning
```