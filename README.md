# Backtesting Engine

We'll be creating a Backtest Replay Engine.

This project creates a stream of historical orders with specified throttling, simulating a realtime market data feed, implemented with Kafka and Spark Structured Streaming.

This will all be displayed with a Streamlit UI.


## Building from scratch:

Update/install packages

```
sudo apt update
sudo apt install -y openjdk-11-jdk python3 python3-venv python3-pip wget tar
```

Start Zookeeper and Kafka (each in its own terminal)

```
cd kafka && bin/zookeeper-server-start.sh config/zookeeper.properties
cd kafka && bin/kafka-server-start.sh config/server.properties
```

Create a new Kafka topic
```
cd kafka
bin/kafka-topics.sh --create --topic ohlcv --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Create and activate venv
```
cd ~
python3 -m venv spark-kafka-env
source spark-kafka-env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Run the Flask UI (which will automatically start up the consumer, and invoke the producer when details are submitted)
```
python app2.py
```

Deleting a topic (for testing)
```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <topic_name>
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Stopping ZooKeeper
```
bin/zookeeper-server-stop.sh
```
