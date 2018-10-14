This folder contains the source code for the application split by technology.

Clone this repo onto the master node of each cluster. Start the technologies in the following order.

### Kafka Cluster
1. cd into the correct folder: ./kafka
2. Make the topic for the kafka brokers to send messages to: bash maketopics.sh
3. Start the kafka producers: python spawn_kafka_stream.py

### Spark-ECG Cluster
1. cd into the correct folder: ./spark/ecg
2. Start the spark consumers: bash runspark.sh

### Spark-HR Cluster
1. cd into the correct folder: ./spark/hr
2. Start the spark consumers: bash runspark.sh

### Website
1. cd into the correct folder: ./dash
2. Start the dash application: sudo python app.py

### Database
1. Follow the instructions in ../setup/setup_db.txt 
2. Follow the instructions in ../setup/run_airflow.txt
