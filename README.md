# ECGdashboard
Hello and welcome to the repository for my Data Engineering project during my fellowship at Insight Data Science in New York 2018.
## Business Problem
My project aims to: 
1. Reduce staffing costs by providing a centralized monitoring dashboard to view multiple patient's ECG signals at once.
2. Index and store raw physiological signals for downstream analysis. 

### Description
Hospital collect and process a lot of signals and are now moving towards storing them. 
My project is to process and display ECG signals in near real time from patients. 
These ECG signals are processed every minute to analyze the beat-to-beat heart rate (HR) of each patient in the hospital. 
These signals are also indexed and stored for use as inputs when developing machine learning models to predict adverse physiological events to improve patient care
<p align="center">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/ecgsignals.jpg" width="600", height="400">
</p>

## Solution Architecture
<p align="center">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/pipeline.png" width="700", height="400">
</p>

ECGdashboard runs a pipeline on the AWS cloud, using the following cluster configurations:
* four m4.large EC2 instances for Kafka producers
* four m4.large EC2 instances for Spark Streaming (2s mini-batch) which writes to postgres database
* four m4.large EC2 instances for Spark Streaming (60s mini-batch) which calculates heart rate over 60s period
* one t2.micro RDS instance for PostgreSQL database
* one t2.micro EC2 instance for Dash app

### Design Choices
* Data storage: PostgreSQL
	* Easy to change over to timescaleDB once implemented
	* Can store waveform, HR, alarm state in same database (build on PostgreSQL)
* Ingestion: Kafka
	* Pub/sub system with multiple producers/consumers
* Stream Processing: Spark streaming
	* Micro-batching to output the data streams to database
* Front end: Dash
	* Easy to build website with python

## Setting up AWS clusters
### Requirements 
* [peg](https://github.com/InsightDataScience/pegasus)
* [confluent-kafka](https://docs.confluent.io/current/installation/installing_cp/zip-tar.html#prod-kafka-cli-install)

### Setup
* To setup clusters run setup/setup_cluster.sh
* To setup database follow setup/db_setup.txt. Once setup, build tables using sql commands in setup/tables.sql
