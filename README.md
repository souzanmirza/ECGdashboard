# ECGdashboard

## Business Problem
Hospital collect and process a lot of signals and are now moving towards storing them. My project is to process, display and save ECG signals from patients thoughout a hospital. These ECG signals are processed every minute to analyze the beat-to-beat heart rate (HR) of each patient in the hospital. Alarms are set to warn nurses of patients with low or high HR and signal failure which is predominately caused by the electrodes losing contact with the skin.

These alarms are triggered very frequently leading to nursing alarm fatigue. To reduce the number of unnecessary alarms, the alarm theshold is dynamically changed within safe limits to reduce the number of alarms. If the alarm is still triggered when the threshold has been changed to it's maximum or minimum value, the nurse is called. Additionally, the alarms are routed to specific nurses instead of to everyone to reduce unnecessary alerts being sent to everyone.

## Solution Architecture
							     ^ Spark
							     V 
ECG Streams --> S3 bucket --> Kafta --> Spark Streaming --> PostgreSQL --> Dash
						             

ECGdashboard runs a pipeline on the AWS cloud, using the following cluster configurations:
* four m4.large EC2 instances for Kafka (only master has producer threads at this moment)
* four m4.large EC2 instances for Spark Streaming (ingesting from Kafka master node)


* Data storage: PostgreSQL
	* Easy to change over to timescaleDB once implemented
	* Can store waveform, HR, alarm state in same database (build on PostgreSQL)
* Ingestion: Kafka
	* Pub/sub system with multiple producers/consumers
* Stream Processing: Spark streaming
	* Micro-batching to output the data streams to database
* Front end: Dash
	* Easy to build website with python

## Setting up AWS account
Requirements: 
* [peg](https://github.com/InsightDataScience/pegasus)
* [confluent-kafka](https://docs.confluent.io/current/installation/installing_cp/zip-tar.html#prod-kafka-cli-install)

Setup
* To setup clusters run setup/setup_cluster.sh
* To setup postgresql follow postgresql rds instructions available from AWS. Note: Make sure 2 subnets in different availability sub-zones, make database publically accessible (so can access from outside the VPC). To setup tables run sql commands in setup/db_setup.txt
