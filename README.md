# ECGdashboard

## Business Problem
Hospital collect and process a lot of signals and are now moving towards storing them. My project is to process, display and save ECG signals from patients thoughout a hospital. These ECG signals are processed every minute to analyze the beat-to-beat heart rate (HR) of each patient in the hospital. Alarms are set to warn nurses of patients with low or high HR and signal failure which is predominately caused by the electrodes losing contact with the skin.

These alarms are triggered very frequently leading to nursing alarm fatigue. To reduce the number of unnecessary alarms, the alarm theshold is dynamically changed within safe limits to reduce the number of alarms. If the alarm is still triggered when the threshold has been changed to it's maximum or minimum value, the nurse is called. Additionally, the alarms are routed to specific nurses instead of to everyone to reduce unnecessary alerts being sent to everyone.

## Solution Architecture
ECG Streams --> S3 bucket --> Kafta --> Spark Streaming --> TimeScaleDB --> Dash
* Data storage: Timescaledb
	* Stores time series data segments so it is easy for windowing and calculating HR for 1 minute segments.
	* Can store waveform, HR, alarm state in same database (build on PostgreSQL)
* Ingestion: Kafka
	* Pub/sub system with multiple producers/consumers
* Stream Processing: Spark streaming
	* Want to learn Scala
* Front end: Dash
	* Easy to build website with python

## Setting up AWS account 
peg up master.yml

peg up workers.yml

peg install <cluster-name> ssh

peg install <cluster-name> aws

peg install <cluster-name> environment