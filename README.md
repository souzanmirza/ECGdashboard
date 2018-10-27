# ECGdashboard
Hello and welcome to the repository for my Data Engineering project during my fellowship at Insight Data Science in New York 2018.

## Table of Contents
1. [Business Problem](README.md#business-problem)
2. [Solution](README.md#solution)
3. [Installation](README.md#installation)
4. [Links](README.md#links)

## Business Problem
### Aims
1. Reduce staffing costs by providing a centralized monitoring dashboard to view multiple patient's ECG signals at once.
2. Index and store raw physiological signals for downstream analysis. 

### Description
Hospitals collect and process a lot of signals and are now moving towards storing them. My project aims to process and display ECG signals in near real time from patients. These ECG signals are processed every minute to analyze the beat-to-beat heart rate (HR) of each patient in the hospital. These signals are also indexed and stored for use as inputs when developing machine learning models to predict adverse physiological events to improve patient care.
<p align="center">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/ecgsignals.jpg" width="600", height="400">
</p>

## Solution

My pipeline loads in ECG timeseries data from an S3 bucket which contains separate files for each patient. The files are ingested line by line simulating sampling of the ECG signals by my kafka brokers. The brokers produce messages into a topic which is subscribed to by two spark streaming clusters. The first spark streaming cluster has a 2-second mini-batch interval which groups the signals from each patient and saves them to my PostgreSQL database. The second cluster has a 60-second mini-batch interval to measure the number of beats over the batch period to calculate heart rate per minute over time and save them to my database. Botht the time series ECG samples and the calculated heart rate's are displayed on my dash front end.

### Front-End Views
<p align="center">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/dashboard_ecg_v3.PNG">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/dashboard_hr_v2.PNG">
</p>


### Architecture
<p align="center">
<img src="https://github.com/souzanmirza/ECGdashboard/blob/master/docs/pipeline.png" width="700", height="400">
</p>

## Installation
### Requirements 
* [peg](https://github.com/InsightDataScience/pegasus)

### Setup
* To setup clusters run setup/setup_cluster.sh.
* To setup database follow setup/db_setup.txt.
* To setup the airflow job follow setup/run_airflow.txt
* To start the pipeline follow the instructions in src/README.md 

### Testing
* Unittests can be run using the run_unittests.sh file in the ./test folder. Results are stored in ./test/results.txt.

### Cluster Configuration
* four m4.large EC2 instances for Kafka producers
* four m4.large EC2 instances for Spark Streaming (2s mini-batch) which writes to postgres database
* four m4.large EC2 instances for Spark Streaming (60s mini-batch) which calculates heart rate over 60s period
* one m4.large EC2 instance for TimescaleDB/PostgreSQL database
* one t2.micro EC2 instance for Dash app

## Links
* [ECGdashboard](http://ecgdashboard.life)
* [Slideshow](https://www.slideshare.net/SouzanMirza/ecgdashboard-insight-fellowship-project-120943085)
* [Demo Video](https://youtu.be/801_PMN0ag0)
