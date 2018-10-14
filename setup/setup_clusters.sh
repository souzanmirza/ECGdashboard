peg up yml/kafkamaster.yml &
peg up yml/kafkaworkers.yml &

wait

peg fetch kafka-cluster

peg install kafka-cluster ssh
peg install kafka-cluster aws
peg install kafka-cluster environment
peg sshcmd-cluster kafka-cluster "sudo apt-get install bc"

peg install kafka-cluster zookeeper
peg service kafka-cluster zookeeper start

peg install kafka-cluster kafka
peg service kafka-cluster kafka start

############################################################

spark-ecg-cluster

peg up yml/sparkecgmaster.yml &
peg up yml/sparkecgworkers.yml &

wait

peg fetch spark-ecg-cluster

peg install spark-ecg-cluster ssh
peg install spark-ecg-cluster aws
peg install spark-ecg-cluster environment
peg sshcmd-cluster spark-ecg-cluster "sudo apt-get install bc"

peg install spark-ecg-cluster hadoop
peg service spark-ecg-cluster hadoop start

peg install spark-ecg-cluster spark
#In /usr/local/spark/conf/spark-env.sh
#Add export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop

peg service spark-ecg-cluster spark start

peg sshcmd-cluster spark-ecg-cluster "pip install psycopg2 pyspark boto3"

############################################################

spark-hr-cluster

peg up yml/sparkhrmaster.yml &
peg up yml/sparkhrworkers.yml &

wait

peg fetch spark-hr-cluster

peg install spark-hr-cluster ssh
peg install spark-hr-cluster aws
peg install spark-hr-cluster environment
peg sshcmd-cluster spark-hr-cluster "sudo apt-get install bc"


peg install spark-hr-cluster hadoop
peg service spark-hr-cluster hadoop start

peg install spark-hr-cluster spark
#In /usr/local/spark/conf/spark-env.sh
#Add export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop

peg service spark-hr-cluster spark start

peg sshcmd-cluster spark-hr-cluster "pip install psycopg2 pyspark boto3 bidict h5py matplotlib numpy scikit-learn scipy shortuuid six biosppy"

#############################################################

peg up yml/website.yml &

wait

peg fetch website

peg install website ssh
peg install website aws
peg install website environment

peg sshcmd-cluster website "pip install psycopg2 numpy pandas dash==0.28.1 dash-html-components==0.13.2 dash-core-components==0.30.2"

#############################################################

peg up yml/database.yml &

wait

peg fetch database

peg install database ssh
peg install database aws
peg install database environment

peg sshcmd-cluster database "pip install psycopg2 boto3 airflow pandas"

