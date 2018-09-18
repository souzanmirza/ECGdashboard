CLUSTER_NAME=kafka-cluster

peg up kafkamaster.yml &
peg up kafkaworkers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment

peg install ${CLUSTER_NAME} zookeeper
peg service ${CLUSTER_NAME} zookeeper start

peg install ${CLUSTER_NAME} kafka
peg service ${CLUSTER_NAME} kafka start