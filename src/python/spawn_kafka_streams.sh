#!/bin/bash
BUCKET=$1
RECORDLIST=$2
IP_ADDR=$3
SESSION=$4

aws s3 cp s3://$BUCKET/$RECORDLIST $RECORDLIST
readarray recordsarray < $RECORDLIST
rlen=${#recordsarray[@]}

tmux new-session -s $SESSION -n bash -d
for (( i=0; i<10; i++));
do
    echo ${recordsarray[$i]}
    tmux new-window -t $((i+1))
    tmux send-keys -t $SESSION:$((i+1)) 'python kafka_producer.py '"$IP_ADDR"' '"${recordsarray[$i]}"'' C-m
done