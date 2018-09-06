#!/bin/bash
 
for((i=1;i<=100;i++));
do 
./bin/alluxio fs ls /
sleep 5s
done

