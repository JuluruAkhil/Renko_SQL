#!/usr/bin/env bash

PID=`cat /home/ubuntu/Renko_SQL/sql_test.pid`

if ! ps -p $PID > /dev/null
then
  rm /home/ubuntu/Renko_SQL/sql_test.pid
  Python3 sql_test.py & echo $! >>/home/ubuntu/Renko_SQL/sql_test.pid
fi