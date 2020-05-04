#!/usr/bin/env bash

PID=`cat /home/ubuntu/Renko_SQL/SQL_Store.pid`

if ! ps -p $PID > /dev/null
then
  rm /home/ubuntu/Renko_SQL/SQL_Store.pid
  Python3 SQL_Store.py & echo $! >>/home/ubuntu/Renko_SQL/SQL_Store.pid
fi