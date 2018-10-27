#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import getopt
import sys

'''
consume test script 
'''

servers = '127.0.0.1:9092'
topics = 'test'

opts, args = getopt.getopt(sys.argv[1:], '-s:-t:', ['servers=', 'topics='])
print(opts)
for opt_name, opt_value in opts:
    if opt_name in ('-s', '--servers'):
        servers = opt_value
    if opt_name in ('-t', '--topics'):
        topics = opt_value

# 消费kafka中最新的数据 并且自动提交offsets[消息的偏移量]
topics = topics.split(",")
try:
    consumer = KafkaConsumer(auto_offset_reset='earliest',
                             bootstrap_servers=servers, consumer_timeout_ms=1000)
    consumer.subscribe(topics)
    for message in consumer:
        print message.value
except Exception as e:
    print e.message
