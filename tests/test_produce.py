#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis
import time
import getopt
import sys

'''
test script for produce to redis list
'''

redis_host = "127.0.0.1"
redis_port = 6379
redis_list = ''
push_count = 1
env = 'dev'
interval = 1

opts, args = getopt.getopt(sys.argv[1:], '-h:-p:-l:-c:-e:-i:', ['host=', 'port=', 'list=', 'count=', 'env=', 'interval='])
for opt_name, opt_value in opts:

    if opt_name in ('-h', '--host'):
        print("[*] redis host is ", opt_value)
        redis_host = opt_value
    if opt_name in ('-p', '--port'):
        redis_port = opt_value
        print("[*] redis port is ", opt_value)
    if opt_name in ('-l', '--list'):
        redis_lists = opt_value
        print("[*] redis list is ", opt_value)
    if opt_name in ('-c', '--count'):
        push_count = opt_value
        print("[*] push count is ", opt_value)
    if opt_name in ('-e', '--env'):
        env = opt_value
        print("[*] env is ", opt_value)
    if opt_name in ('-i', '--interval'):
        interval = opt_value
        print("[*] interval is ", opt_value)

r = redis.Redis(redis_host, redis_port)
i = 0
lists = redis_lists.split(",")

docEntryDev = 'asdasd'

docEntryProd = 'asdasd'

while i < int(push_count):
    for item in lists:
        if env == 'dev':
            r.rpush(item, docEntryDev)
        elif env == 'prod':
            r.rpush(item, docEntryProd)
        print "printed " + str(i) + " item"
    i = i + 1
    time.sleep(float(interval))

