#!/usr/bin/env python
# -*- coding: utf-8 -*-
import signal
import sys
import time
import logging
import threading
import traceback
import yaml
from kafka.errors import KafkaTimeoutError, NotLeaderForPartitionError
from redis import ConnectionError
from model import kfk_producer, redis_consumer, redis_monitor, noticer, global_vars as gl

gl._init()
gl.set_value('NEED_KILL', False)
gl.set_value('IS_DEBUG', False)
gl.set_value('TASK_NUM', 0)

logging.basicConfig(
    level=logging.ERROR,
    filename='/data1/ms/log/redis2kafka/redis2kfk-%s.log' % time.strftime("%Y-%m-%d"),
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(process)d %(threadName)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
)


def do_exit():
    gl.set_value('NEED_KILL', True)


def bind_signal():
    signal.signal(signal.SIGINT, do_exit)
    signal.signal(signal.SIGTERM, do_exit)
    signal.signal(signal.SIGHUP, do_exit)
    signal.signal(signal.SIGQUIT, do_exit)


def main():
    try:
        with open(".env") as env:
            env_context = env.readline()
            env_debug = env.readline()
    except Exception as e:
        print "failed to read env"
        logging.error(e)
        exit()

    mode = env_context.split("=")
    mode[1] = mode[1].strip('\n\r')  # remove special chars
    if "dev" not in mode and "prod" not in mode:
        print "env:mode is not valid"
        exit()

    debug = env_debug.split("=")
    debug = debug[1].strip("\n\r")
    if int(debug) not in [1, 0]:
        print "env:debug is not valid"
    if debug == "1":
        gl.set_value('IS_DEBUG', True)

    mode = mode[1]
    print "Curr Mode is " + mode
    try:
        with open("./conf/" + mode + ".yml") as conf:
            conf_context = conf.read()
    except Exception as e:
        print "failed to read conf"
        logging.error(e)

    conf_map = yaml.load(conf_context)

    params = conf_map['settings']

    threadRate = params['ThreadsNumberPerTask']

    conf_map = conf_map['kafka']

    redis_map = []

    for service in conf_map:
        for act in conf_map[service]['actionTopics']:
            redis_task = {}
            redis_task['service'] = service
            redis_task['act'] = act
            redis_task['dsn'] = conf_map[service]['actionTopics'][act]['redis']['dsn']
            redis_task['redis_host'] = redis_task['dsn'].split(":")[0]
            redis_task['redis_port'] = redis_task['dsn'].split(":")[1]
            redis_task['redis_key'] = conf_map[service]['actionTopics'][act]['redis']['queue_key']
            redis_task['servers'] = conf_map[service]['servers']
            redis_task['topic'] = conf_map[service]['actionTopics'][act]['topic']
            redis_task['kfk_username'] = conf_map[service]['actionTopics'][act]['username']
            redis_task['kfk_password'] = conf_map[service]['actionTopics'][act]['password']
            redis_task['api_version'] = conf_map[service]['actionTopics'][act]['apiVersion']
            redis_map.append(redis_task)

    if gl.get_value('IS_DEBUG', False):
        print redis_map

    gl.set_value('TASK_NUM', len(redis_map))
    # start multi task
    for item in redis_map:
        # start consumer
        ThreadsNumberPerTask = 0
        while ThreadsNumberPerTask < threadRate:
            _thread = threading.Thread(target=multi_work, args=(
                item['servers'], item['topic'], item['redis_host'], item['redis_port'], item['redis_key'], params,
                item['kfk_username'],
                item['kfk_password'], item['api_version']))
            _thread.setDaemon(True)
            _thread.start()
            ThreadsNumberPerTask = ThreadsNumberPerTask + 1

        # start monitor
        _thread_monitor = threading.Thread(target=redis_list_monitor,
                                           args=(item['redis_host'], item['redis_port'], item['redis_key'],
                                                 params))
        _thread_monitor.setDaemon(True)
        _thread_monitor.start()

    while True:
        if gl.get_value('NEED_KILL', False) is True:
            print "you kill this process"
            exit(0)
        else:
            time.sleep(0.5)


def redis_list_monitor(redis_host, redis_port, redis_key, params):
    while True:
        rm = redis_monitor.RedisMonitor(redis_host, redis_port, redis_key)
        if rm.list_length() > int(params['maxRedisList']):
            is_list_long = True
            noticer.Noticer.send_wehcat("redis2kafka", "RedisListTooLong", params['mailTo'],
                                        "RedisListTooLong :" + redis_host + "::" + redis_port + " " + " :: " + redis_key + "\n Length is  " + str(rm.list_length()))
            if not gl.get_value('IS_DEBUG', False):
                noticer.Noticer.send_email("redis2kafka", "redis2Kafka_Warning", params['mailTo'],
                                           "redisList too Long :" + redis_host + "::" + redis_port + " " + redis_key + " curr length: " + str(
                                               rm.list_length()))
        else:
            is_list_long = False
        if gl.get_value('NEED_KILL', False) is True:
            return
        if gl.get_value('IS_DEBUG', False):
            print "redisList :" + redis_host + "::" + redis_port + " " + redis_key + " curr length: " + str(
                rm.list_length())
            time.sleep(params['debugListLenSleep'])
        elif is_list_long:
            time.sleep(params['listLongSleep'])
        else:
            time.sleep(params['listSleep'])


def multi_work(kfk_servers, kfk_topic, redis_host, redis_port, redis_key, params, username=None, password=None,
               api_version=None):
    print "Thread" + "  " + redis_key + "\n"
    servers = ",".join(kfk_servers)
    while True:
        try:
            if gl.get_value('NEED_KILL', False):
                print "Thread gracefully quit thread"
                return
            producer = kfk_producer.KfkProducer(servers, username, password, api_version, int(params['maxBlock']),
                                                int(params['metaDataMaxAge']), int(params['requestTimeOut']),
                                                int(params['kafkaTimeOut']))
            redis = redis_consumer.RedisClient(redis_host, redis_port, int(params['redisPopTimeOut']))
            while True:
                item = redis.load_data(redis_key)
                if item is not None:
                    producer.send_data(item[1], kfk_topic)
                if gl.get_value('NEED_KILL', False):
                    print "Thread gracefully quit thread"
                    return
        except ConnectionError as e:
            noticer.Noticer.send_wehcat("redis2kafka", "RedisConnectionError", params['mailTo'],
                                       "redis cannot connect :" + redis_host + "::" + redis_port + " " + redis_key + "\n err msg: " + e.message)
            noticer.Noticer.send_email("redis2kafka", "redis2Kafka_Warning", params['mailTo'],
                                       "redis cannot connect :" + redis_host + "::" + redis_port + " " + redis_key + "\n err msg: " + e.message)
            logging.error('%s %s::%s \nData: %s' % (e, redis_host, redis_port, item[1]))
            time.sleep(params['redisTimeoutSleep'])
        except KafkaTimeoutError as e:
            redis.push_back(redis_key, item[1])
            noticer.Noticer.send_wehcat("redis2kafka", "KafkaTimeoutError", params['mailTo'],
                                        "kafka cannot connect :" + servers + "::" + kfk_topic + " " + "\n err msg: " + e.message)
            noticer.Noticer.send_email("redis2kafka", "redis2Kafka_Warning", params['mailTo'],
                                       "kafka cannot connect :" + servers + "::" + kfk_topic + " " + "\n err msg: " + e.message)
            logging.error('%s %s %s \nData: %s' % (e, servers, kfk_topic, item[1]))
            time.sleep(params['kafkaTimeoutSleep'])
        except NotLeaderForPartitionError as e:
            redis.push_back(redis_key, item[1])

            noticer.Noticer.send_wehcat("redis2kafka", "NotLeaderForPartitionError", params['mailTo'],
                                        "kafka errors :" + servers + "::" + kfk_topic + " " + "\n err msg: " + e.message)
            noticer.Noticer.send_email("redis2kafka", "redis2Kafka_Warning", params['mailTo'],
                                       "kafka errors :" + servers + "::" + kfk_topic + " " + "\n err msg: " + e.message)
            logging.error('%s %s %s \nData: %s' % (e, servers, kfk_topic, item[1]))
            time.sleep(params['kafkaNoLeaderSleep'])
        except Exception as e:
            # push back data
            noticer.Noticer.send_wehcat("redis2kafka", "Exception", params['mailTo'],
                                        "Exception :" + "\n err msg: " + e.message)
            noticer.Noticer.send_email("redis2kafka", "Exception", params['mailTo'],
                                       "Exception :" + "\n err msg: " + e.message)
            redis.push_back(redis_key, item[1])
            traceback.print_exc(e)
            logging.error('%s \nData: %s' % (e, item[1]))
            time.sleep(params['BaseExceptionSleep'])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        gl.set_value('NEED_KILL', True)
        while gl.get_value('NEED_KILL', False) and threading.active_count() > gl.get_value('TASK_NUM', 0) + 1:
            print "waiting for produce threads exit..."
            time.sleep(1)

        print "Main Thread Exit"
        sys.exit()
