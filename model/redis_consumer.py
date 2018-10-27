#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis


class RedisClient:
    def __init__(self, host='localhost', port=6379, pop_timeout=5):
        try:
            self.redis = redis.Redis(
                host=host, port=port,
                db=0, password=None, socket_timeout=None,
                socket_connect_timeout=None,
                socket_keepalive=True,
            )
            self.pop_timeout=pop_timeout
        except Exception as e:
            raise e

    def load_data(self, redis_key):
        try:
            data = self.redis.blpop(redis_key, self.pop_timeout)
            return data
        except Exception as e:
            raise e

    def push_back(self, redis_key, data, debug=False):
        try:
            if debug:
                print "Redis lpush  : "+ redis_key + " " + data
            self.redis.lpush(redis_key, data)
        except Exception:
            raise
