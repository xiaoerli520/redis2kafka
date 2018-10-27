#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis


class RedisMonitor:
    def __init__(self, host='localhost', port=6379, list_key=None):
        self.redis = redis.Redis(
            host=host, port=port,
            db=0, password=None, socket_timeout=None,
            socket_connect_timeout=None,
            socket_keepalive=True,
        )
        self.list_key = list_key

    def list_length(self):
        return self.redis.llen(self.list_key)
