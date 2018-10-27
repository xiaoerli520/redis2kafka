#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import global_vars as gl
import logging


'''
使用kafka的生产模块
'''


class KfkProducer:
    def __init__(self, bootstrap_servers='{kafka_host}:{kafka_port}', username=None, password=None, api_version=None,
                 max_block_ms=3000, metadata_max_age_ms=10000, request_timeout_ms=5000, kafka_time_out=2):

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                api_version=api_version,
                security_protocol='PLAINTEXT' if password is None else 'SASL_PLAINTEXT',
                sasl_mechanism=None if password is None else 'PLAIN',
                sasl_plain_username=username,
                sasl_plain_password=password,
                max_block_ms=max_block_ms,
                metadata_max_age_ms=metadata_max_age_ms,
                request_timeout_ms=request_timeout_ms
            )
            self.kafka_time_out = kafka_time_out
        except KafkaError as e:
            raise e
        except Exception as e:
            raise e

    def send_data(self, data_str, kafka_topic):
        try:
            producer = self.producer
            producer.send(kafka_topic, data_str).get(self.kafka_time_out)
            if gl.get_value('IS_DEBUG', False):
                logging.error(kafka_topic + " :: " + data_str)
                print "Kfk Producer Send : " + data_str
            producer.flush()
        except KafkaTimeoutError as e:
            raise e
        except KafkaError as e:
            raise e
        except Exception as e:
            raise e
