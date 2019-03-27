# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import socket

from charmhelpers.core import host, hookenv
from charmhelpers.core.templating import render
from charmhelpers.core.hookenv import config

from charms.layer import snap

METAMORPHOSIS_SNAP = 'metamorphosis'
METAMORPHOSIS_SERVICE = 'snap.{}.metamorphosis.service'.format(
    METAMORPHOSIS_SNAP)
METAMORPHOSIS_SNAP_COMMON = '/var/snap/{}/common'.format(METAMORPHOSIS_SNAP)


class Metamorphosis(object):
    def __init__(self):
        self.cfg = config()

    def configure(self, kafka_brokers, influxdb, topics_yaml):
        kafka = []
        for unit in kafka_brokers:
            ip = resolve_private_address(unit['host'])
            kafka.append('{}:{}'.format(ip, unit['port']))
        kafka.sort()
        kafka_connect = ','.join(kafka)

        influxdb_connect = ""
        if not influxdb.user() and not influxdb.password():
            influxdb_connect = '{}:{}'.format(
                influxdb.hostname(),
                influxdb.port())
        else:
            influxdb_connect = '{}:{}@{}:{}'.format(
                influxdb.user(),
                influxdb.password(),
                influxdb.hostname(),
                influxdb.port())

        context = {
            'kafka_brokers': kafka_connect,
            'influx_db': influxdb_connect,
            'topics_yaml': topics_yaml,
        }

        render(
            source='config.yaml',
            target=os.path.join(
                METAMORPHOSIS_SNAP_COMMON,
                'etc',
                'exporter.config'
            ),
            owner="root",
            perms=0o644,
            context=context
        )

        self.restart()

    def restart(self):
        '''
        Restarts the metamorphosis service.
        '''
        host.service_restart(METAMORPHOSIS_SERVICE)

    def start(self):
        '''
        Starts the metamorphosis service.
        '''
        host.service_start(METAMORPHOSIS_SERVICE)

    def stop(self):
        '''
        Stops the metamorphosis service
        '''
        host.service_stop(METAMORPHOSIS_SERVICE)

    def version(self):
        '''
        Will attempt to get the version from the version fieldof the
        Kafka snap file.
        If there is a reader exception or a parser exception, unknown
        will be returned
        '''
        return snap.get_installed_version(METAMORPHOSIS_SNAP) or 'unknown'


def resolve_private_address(addr):
    IP_pat = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
    contains_IP_pat = re.compile(r'\d{1,3}[-.]\d{1,3}[-.]\d{1,3}[-.]\d{1,3}')
    if IP_pat.match(addr):
        return addr  # already IP
    try:
        ip = socket.gethostbyname(addr)
        return ip
    except socket.error as e:
        hookenv.log(
            'Unable to resolve private IP: %s (will attempt to guess)' %
            addr,
            hookenv.ERROR
        )
        hookenv.log('%s' % e, hookenv.ERROR)
        contained = contains_IP_pat.search(addr)
        if not contained:
            raise ValueError(
                'Unable to resolve or guess IP from private-address: %s' % addr
            )
        return contained.groups(0).replace('-', '.')
