#!/usr/bin/python
#
# Copyright (C) 2011  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
# Written by Mark Smith <mark@qq.is>.
#

"""Statistics from a Redis instance.

Note: this collector parses your Redis configuration files to determine what cluster
this instance is part of.  If you want the cluster tag to be accurate, please edit
your Redis configuration file and add a comment like this somewhere in the file:

# tcollector.cluster = main

You can name the cluster anything that matches the regex [a-z0-9-_]+.

This collector outputs the following metrics:

 - redis.bgrewriteaof_in_progress
 - redis.bgsave_in_progress
 - redis.blocked_clients
 - redis.changes_since_last_save
 - redis.client_biggest_input_buf
 - redis.client_longest_output_list
 - redis.connected_clients
 - redis.connected_slaves
 - redis.expired_keys
 - redis.evicted_keys
 - redis.hash_max_zipmap_entries
 - redis.hash_max_zipmap_value
 - redis.keyspace_hits
 - redis.keyspace_misses
 - redis.mem_fragmentation_ratio
 - redis.pubsub_channels
 - redis.pubsub_patterns
 - redis.total_commands_processed
 - redis.total_connections_received
 - redis.uptime_in_seconds
 - redis.used_cpu_sys
 - redis.used_cpu_user
 - redis.used_memory
 - redis.used_memory_rss

For more information on these values, see this (not very useful) documentation:

    http://redis.io/commands/info
"""

import re
import subprocess
import sys
import time
import telnetlib
import json
import httplib
from collectors.lib import utils

# If we are root, drop privileges to this user, if necessary.  NOTE: if this is
# not root, this MUST be the user that you run redis-server under.  If not, we
# will not be able to find your Redis instances.
USER = "root"

# Every SCAN_INTERVAL seconds, we look for new redis instances.  Prevents the
# situation where you put up a new instance and we never notice.
SCAN_INTERVAL = 300

# these are the things in the info struct that we care about
KEYS = [
    'pubsub_channels', 'bgrewriteaof_in_progress', 'connected_slaves', 'connected_clients', 'keyspace_misses',
    'used_memory', 'total_commands_processed', 'used_memory_rss', 'total_connections_received', 'pubsub_patterns',
    'used_cpu_sys', 'blocked_clients', 'used_cpu_user', 'expired_keys', 'bgsave_in_progress', 'hash_max_zipmap_entries',
    'hash_max_zipmap_value', 'client_longest_output_list', 'client_biggest_input_buf', 'uptime_in_seconds',
    'changes_since_last_save', 'mem_fragmentation_ratio', 'keyspace_hits', 'evicted_keys'
];


def main():
    """Main loop"""

    ##if USER != "root":
    ##    utils.drop_privileges(user=USER)
    ##sys.stdin.close()

    interval = 15
    host="10.111.1.25"
    port=9990
    uri=host+":%d"%port
    # we scan for instances here to see if there are any redis servers
    # running on this machine...
    last_scan = time.time()
    instances = scan_for_instances_http(host, port)  # ip:port:role
    if not len(instances):
        return 13
    def print_stat(metric, value, tags=""):
        if value is not None:
            print "redis.%s %d %s %s" % (metric, ts, value, tags)

    while True:
        ts = int(time.time())

        # if we haven't looked for redis instances recently, let's do that
        if ts - last_scan > SCAN_INTERVAL:
            instances = scan_for_instances_http()
            last_scan = ts
        urlrequest="/list/redis/info?ip=%s&port=%d&section=cmd"
        # now iterate over every instance and gather statistics
        for ipport in instances:
            host=ipport.split(":")[0]
            ports=ipport.split(":")[1]
            port = int(ports)
            tags = "cluster=%s port=%s" % (host, ports)
            # connect to the instance and attempt to gather info
            con = httplib.HTTPConnection(uri)
            urlre = urlrequest %(host,port)
            con.request('get',urlre)
            resjs = con.getresponse().read()
            jsobj = json.loads(resjs)
            ##the version link info
            metrics="linker"
            value = jsobj[0].get(metrics)
            print_stat(metrics, value, tags)
            nums = len(jsobj[1])
            cmdjs = jsobj[1]
            metricslist=jsobj[1][0]
            for i in range(1,nums):
                for k in range(1,3+1):
                    metrics=metricslist[k]
                    value = cmdjs[i][k]
                    tags_tmp=" cmd=%s" %cmdjs[i][0]
                    tagsone = tags+tags_tmp
                    print_stat(metrics,value,tagsone)
        sys.stdout.flush()
        time.sleep(interval)
        break
def scan_for_instances_http(host,port):
    timeout = 1000
    uri=host+":%d"%port
    urlre = "/list/redis"
    con = httplib.HTTPConnection(uri,timeout=timeout)
    con.request('get',urlre )
    redisjs = con.getresponse().read()
    alljs = json.loads(redisjs)
    unionsList = alljs.get("unions");
    out = {}
    for shards in unionsList:
        masterhost = shards.get("master")
        out[masterhost]="master"
        slavelist = shards.get("slave")
        for slave in slavelist:
            slavehost = slave.get("id")
            out[slavehost]="slave"
    return out

def scan_for_instances(host,port):
    """Use netstat to find instances of Redis listening on the local machine, then
    figure out what configuration file they're using to name the cluster."""
    timeout = 1000
    uri=host+":%d"%port
    listcmd = "list redis\r\n"
    tn = telnetlib.Telnet(host, port, timeout)
    tn.read_very_eager()
    time.sleep(1)
    tn.read_very_eager()
    tn.write(listcmd)
    redislist = tn.read_very_eager()
    alljs = json.loads(redislist)
    unionsList = alljs.get("unions");
    out = {}
    for shards in unionsList:
        masterhost = shards.get("master")
        out[masterhost]="master"
        slavelist = shards.get("slave")
        for slave in slavelist:
            slavehost = slave.get("id")
            out[slavehost]="slave"
    return out


if __name__ == "__main__":
    sys.exit(main())
