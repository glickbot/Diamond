# coding=utf-8

"""
Collects data from one or more Riak Servers

#### Dependencies

 * riak

#### Notes

The collector is modeled after the code in redisstat. The statistics gathered 
use the Riak /stats API as documented here:

http://docs.basho.com/riak/1.2.0/references/apis/http/HTTP-Status/

Example config file RiakCollector.conf

```
enabled=True
host=riak.example.com
port=8098
```

or for multi-instance mode:

```
enabled=True
instances = nick1@host1:port1, nick2@host2:port2, ...
```

Note: when using the host/port config mode, the port number is used in
the metric key. When using the multi-instance mode, the nick will be used.
If not specified the port will be used.


"""

import diamond.collector
import time
import urllib2
import json


class RiakCollector(diamond.collector.Collector):

    _DEFAULT_HOST = 'localhost'
    _DEFAULT_PORT = 8091

    _KEYS = {
            # CPU and Memory
            # CPU statistics are taken directly from Erlang's cpu_sup module. Documentation for which can be found at ErlDocs: cpu_sup.
            'cpu.nprocs': 'cpu_nprocs', # Number of operating system processes
            'cpu.avg1': 'cpu_avg1', # The average number of active processes for the last 1 minute (equivalent to top(1) command's load average when divided by 256()
            'cpu.avg5': 'cpu_avg5', # The average number of active processes for the last 5 minutes (equivalent to top(1) command's load average when divided by 256()
            'cpu.avg15': 'cpu_avg15', # The average number of active processes for the last 15 minutes (equivalent to top(1) command's load average when divided by 256()
            # Memory statistics are taken directly from the Erlang virtual machine. Documentation for which can be found at ErlDocs: Memory.
            'memory.total': 'memory_total', # Total allocated memory (sum of processes and system)
            'memory.processes': 'memory_processes', # Total amount of memory allocated for Erlang processes
            'memory.processes.used': 'memory_processes_used', # Total amount of memory used by Erlang processes
            'memory.system': 'memory_system', # Total allocated memory that is not directly related to an Erlang process
            'memory.atom': 'memory_atom', # Total amount of memory currently allocated for atom storage
            'memory.atom.used': 'memory_atom_used', # Total amount of memory currently used for atom storage
            'memory.binary': 'memory_binary', # Total amount of memory used for binaries
            'memory.code': 'memory_code', # Total amount of memory allocated for Erlang code
            'memory.ets': 'memory_ets', # Total memory allocated for Erlang Term Storage
            'memory.mem.total': 'mem_total', # Total available system memory
            'memory.mem.allocated': 'mem_allocated', # Total memory allocated for this node
            # Node, Cluster & System
            'system.read.repairs': 'read_repairs', # Number of read repair operations this this node has coordinated in the last minute
            'system.read.repairs.total': 'read_repairs_total', # Number of read repair operations this this node has coordinated since node was started
            'system.coord.redirs.total': 'coord_redirs_total', # Number of requests this node has redirected to other nodes for coordination since node was started
            'system.ring.partitions': 'ring_num_partitions', # Number of partitions in the ring
            'system.ring.creation.size': 'ring_creation_size', # Number of partitions this node is configured to own
            'system.ignored.gossip.total': 'ignored_gossip_total', # Total number of ignored gossip messages since node was started
            'system.handoff.timeouts': 'handoff_timeouts', # Number of handoff timeouts encountered by this node
            'system.precommit.fail': 'precommit_fail', # Number of pre commit hook failures
            'system.postcommit.fail': 'postcommit_fail', # Number of post commit hook failures
            'system.sys.global.heaps.size': 'sys_global_heaps_size', # Current size of the shared global heap
            'system.sys.logical.processors': 'sys_logical_processors', # Number of logical processors available on the system
            'system.sys.process.count': 'sys_process_count', # Number of processes existing on this node
            'system.sys.thread.pool.size': 'sys_thread_pool_size', # Number of threads in the asynchronous thread pool
            'system.sys.wordsize': 'sys_wordsize', # Size of Erlang term words in bytes as an integer, for examples, on 32-bit architectures 4 is returned and on 64-bit architectures 8 is returned
            'system.pbc.connects.total': 'pbc_connects_total', # Number of protocol buffers connections since node was started
            'system.pbc.connects': 'pbc_connects', # Number of protocol buffers connections in the last minute
            'system.pbc.active': 'pbc_active', # Number of active protocol buffers connections
            # Node & VNode Counters
            'vnode.gets': 'vnode_gets', # Number of GET operations coordinated by vnodes on this node within the last minute
            'vnode.puts': 'vnode_puts', # Number of PUT operations coordinated by vnodes on this node within the last minute
            'vnode.gets.total': 'vnode_gets_total', # Number of GET operations coordinated by vnodes on this node since node was started
            'vnode.puts.total': 'vnode_puts_total', # Number of PUT operations coordinated by vnodes on this node since node was started
            'node.gets': 'node_gets', # Combined number of local and non-local GET operations coordinated by this node in the last minute
            'node.gets.total': 'node_gets_total', # Combined number of local and non-local GET operations coordinated by this node since node was started
            # Microsecond Timers
            'node.get.fsm.time.mean': 'node_get_fsm_time_mean', # Mean time between reception of client GET request and subsequent response to client
            'node.get.fsm.time.median': 'node_get_fsm_time_median', # Median time between reception of client GET request and subsequent response to client
            'node.get.fsm.time.95': 'node_get_fsm_time_95', # 95th percentile time between reception of client GET request and subsequent response to client
            'node.get.fsm.time.99': 'node_get_fsm_time_99', # 99th percentile time between reception of client GET request and subsequent response to client
            'node.get.fsm.time.100': 'node_get_fsm_time_100', # 100th percentile time between reception of client GET request and subsequent response to client
            'node.puts': 'node_puts', # Combined number of local and non-local PUT operations coordinated by this node in the last minute
            'node.puts.total': 'node_puts_total', # Combined number of local and non-local PUT operations coordinated by this node since node was started
            'node.put.fsm.time.mean': 'node_put_fsm_time_mean', # Mean time between reception of client PUT request and subsequent response to client
            'node.put.fsm.time.median': 'node_put_fsm_time_median', # Median time between reception of client PUT request and subsequent response to client
            'node.put.fsm.time.95': 'node_put_fsm_time_95', # 95th percentile time between reception of client PUT request and subsequent response to client
            'node.put.fsm.time.99': 'node_put_fsm_time_99', # 99th percentile time between reception of client PUT request and subsequent response to client
            'node.put.fsm.time.100': 'node_put_fsm_time_100', # 100th percentile time between reception of client PUT request and subsequent response to client
            # Object, Index & Sibling Metrics
            'node.get.fsm.objsize.mean': 'node_get_fsm_objsize_mean', # Mean object size encountered by this node within the last minute
            'node.get.fsm.objsize.median': 'node_get_fsm_objsize_median', # Median object size encountered by this node within the last minute
            'node.get.fsm.objsize.95': 'node_get_fsm_objsize_95', # 95th percentile object size encountered by this node within the last minute
            'node.get.fsm.objsize.99': 'node_get_fsm_objsize_99', # 99th percentile object size encountered by this node within the last minute
            'node.get.fsm.objsize.100': 'node_get_fsm_objsize_100', # 100th percentile object size encountered by this node within the last minute
            'vnode.index.reads': 'vnode_index_reads', # Number of vnode index read operations performed in the last minute
            'vnode.index.writes': 'vnode_index_writes', # Number of vnode index write operations performed in the last minute
            'vnode.index.deletes': 'vnode_index_deletes', # Number of vnode index delete operations performed in the last minute
            'vnode.index.reads.total': 'vnode_index_reads_total', # Number of vnode index read operations performed since the node was started
            'vnode.index.writes.total': 'vnode_index_writes_total', # Number of vnode index write operations performed since the node was started
            'vnode.index.deletes.total': 'vnode_index_deletes_total', # Number of vnode index delete operations performed since the node was started
            'node.get.fsm.siblings.mean': 'node_get_fsm_siblings_mean', # Mean number of siblings encountered during all GET operations by this node within the last minute
            'node.get.fsm.siblings.median': 'node_get_fsm_siblings_median', # Median number of siblings encountered during all GET operations by this node within the last minute
            'node.get.fsm.siblings.95': 'node_get_fsm_siblings_95', # 95th percentile of siblings encountered during all GET operations by this node within the last minute
            'node.get.fsm.siblings.99': 'node_get_fsm_siblings_99', # 99th percentile of siblings encountered during all GET operations by this node within the last minute
            'node.get.fsm.siblings.100': 'node_get_fsm_siblings_100', # 100th percentile of siblings encountered during all GET operations by this node within the last minute
            # Pipeline Metrics
            'pipeline.active': 'pipeline_active', # The number of pipelines active in the last 60 seconds
            'pipeline.create.count': 'pipeline_create_count', # The total number of pipelines created since the node was started
            'pipeline.create.error.count': 'pipeline_create_error_count', # The total number of pipeline creation errors since the node was started
            'pipeline.create.error.one': 'pipeline_create_error_one', # The number of pipelines created in the last 60 seconds
            'pipeline.create.one': 'pipeline_create_one', # The number of pipeline creation errors in the last 60 seconds
            }

    def __init__(self, *args, **kwargs):
        super(RiakCollector, self).__init__(*args, **kwargs)

        instance_list = self.config['instances']
        # configobj make str of single-element list, let's convert
        if isinstance(instance_list, basestring):
            instance_list = [instance_list]

        # process original single riak instance
        if len(instance_list) == 0:
            host = self.config['host']
            port = self.config['port']
            instance_list.append('%s:%s' % (host, port))

        self.instances = {}
        for instance in instance_list:

            if '@' in instance:
                (nickname, hostport) = instance.split('@', 2)
            else:
                nickname = None
                hostport = instance

            if ':' in hostport:
                if hostport[0] == ':':
                    host = self._DEFAULT_HOST
                    port = int(hostport[1:])
                else:
                    parts = hostport.split(':')
                    host = parts[0]
                    port = int(parts[1])
            else:
                host = hostport
                port = self._DEFAULT_PORT

            if nickname is None:
                nickname = str(port)

            self.instances[nickname] = (host, port)

    def get_default_config_help(self):
        config_help = super(RiakCollector, self).get_default_config_help()
        config_help.update({
            'host': 'Hostname to collect from',
            'port': 'Port number to collect from',
            'instances': "Riak addresses, comma separated, syntax:"
            + " nick1@host:port, nick2@:port or nick3@host"
        })
        return config_help

    def get_default_config(self):
        """
        Return default config

:rtype: dict

        """
        config = super(RiakCollector, self).get_default_config()
        config.update({
            'host': self._DEFAULT_HOST,
            'port': self._DEFAULT_PORT,
            'path': 'riak',
            'instances': [],
        })
        return config

    def _client(self, host, port):
        """Return a riak client for the configuration.

:param str host: riak host
:param int port: riak port
:rtype: urllib.addinfourl

        """
        try:
            cli = urllib2.urlopen("http://%s:%s/stats" % (host, port))
            return cli
        except Exception, ex:
            self.log.error("RiakCollector: failed to connect to %s:%s. %s.",
                           host, port, ex)

    def _precision(self, value):
        """Return the precision of the number

:param str value: The value to find the precision of
:rtype: int

        """
        value = str(value)
        decimal = value.rfind('.')
        if decimal == -1:
            return 0
        return len(value) - decimal - 1

    def _publish_key(self, nick, key):
        """Return the full key for the partial key.

:param str nick: Nickname for Riak instance
:param str key: The key name
:rtype: str

        """
        return '%s.%s' % (nick, key)

    def _get_info(self, host, port):
        """Return info dict from specified Riak instance

:param str host: riak host
:param int port: riak port
:rtype: dict

        """

        client = self._client(host, port)
        if client is None:
            return None

        json_data = client.read()
        info = json.loads(json_data)
        del client

        return info

    def collect_instance(self, nick, host, port):
        """Collect metrics from a single Riak instance

:param str nick: nickname of riak instance
:param str host: riak host
:param int port: riak port

        """

        # Connect to riak and get the info
        info = self._get_info(host, port)
        if info is None:
            return

        # The structure should include the port for multiple instances per
        # server
        data = dict()

        # Iterate over the top level keys
        for key in self._KEYS:
            if self._KEYS[key] in info:
                data[key] = info[self._KEYS[key]]

        # Publish the data to graphite
        for key in data:
            self.publish(self._publish_key(nick, key),
                         data[key],
                         self._precision(data[key]),
                         'GAUGE')

    def collect(self):
        """Collect the stats from the riak instance and publish them.

        """
        for nick in self.instances.keys():
            (host, port) = self.instances[nick]
            self.collect_instance(nick, host, int(port))
