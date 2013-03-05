#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch, call

from diamond.collector import Collector
from riakstat import RiakCollector

################################################################################


class TestRiakCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('RiakCollector', {
            'interval': '10'
        })

        self.collector = RiakCollector(config, None)

    def test_import(self):
        self.assertTrue(RiakCollector)

    @patch.object(Collector, 'publish')
    def test_real_data(self, publish_mock):

        data_1 = {
                  "vnode_gets": 0,
                  "vnode_gets_total": 1041147,
                  "vnode_puts": 0,
                  "vnode_puts_total": 18855554,
                  "vnode_index_reads": 0,
                  "vnode_index_reads_total": 368880,
                  "vnode_index_writes": 0,
                  "vnode_index_writes_total": 18855546,
                  "vnode_index_writes_postings": 0,
                  "vnode_index_writes_postings_total": 29339877,
                  "vnode_index_deletes": 0,
                  "vnode_index_deletes_total": 0,
                  "vnode_index_deletes_postings": 0,
                  "vnode_index_deletes_postings_total": 16294103,
                  "node_gets": 0,
                  "node_gets_total": 4875921,
                  "node_get_fsm_siblings_mean": 0,
                  "node_get_fsm_siblings_median": 0,
                  "node_get_fsm_siblings_95": 0,
                  "node_get_fsm_siblings_99": 0,
                  "node_get_fsm_siblings_100": 0,
                  "node_get_fsm_objsize_mean": 0,
                  "node_get_fsm_objsize_median": 0,
                  "node_get_fsm_objsize_95": 0,
                  "node_get_fsm_objsize_99": 0,
                  "node_get_fsm_objsize_100": 0,
                  "node_get_fsm_time_mean": 0,
                  "node_get_fsm_time_median": 0,
                  "node_get_fsm_time_95": 0,
                  "node_get_fsm_time_99": 0,
                  "node_get_fsm_time_100": 0,
                  "node_puts": 0,
                  "node_puts_total": 18342890,
                  "node_put_fsm_time_mean": 0,
                  "node_put_fsm_time_median": 0,
                  "node_put_fsm_time_95": 0,
                  "node_put_fsm_time_99": 0,
                  "node_put_fsm_time_100": 0,
                  "read_repairs": 0,
                  "read_repairs_total": 251,
                  "coord_redirs_total": 14381194,
                  "executing_mappers": 0,
                  "precommit_fail": 0,
                  "postcommit_fail": 0,
                  "pbc_active": 336,
                  "pbc_connects": 0,
                  "pbc_connects_total": 1869,
                  "cpu_nprocs": 175,
                  "cpu_avg1": 3,
                  "cpu_avg5": 100,
                  "cpu_avg15": 223,
                  "mem_total": 8589934592,
                  "mem_allocated": 6461165568,
                  "nodename": "riak@10.0.0.10",
                  "connected_nodes": [
                      "riak@10.0.0.11",
                      "riak@10.0.0.12",
                      "riak@10.0.0.13",
                      "riak@10.0.0.14",
                  ],
                  "sys_driver_version": "2.0",
                  "sys_global_heaps_size": 0,
                  "sys_heap_type": "private",
                  "sys_logical_processors": 4,
                  "sys_otp_release": "R15B01",
                  "sys_process_count": 918,
                  "sys_smp_support": True,
                  "sys_system_version": "Erlang R15B01 (erts-5.9.1) [source] [64-bit] [smp:4:4] [async-threads:64] [kernel-poll:true]",
                  "sys_system_architecture": "x86_64-unknown-linux-gnu",
                  "sys_threads_enabled": True,
                  "sys_thread_pool_size": 64,
                  "sys_wordsize": 8,
                  "ring_members": [
                      "riak@10.0.0.10",
                      "riak@10.0.0.11",
                      "riak@10.0.0.12",
                      "riak@10.0.0.13",
                      "riak@10.0.0.14",
                  ],
                  "ring_num_partitions": 64,
                  "ring_ownership": "[{'riak@10.0.0.10',12},\n {'riak@10.0.0.11',12},\n {'riak@10.0.0.12',16},\n {'riak@10.0.0.13',12},\n {'riak@10.0.0.14',12}]",
                  "ring_creation_size": 64,
                  "storage_backend": "riak_kv_eleveldb_backend",
                  "erlydtl_version": "0.7.0",
                  "riak_control_version": "1.2.0",
                  "cluster_info_version": "1.2.2",
                  "riak_api_version": "1.2.0",
                  "riak_search_version": "1.2.1",
                  "merge_index_version": "1.2.1",
                  "riak_kv_version": "1.2.1",
                  "riak_pipe_version": "1.2.1",
                  "riak_core_version": "1.2.1",
                  "lager_version": "1.2.0",
                  "syntax_tools_version": "1.6.8",
                  "compiler_version": "4.8.1",
                  "bitcask_version": "1.5.2",
                  "basho_stats_version": "1.0.2",
                  "luke_version": "0.2.5",
                  "webmachine_version": "1.9.2",
                  "mochiweb_version": "1.5.1",
                  "inets_version": "5.9",
                  "erlang_js_version": "1.2.1",
                  "runtime_tools_version": "1.8.8",
                  "os_mon_version": "2.2.9",
                  "riak_sysmon_version": "1.1.2",
                  "ssl_version": "5.0.1",
                  "public_key_version": "0.15",
                  "crypto_version": "2.1",
                  "sasl_version": "2.2.1",
                  "stdlib_version": "1.18.1",
                  "kernel_version": "2.15.1",
                  "memory_total": 32880512,
                  "memory_processes": 10745216,
                  "memory_processes_used": 10745202,
                  "memory_system": 22135296,
                  "memory_atom": 553569,
                  "memory_atom_used": 538824,
                  "memory_binary": 3192208,
                  "memory_code": 12004016,
                  "memory_ets": 999664,
                  "ignored_gossip_total": 0,
                  "rings_reconciled_total": 22,
                  "rings_reconciled": 0,
                  "gossip_received": 9,
                  "rejected_handoffs": 0,
                  "handoff_timeouts": 0,
                  "converge_delay_min": 0,
                  "converge_delay_max": 0,
                  "converge_delay_mean": 0,
                  "converge_delay_last": 0,
                  "rebalance_delay_min": 0,
                  "rebalance_delay_max": 0,
                  "rebalance_delay_mean": 0,
                  "rebalance_delay_last": 0,
                  "riak_kv_vnodes_running": 12,
                  "riak_kv_vnodeq_min": 0,
                  "riak_kv_vnodeq_median": 0,
                  "riak_kv_vnodeq_mean": 0,
                  "riak_kv_vnodeq_max": 0,
                  "riak_kv_vnodeq_total": 0,
                  "riak_pipe_vnodes_running": 12,
                  "riak_pipe_vnodeq_min": 0,
                  "riak_pipe_vnodeq_median": 0,
                  "riak_pipe_vnodeq_mean": 0,
                  "riak_pipe_vnodeq_max": 0,
                  "riak_pipe_vnodeq_total": 0
                  }
        data_2 = {
                  "vnode_gets": 0,
                  "vnode_gets_total": 1045337,
                  "vnode_puts": 0,
                  "vnode_puts_total": 181231554,
                  "vnode_index_reads": 0,
                  "vnode_index_reads_total": 368880,
                  "vnode_index_writes": 0,
                  "vnode_index_writes_total": 18855546,
                  "vnode_index_writes_postings": 0,
                  "vnode_index_writes_postings_total": 29339877,
                  "vnode_index_deletes": 0,
                  "vnode_index_deletes_total": 0,
                  "vnode_index_deletes_postings": 0,
                  "vnode_index_deletes_postings_total": 16294103,
                  "node_gets": 0,
                  "node_gets_total": 4875921,
                  "node_get_fsm_siblings_mean": 0,
                  "node_get_fsm_siblings_median": 0,
                  "node_get_fsm_siblings_95": 0,
                  "node_get_fsm_siblings_99": 0,
                  "node_get_fsm_siblings_100": 0,
                  "node_get_fsm_objsize_mean": 0,
                  "node_get_fsm_objsize_median": 0,
                  "node_get_fsm_objsize_95": 0,
                  "node_get_fsm_objsize_99": 0,
                  "node_get_fsm_objsize_100": 0,
                  "node_get_fsm_time_mean": 0,
                  "node_get_fsm_time_median": 0,
                  "node_get_fsm_time_95": 0,
                  "node_get_fsm_time_99": 0,
                  "node_get_fsm_time_100": 0,
                  "node_puts": 0,
                  "node_puts_total": 18342890,
                  "node_put_fsm_time_mean": 0,
                  "node_put_fsm_time_median": 0,
                  "node_put_fsm_time_95": 0,
                  "node_put_fsm_time_99": 0,
                  "node_put_fsm_time_100": 0,
                  "read_repairs": 0,
                  "read_repairs_total": 251,
                  "coord_redirs_total": 14381194,
                  "executing_mappers": 0,
                  "precommit_fail": 0,
                  "postcommit_fail": 0,
                  "pbc_active": 336,
                  "pbc_connects": 0,
                  "pbc_connects_total": 1869,
                  "cpu_nprocs": 175,
                  "cpu_avg1": 3,
                  "cpu_avg5": 100,
                  "cpu_avg15": 223,
                  "mem_total": 8589934592,
                  "mem_allocated": 6461165568,
                  "nodename": "riak@10.0.1.10",
                  "connected_nodes": [
                      "riak@10.0.1.11",
                      "riak@10.0.1.12",
                      "riak@10.0.1.13",
                      "riak@10.0.1.14",
                  ],
                  "sys_driver_version": "2.0",
                  "sys_global_heaps_size": 0,
                  "sys_heap_type": "private",
                  "sys_logical_processors": 4,
                  "sys_otp_release": "R15B01",
                  "sys_process_count": 918,
                  "sys_smp_support": True,
                  "sys_system_version": "Erlang R15B01 (erts-5.9.1) [source] [64-bit] [smp:4:4] [async-threads:64] [kernel-poll:true]",
                  "sys_system_architecture": "x86_64-unknown-linux-gnu",
                  "sys_threads_enabled": True,
                  "sys_thread_pool_size": 64,
                  "sys_wordsize": 8,
                  "ring_members": [
                      "riak@10.0.1.10",
                      "riak@10.0.1.11",
                      "riak@10.0.1.12",
                      "riak@10.0.1.13",
                      "riak@10.0.1.14",
                  ],
                  "ring_num_partitions": 64,
                  "ring_ownership": "[{'riak@10.0.1.10',12},\n {'riak@10.0.1.11',12},\n {'riak@10.0.1.12',16},\n {'riak@10.0.1.13',12},\n {'riak@10.0.1.14',12}]",
                  "ring_creation_size": 64,
                  "storage_backend": "riak_kv_eleveldb_backend",
                  "erlydtl_version": "0.7.0",
                  "riak_control_version": "1.2.0",
                  "cluster_info_version": "1.2.2",
                  "riak_api_version": "1.2.0",
                  "riak_search_version": "1.2.1",
                  "merge_index_version": "1.2.1",
                  "riak_kv_version": "1.2.1",
                  "riak_pipe_version": "1.2.1",
                  "riak_core_version": "1.2.1",
                  "lager_version": "1.2.0",
                  "syntax_tools_version": "1.6.8",
                  "compiler_version": "4.8.1",
                  "bitcask_version": "1.5.2",
                  "basho_stats_version": "1.0.2",
                  "luke_version": "0.2.5",
                  "webmachine_version": "1.9.2",
                  "mochiweb_version": "1.5.1",
                  "inets_version": "5.9",
                  "erlang_js_version": "1.2.1",
                  "runtime_tools_version": "1.8.8",
                  "os_mon_version": "2.2.9",
                  "riak_sysmon_version": "1.1.2",
                  "ssl_version": "5.0.1",
                  "public_key_version": "0.15",
                  "crypto_version": "2.1",
                  "sasl_version": "2.2.1",
                  "stdlib_version": "1.18.1",
                  "kernel_version": "2.15.1",
                  "memory_total": 32880512,
                  "memory_processes": 10745216,
                  "memory_processes_used": 10745202,
                  "memory_system": 22135296,
                  "memory_atom": 553569,
                  "memory_atom_used": 538824,
                  "memory_binary": 3192208,
                  "memory_code": 12004016,
                  "memory_ets": 999664,
                  "ignored_gossip_total": 0,
                  "rings_reconciled_total": 22,
                  "rings_reconciled": 0,
                  "gossip_received": 9,
                  "rejected_handoffs": 0,
                  "handoff_timeouts": 0,
                  "converge_delay_min": 0,
                  "converge_delay_max": 0,
                  "converge_delay_mean": 0,
                  "converge_delay_last": 0,
                  "rebalance_delay_min": 0,
                  "rebalance_delay_max": 0,
                  "rebalance_delay_mean": 0,
                  "rebalance_delay_last": 0,
                  "riak_kv_vnodes_running": 12,
                  "riak_kv_vnodeq_min": 0,
                  "riak_kv_vnodeq_median": 0,
                  "riak_kv_vnodeq_mean": 0,
                  "riak_kv_vnodeq_max": 0,
                  "riak_kv_vnodeq_total": 0,
                  "riak_pipe_vnodes_running": 12,
                  "riak_pipe_vnodeq_min": 0,
                  "riak_pipe_vnodeq_median": 0,
                  "riak_pipe_vnodeq_mean": 0,
                  "riak_pipe_vnodeq_max": 0,
                  "riak_pipe_vnodeq_total": 0
                  }


        patch_collector = patch.object(RiakCollector, '_get_info',
                                       Mock(return_value=data_1))
        patch_time = patch('time.time', Mock(return_value=10))

        patch_collector.start()
        patch_time.start()
        self.collector.collect()
        patch_collector.stop()
        patch_time.stop()

        self.assertPublishedMany(publish_mock, {})

        patch_collector = patch.object(RiakCollector, '_get_info',
                                       Mock(return_value=data_2))
        patch_time = patch('time.time', Mock(return_value=20))

        patch_collector.start()
        patch_time.start()
        self.collector.collect()
        patch_collector.stop()
        patch_time.stop()

        metrics = {"8091.vnode.gets.last_minute": 0,
                  "8091.vnode.gets.total": 1045337,
                  "8091.vnode.puts.last_minute": 0,
                  "8091.vnode.puts.total": 181231554,
                  "8091.vnode.index.reads.last_minute": 0,
                  "8091.vnode.index.reads.total": 368880,
                  "8091.vnode.index.writes.last_minute": 0,
                  "8091.vnode.index.writes.total": 18855546,
                  "8091.vnode.index.deletes.last_minute": 0,
                  "8091.vnode.index.deletes.total": 0,
                  "8091.system.connected_nodes_count": 4,
                  "8091.system.ring.members_count": 5,
                  }


        self.assertPublishedMany(publish_mock, metrics)

        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['path'])

    @patch.object(Collector, 'publish')
    def test_hostport_or_instance_config(self, publish_mock):

        testcases = {
            'default': {
                'config': {},  # test default settings
                'calls': [call('8091', 'localhost', 8091)],
            },
            'host_set': {
                'config': {'host': 'myhost'},
                'calls': [call('8091', 'myhost', 8091)],
            },
            'port_set': {
                'config': {'port': 5005},
                'calls': [call('5005', 'localhost', 5005)],
            },
            'hostport_set': {
                'config': {'host': 'megahost', 'port': 5005},
                'calls': [call('5005', 'megahost', 5005)],
            },
            'instance_1_host': {
                'config': {'instances': ['nick@myhost']},
                'calls': [call('nick', 'myhost', 8091)],
            },
            'instance_1_port': {
                'config': {'instances': ['nick@:9191']},
                'calls': [call('nick', 'localhost', 9191)],
            },
            'instance_1_hostport': {
                'config': {'instances': ['nick@host1:8765']},
                'calls': [call('nick', 'host1', 8765)],
            },
            'instance_2': {
                'config': {'instances': ['foo@hostX', 'bar@:1000']},
                'calls': [
                    call('foo', 'hostX', 8091),
                    call('bar', 'localhost', 1000)
                ],
            },
            'old_and_new': {
                'config': {
                    'host': 'myhost',
                    'port': 1234,
                    'instances': [
                        'foo@hostX',
                        'bar@:1000',
                        'hostonly',
                        ':1234'
                    ]
                },
                'calls': [
                    call('foo', 'hostX', 8091),
                    call('bar', 'localhost', 1000),
                    call('8091', 'hostonly', 8091),
                    call('1234', 'localhost', 1234),
                ],
            },
        }

        for testname, data in testcases.items():
            config = get_collector_config('RiakCollector', data['config'])

            collector = RiakCollector(config, None)

            mock = Mock(return_value={}, name=testname)
            patch_c = patch.object(RiakCollector, 'collect_instance', mock)

            patch_c.start()
            collector.collect()
            patch_c.stop()

            expected_call_count = len(data['calls'])
            self.assertEqual(mock.call_count, expected_call_count,
                             msg='[%s] mock.calls=%d != expected_calls=%d' %
                             (testname, mock.call_count, expected_call_count))
            for exp_call in data['calls']:
                # Test expected calls 1 by 1,
                # because self.instances is a dict (=random order)
                mock.assert_has_calls(exp_call)

    @patch.object(Collector, 'publish')
    def test_key_naming_when_using_instances(self, publish_mock):

        config_data = {
            'instances': [
                'nick1@host1:1111',
                'nick2@:2222',
                'nick3@host3',
                'bla'
            ]
        }
        get_info_data = {
            'vnode_gets_total': 200,
            'vnode_puts_total': 100,
        }
        expected_calls = [
            call('nick1.vnode.gets.total', 200, 0, 'GAUGE'),
            call('nick1.vnode.puts.total', 100, 0, 'GAUGE'),
            call('nick2.vnode.gets.total', 200, 0, 'GAUGE'),
            call('nick2.vnode.puts.total', 100, 0, 'GAUGE'),
            call('nick3.vnode.gets.total', 200, 0, 'GAUGE'),
            call('nick3.vnode.puts.total', 100, 0, 'GAUGE'),
            call('8091.vnode.gets.total', 200, 0, 'GAUGE'),
            call('8091.vnode.puts.total', 100, 0, 'GAUGE'),
        ]

        config = get_collector_config('RiakCollector', config_data)
        collector = RiakCollector(config, None)

        patch_c = patch.object(RiakCollector, '_get_info',
                               Mock(return_value=get_info_data))

        patch_c.start()
        collector.collect()
        patch_c.stop()

        self.assertEqual(publish_mock.call_count, len(expected_calls))
        for exp_call in expected_calls:
            # Test expected calls 1 by 1,
            # because self.instances is a dict (=random order)
            publish_mock.assert_has_calls(exp_call)


################################################################################
if __name__ == "__main__":
    unittest.main()
