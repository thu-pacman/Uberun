'''
For global configuration
'''
class SSConfig:
    # Logger output control
    LOG = {
        'colorful': True,
        'error': True,
        'warn': True,
        'info': True,
        'debug': False,
        'succ': False,
        'echo': False,
    }
    # Network setting
    NET = {
        'eoc': chr(200),
        'broken_conn_str': 'Broken Connection',
        'new_conn_str': 'New Connection',
        'master_hostname': 'bic05',
        'master_port': 19229,
        'master_backlog': 128,
    }
    # Cluster setting
    CLUSTER = {
        'core_per_node': 28,
        'llcway_per_node': 20,
        'membw_per_node': 120,
        'cpu_freq_factor': {1: 1.0, 2: 1.02, 4: 1.05, 8:1.15}, # emperical values to cancel out CPU frequency boost.
    }
    # Database setting
    DB = {
        'profile_fname': 'progs_profile.txt',
        'history_prefix': 'job_history',
        'default_stride': 100,
        'slow_stride': 50,
    }
    # Profiling setting
    PROF = {
        'sample_ways': [20, 8, 4, 2],
    }
    # Running jobs
    RUN = {
        'deploy_path': '/home/txc/SSprototype/',
        # where the executable is
        'exe_path': {
            'npb': '/home/txc/ssTest/npb/',
            'hc' : '/home/txc/ssTest/h264/runh264.sh',
            'bw' : '/home/txc/ssTest/bwaves/bwaves_base.cpu2006.linux64.intel64.fast',
            'bfs': '/home/txc/ssTest/graph500_reference_bfs',
            'gan': '/home/txc/TensorFlow-Examples/examples/3_NeuralNetworks/gan.sh',
            'rnn': '/home/txc/TensorFlow-Examples/examples/3_NeuralNetworks/rnn.sh',
            'spark': '/home/txc/ssTest/spark/',
        },
        # where you need to cd then run
        'exe_dir': {
            'hc' : '/home/txc/ssTest/h264',
            'bw' : '/home/txc/ssTest/bwaves',
        }
    }

