shard_ppoz_name = [
                   'bpm',
                   'bpm_Shard_01',
                   'bpm_Shard_02',
                   'bpm_Shard_03',
                   'bpm_Shard_04',
                   'bpm_Shard_05',
                   'bpm_Shard_06',
                   'bpm_Shard_07',
                   'bpm_Shard_08',
                   'bpm_Shard_09'
                   ]
shard_ppoz_num = [
                 "02",
                 "04",
                 "06",
                 "08",
                 "10",
                 "12",
                 "14",
                 "16",
                 "18",
                 "20"
                  ]

camunda_shard = ['http://ppoz-process-core-' + i + '.prod.egrn:9084' for i in shard_ppoz_num]
camunda_gmp = 'http://ppoz-gmp-process-01.prod.egrn:9080'
camunda_pkurp = 'http://ppoz-pkurp-process-01.prod.egrn:8080'
mongodb_conn = ['mongodb://support:support@ppoz-mongos-request-07.prod.egrn:27017']

param_list = {'MongoDB_req': ''}

timedelta_days = 2
unload_in_files = 'no'
debug_start = 'no'
get_from_ppoz = 'no'
get_from_gmp = 'no'
get_from_mongo = 'yes'

terminal_status = ['processed', 'rejected', 'obstacles_not_removed', 'returned', 'interrupted', 'validationError',
                   'validationError_EGRN', 'noInformation', 'validationError_SID']
suspended_status = ['suspended', 'pkurp_suspended_by_declarant']
