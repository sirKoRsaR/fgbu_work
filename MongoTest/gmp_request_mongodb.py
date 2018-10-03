from pymongo import MongoClient
import json
import datetime
from bson import json_util
from pprint import pprint

nowtime = datetime.datetime.now()


shard_ppoz = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', 'error']
outFiles = ['log_req_gmp_shard_' + i + '.log' for i in shard_ppoz]
outFileOpen = [open(i, 'w') for i in outFiles]
countReq = 0

client = MongoClient('mongodb://support:support@ppoz-mongos-request-07.prod.egrn:27017')
mdb = client['rrpdb']

coll = mdb.requests
for itemResult in coll.find({
                            # '_id': 'PKPVDMFC-2018-08-14-039594',
                            'status': 'quittancesCreated',
                            # 'region': '24',
                            # 'processingFlags': None,
                            'processingFlags.sentToRegSystem': None,
                            'lastActionDate': {'$lt': (nowtime - datetime.timedelta(days=1))}
                            },
                            {
                                '_id': 1,
                                'region': 1,
                                'gmpServiceRequestNumber': 1,
                                'status': 1,
                                'processingFlags': 1,
                                'bpmNodeId.PPOZ': 1,
                                'lastActionDate': 1
                            }):
    # .limit(10):
    # print(json.loads(itemResult))
    countReq = countReq + 1
    # print(itemResult)

    # print(itemResult['bpmNodeId']['PPOZ'])
    # print(itemResult['lastActionDate'])

    if itemResult['bpmNodeId']['PPOZ'] == 'bpm':
        json.dump(itemResult, outFileOpen[0], default=json_util.default)
        outFileOpen[0].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_01':
        json.dump(itemResult, outFileOpen[1], default=json_util.default)
        outFileOpen[1].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_02':
        json.dump(itemResult, outFileOpen[2], default=json_util.default)
        outFileOpen[2].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_03':
        json.dump(itemResult, outFileOpen[3], default=json_util.default)
        outFileOpen[3].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_04':
        json.dump(itemResult, outFileOpen[4], default=json_util.default)
        outFileOpen[4].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_05':
        json.dump(itemResult, outFileOpen[5], default=json_util.default)
        outFileOpen[5].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_06':
        json.dump(itemResult, outFileOpen[6], default=json_util.default)
        outFileOpen[6].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_07':
        json.dump(itemResult, outFileOpen[7], default=json_util.default)
        outFileOpen[7].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_08':
        json.dump(itemResult, outFileOpen[8], default=json_util.default)
        outFileOpen[8].write('\n')
    elif itemResult['bpmNodeId']['PPOZ'] == 'bpm_Shard_09':
        json.dump(itemResult, outFileOpen[9], default=json_util.default)
        outFileOpen[9].write('\n')
    else:
        json.dump(itemResult, outFileOpen[10], default=json_util.default)
        outFileOpen[10].write('\n')

print(countReq)
for j in range(len(outFileOpen)):
    outFileOpen[j].close()
