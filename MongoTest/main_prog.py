import datetime
import json

import requests
from bson import json_util
from pymongo import MongoClient

shard_ppoz = ['bpm', 'bpm_Shard_01', 'bpm_Shard_02', 'bpm_Shard_03', 'bpm_Shard_04', 'bpm_Shard_05',
              'bpm_Shard_06', 'bpm_Shard_07', 'bpm_Shard_08', 'bpm_Shard_09', 'error']
shard_ppoz_api = ["02", "04", "06", "08", "10", "12", "14", "16", "18", "20"]
camunda_shard = ['http://ppoz-process-core-' + i + '.prod.egrn:9084'
                 for i in shard_ppoz_api]
camunda_gmp = ['http://ppoz-gmp-process-01.prod.egrn:9080']
timedelta_days = 1
unload_in_files = 'yes'
debug_start = 'no'


class CamundaApi:

    def __init__(self, server_name):
        self.serverName = server_name

    def get_request_string(self, getApiMethod):
        request_string = self.serverName
        request_string += '/engine-rest/engine/default'
        request_string += getApiMethod
        return request_string

    def get_json_element(self, in_json, in_find_element):
        out_element = []
        for entry in in_json:
            if entry['definitionId'].find(in_find_element) == -1:
                return json.loads('{"id": "None"}')
            else:
                return json.loads('{ "id": "' + entry['id'] + '", "definitionId": "' + entry['definitionId'] + '"}')

    def get_box_api(self, in_key):
        request_str = self.get_request_string('/process-instance?businessKey=' + in_key)
        # print(request_str)
        req_get_data = requests.get(request_str)
        if req_get_data.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                  req_get_data.content.decode('utf-8')))
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        # print(api_result)
        return api_result

    # def get_process_definition(self, inMethod):
    #     #request_str = self.get_request_string('/process-definition')
    #     if inMethod == "stat":
    #         request_str = self.get_request_string('/process-instance?=')
    #     else:
    #         print('Неверно указан параметр вызова API')
    #     print("Вызов: " + request_str)
    #     reqGetData = requests.get(request_str)
    #     if reqGetData.status_code != 200:
    #         exit('Server {} answer: {} {}'.format(self.serverName, reqGetData.status_code,
    #                                               reqGetData.content.decode('utf-8')))
    #     return json.loads(reqGetData.content.decode('utf-8'))


class RequestMongo:

    def __init__(self, in_method_type):
        self.method_type = in_method_type

    def mongo_connection(self):
        client_mongo = MongoClient('mongodb://support:support@ppoz-mongos-request-07.prod.egrn:27017')
        mongo_db = client_mongo['rrpdb']
        collection = mongo_db.requests
        return collection

    # def open_files(self, in_file_name_type):
    #     obj_open_files = []
    #     for j in shard_ppoz:
    #         file_open = 'log_' + in_file_name_type + '_' + j + '.log'
    #         obj_open_files[j] = open(file_open, 'w')
    #     return obj_open_files
    #
    # def close_files(self, in_file_name_type, in_obj_open_files):
    #     for j in shard_ppoz:
    #         # file_open = 'log_' + in_file_name_type + '_' + j + '.log'
    #         # print(file_open)
    #         in_obj_open_files.close()

    def write_data_in_file(self, in_data, in_num_of_shard):
        out_files = ['log_' + self.method_type + '_' + i + '.log' for i in shard_ppoz]
        out_files_open = [open(i, 'a') for i in out_files]
        json.dump(in_data, out_files_open[in_num_of_shard], default=json_util.default)
        out_files_open[in_num_of_shard].write('\n')

        for j in range(len(out_files_open)):
            out_files_open[j].close()

    def get_gmp_freeze_v01(self):
        count_req = 0
        server_api_ppoz = [CamundaApi(camunda_shard[i]) for i in range(len(shard_ppoz) - 1)]
        server_api_gmp = [CamundaApi(camunda_gmp[0])]
        json_result = []
        if self.method_type == 'gmp':
            collection = self.mongo_connection()
            nowtime = datetime.datetime.now()
            for item_result in collection.find({
                # '_id': 'PKPVDMFC-2018-08-22-071405',
                'status': 'quittancesCreated',
                # 'region': '24',
                # 'processingFlags': None,
                'processingFlags.sentToRegSystem': None,
                'lastActionDate': {'$lt': (nowtime - datetime.timedelta(days=timedelta_days))}
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
                count_req = count_req + 1
                # json_result = json_result + [item_result]
                try:
                    num_of_shard = shard_ppoz.index(item_result['bpmNodeId']['PPOZ'])
                except ValueError:
                    num_of_shard = 11

                # print(item_result['_id'])
                # server_api_ppoz[num_of_shard].get_box_api(item_result['_id'])
                get_pid_api = server_api_ppoz[num_of_shard].get_json_element(server_api_ppoz[num_of_shard].
                                                                             get_box_api(item_result['_id']),
                                                                             'ppoz_gmp')
                get_pid_api_gmp = \
                    server_api_ppoz[num_of_shard].get_json_element(server_api_gmp[0].
                                                                   get_box_api(item_result['gmpServiceRequestNumber']),
                                                                   '')
                # print(get_pid_api_gmp)
                item_result['PPOZ'] = get_pid_api
                item_result['GMP'] = get_pid_api_gmp

                if unload_in_files == unload_in_files:
                    self.write_data_in_file(item_result, num_of_shard)
                # print(item_result)
            print('Всего выгружено ' + str(count_req) + ' обращений')


if __name__ == '__main__':
    requestGmpMongo = RequestMongo('gmp')
    requestGmpMongo.get_gmp_freeze_v01()
