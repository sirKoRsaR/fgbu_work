import json
import datetime
import config
import classes.CamundaAPI as CamundaApi
from pymongo import MongoClient


class MongoRequest:

    def __init__(self, param_list):
        self.param_list = param_list
        if param_list['MongoDB_req'] == 1:
            self.get_gmp_freeze()

    def mongo_req_starter(self):
        if self.param_list['MongoDB_req'] == '1':
            print('Выполняем первый запрос')
            self.get_gmp_freeze()

    @staticmethod
    def mongo_connection():
        client_mongo = MongoClient(config.mongodb_conn)
        mongo_db = client_mongo['rrpdb']
        collection = mongo_db.requests
        return collection

    # def mongo_request_list(self):
        # TODO Сделать автоматический сбор запросов

    def write_data_in_file(self, in_data, in_num_of_shard):
        out_files = ['log_' + self.method_type + '_' + i + '.log' for i in config.shard_ppoz_name]
        out_files_open = [open(i, 'a') for i in out_files]
        json.dump(in_data, out_files_open[in_num_of_shard], default=json_util.default)
        out_files_open[in_num_of_shard].write('\n')

        for j in range(len(out_files_open)):
            out_files_open[j].close()

    def get_gmp_freeze(self):
        count_req = 0
        collection = self.mongo_connection()
        nowtime = datetime.datetime.now()
        for item_result in collection.find({
                # '_id': 'PKPVDMFC-2018-08-22-071405',
                'status': 'quittancesCreated',
                'bpmNodeId.PPOZ': 'bpm_Shard_01',
                # 'region': '24',
                # 'processingFlags': None,
                'processingFlags.sentToRegSystem': None,
                'lastActionDate': {'$lt': (nowtime - datetime.timedelta(days=config.timedelta_days))}
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
                    num_of_shard = config.shard_ppoz_name.index(item_result['bpmNodeId']['PPOZ'])
                except ValueError:
                    num_of_shard = 11

                # print(item_result['_id'])
                # server_api_ppoz[num_of_shard].get_box_api(item_result['_id'])

                # print(item_result)
        print('Всего найдено ' + str(count_req) + ' обращений(я)')

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