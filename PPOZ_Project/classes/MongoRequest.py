import json
import datetime
from bson import json_util
import config
import classes.LogForever as LogForever
import classes.CamundaAPI as CamundaApi
from pymongo import MongoClient


class MongoRequest(object):

    def __init__(self, in_bd, in_collection, param_list=None):
        self.param_list = param_list
        if self.param_list is None:
            pass
        elif self.param_list['MongoDB_req'] == 1:
            self.get_gmp_freeze()
        self.collection = self.mongo_connection(in_bd, in_collection)
        self.logger = LogForever.LogForever('MongoDB')
        self.logger.put_msg(f'Server PPOZ MongoDB {in_bd}.{in_collection} initialize', 'info')

    def mongo_req_starter(self):
        if self.param_list['MongoDB_req'] == '1':
            print('Выполняем первый запрос')
            self.get_gmp_freeze()

    @staticmethod
    def mongo_connection(in_bd, in_collection):
        collection = MongoClient(config.mongodb_conn)[in_bd][in_collection]
        # mongo_db = client_mongo[in_bd][in_collection]
        # collection = mongo_db[in_collection]
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
        # collection = self.mongo_connection('rrpdb', 'requests')
        nowtime = datetime.datetime.now()
        for item_result in self.collection.find({
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

    def get_gmp_request(self, in_gmp):
        """

        :param in_gmp:
        :return:
        """
        # collection = self.mongo_connection('rrgmp', 'gmpRequest')
        item_result = self.collection.find_one(
            {
                # 'gmpServiceRequestNumber': in_gmp
                '_id': in_gmp
            },
            {
                '_id': 1,
                'status': 1,
                'lastUpdated': 1,
                'receivedDate': 1,
                'expireDate': 1,
                'billingInfo.gmpStatus': 1,
                'billingInfo.gmpStatusDate': 1,
                'billingInfo.charge.supplierBillID': 1,
                'billingInfo.prepaidAmount': 1,
                'billingInfo.amountToPay': 1,
                'billingInfo.chargePaymentStatus': 1,
                'billingInfo.extRequestIds': 1,
                'subscriptions': 1
            })
        # print(in_gmp + ' отработан')
        return item_result

    def get_request(self, in_bk):
        """

        :param in_bk:
        :return:
        """
        item_result = self.collection.find_one(
            {
                '_id': in_bk
            },
            {
                '_id': 1,
                'region': 1,
                'processInstanceId': 1,
                'status': 1,
                'statusHistory': 1,
                'statements.billingInfo': 1,
                'statements.actionCode': 1,
                'packagePaymentExpiresDate': 1,
                'gmpServiceResponse.quittances': 1,
                'gmpServiceRequestNumber': 1,
                'processingFlags': 1,
                'awaitingPaymentCancel': 1,
                'bpmNodeId.PPOZ': 1
            })
        return item_result

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
