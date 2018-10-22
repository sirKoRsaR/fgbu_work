import json
import datetime
import js2py
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
        self.client = MongoClient(config.mongodb_conn)
        self.collection = self.mongo_connection(self.client, in_bd, in_collection)
        self.logger = LogForever.LogForever('MongoDB')
        self.logger.put_msg(f'Class: {__name__}.{in_bd}.{in_collection} initialize', 'info')

    def mongo_req_starter(self):
        if self.param_list['MongoDB_req'] == '1':
            print('Выполняем первый запрос')
            self.get_gmp_freeze()

    @staticmethod
    def mongo_connection(in_client, in_bd, in_collection):
        collection = in_client[in_bd][in_collection]
        # collection = mongo_db[in_collection]
        return collection

    def client_close(self):
        self.client.close()
        self.logger.put_msg(f'Server PPOZ MongoDB close', 'info')

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

    def get_gmp_request(self, in_gmp=None, in_bk=None, in_projection=None):
        """

        :param in_projection:
        :param in_bk:
        :param in_gmp:
        :return:
        """
        query = {}
        projection = {}
        sort = []
        if in_projection is None:
            projection = {
                            '_id': 1,
                            'status': 1,
                            'lastUpdated': 1,
                            'receivedDate': 1,
                            'expireDate': 1,
                            'billingInfo.gmpStatus': 1,
                            'billingInfo.gmpStatusDate': 1,
                            'billingInfo.chargePaymentStatus': 1,
                            'billingInfo.prepaidAmount': 1,
                            'billingInfo.chargedAmount': 1,
                            'billingInfo.amountToPay': 1,
                            'billingInfo.extRequestIds': 1,
                            'billingInfo.charge.supplierBillID': 1,
                            'billingInfo.charge.changeStatus': 1,
                            'billingInfo.charge.amount': 1,
                            'subscriptions': 1
                        }
        else:
            projection = in_projection
        if in_gmp is not None:
            query = {'_id': in_gmp}
        elif in_gmp is None and in_bk is not None:
            query = {'billingInfo.extRequestIds': in_bk}
        item_result = self.collection.find(query, projection=projection, sort=sort)
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
                'state': 1,
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

    def get_gmp_error(self):
        item_result = self.collection.find(
            {
                'status': 'error',
                'lastUpdated': {'$gt': (datetime.datetime.now() - datetime.timedelta(days=120))},
                'billingInfo.errorMessage.error': {"$regex": u"^Не удалось импортировать начисление в ГИС ГМП, "
                                                             u"возникла ошибка, описание: -1"}
            },
            {
                '_id': 1,
                'lastUpdated': 1,
                'billingInfo.extRequestIds': 1,
                'billingInfo.errorMessage.error': 1
            }
        )
        return item_result

    def get_query(self, in_query=None, in_projection=None, in_limit=0):
        projection = {}
        if in_projection is None:
            projection = {
                            '_id': 1,
                            'region': 1,
                            'processInstanceId': 1,
                            'status': 1,
                            'state': 1,
                            'requestType': 1,
                            'statusHistory': 1,
                            'statements.billingInfo': 1,
                            'statements.actionCode': 1,
                            'packagePaymentExpiresDate': 1,
                            'gmpServiceResponse.quittances': 1,
                            'gmpServiceRequestNumber': 1,
                            'processingFlags': 1,
                            'awaitingPaymentCancel': 1,
                            'bpmNodeId.PPOZ': 1
                        }
        else:
            projection = in_projection
        sort = []
        item_result = self.collection.find(in_query, projection=projection, sort=sort, limit=in_limit)
        return item_result
            # PKPVDMFC-2018-08-15-085750

    # def get_request_univers(self,
    #                         in_query_param={'_id': '34/15380/2018-3193', 'requestType': 'egron'}):
    #     query = in_query_param
    #     projection = {}


