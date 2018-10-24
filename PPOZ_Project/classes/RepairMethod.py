import classes.LogForever as LogForever
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import requests
import config
import json
import stomp
import time
from datetime import datetime, timedelta


class RepairMethod(object):

    def __init__(self):
        self.logger = LogForever.LogForever('RepairMethod')
        self.logger.put_msg(f'Class {__name__} initialize', 'info')

    def send_to_mq_gmp(self, destination='/queue/gmpBpmIn',
                       in_key=None,
                       in_headers={'command': 'ru.atc.rosreestr.cmd.StartProcessCommand'}):
        self.logger.put_msg(f'Class: {__name__}.{self.send_to_mq_gmp.__name__} start method', 'info')
        msg = str(json.dumps({'businessKey': in_key}))
        conn = stomp.Connection([(config.broker_mq['url'], config.broker_mq['port'])], auto_content_length=False)
        conn.start()
        conn.connect()
        tx = conn.begin()
        headers = dict(in_headers)
        headers['persistent'] = 'true'
        conn.send(body=msg, destination=destination, headers=headers)
        conn.commit(tx)
        time.sleep(1)
        conn.disconnect()
        self.logger.put_msg('businessKey_send_to_mq_gmp: %s ' % (format(msg)), 'info')
        self.logger.put_msg(f'Class: {__name__}.{self.send_to_mq_gmp.__name__} close method', 'info')

    @staticmethod
    def collector_amount(in_gmp):
        decision_list = {'_id': [], 'statusGmp': [], 'expireDate': [], 'billing': []}
        for ii in in_gmp:
            decision_list['_id'].append(ii.get('_id'))
            decision_list['statusGmp'].append(ii.get('status'))
            decision_list['expireDate'].append(ii.get('expireDate'))
            try:
                ii['billingInfo']
            except (AttributeError, KeyError):
                print(ii['_id'], 'No element:billingInfo')
                continue
            decision_billing = {'result': [], 'gmpStatus': [],
                                'chargePaymentStatus': [], 'amount': [], 'supplierBillID': []}
            count = 0
            for billing in ii['billingInfo']:
                # decision_billing['billing'].append(billing.get('_id'))
                decision_billing['gmpStatus'].append(billing.get('gmpStatus'))
                decision_billing['chargePaymentStatus'].append(billing.get('chargePaymentStatus'))
                try:
                    billing['charge']
                except (AttributeError, KeyError, TypeError):
                    print(ii['_id'], 'No element:billingInfo.charge')
                    continue
                decision_billing['amount'].append(billing.get('charge').get('amount'))
                decision_billing['supplierBillID'].append(billing.get('charge').get('supplierBillID'))
                decision_billing['result'].append(None)
                if billing.get('gmpStatus') in ['chargeImport']:
                    if billing.get('charge').get('amount') == 0:
                        decision_billing['result'][count] = 'OK_0'
                        # импорт начислений, но сумма 0 - ОК
                    elif billing.get('charge').get('amount') == billing.get('prepaidAmount'):
                        decision_billing['result'][count] = 'OK_PRE'
                        # импорт начислений, но сумма не 0 и равна предопдате
                    else:
                        decision_billing['result'][count] = '?'
                        # импорт начислений, но сумма не 0 и нет предопдаты - ??
                elif billing.get('gmpStatus') in ['chargeCanceled']:
                    if billing.get('charge').get('amount') == 0:
                        decision_billing['result'][count] = 'OK_0'
                        # Истек срок ожидания, но сумма 0
                    elif billing.get('charge').get('amount') == billing.get('prepaidAmount')\
                            and billing.get('charge').get('amount') != 0:
                        decision_billing['result'][count] = 'OK_PRE'
                        # импорт начислений, но сумма не 0 и равна предопдате
                    elif billing.get('charge').get('amount') != 0:
                        decision_billing['result'][count] = 'restart'  # ????
                        # Истек срок ожидания и сумма НЕ 0
                        # TODO: Проверить статус всего запроса ГМП и дату expireDate
                    else:
                        decision_billing['result'][count] = '??'  # ????
                elif billing.get('gmpStatus') in ['checkPayment']:
                    pass
                elif billing.get('gmpStatus') in ['checkCharge']:
                    pass
                elif billing.get('gmpStatus') in ['paid']:
                    if billing.get('chargePaymentStatus') in ['1', '2'] \
                            and billing.get('charge').get('amount') != 0:
                        decision_billing['result'][count] = 'OK'
                        # оплачено и сквитировано
                    elif billing.get('chargePaymentStatus') in ['1', '2'] \
                            and billing.get('charge').get('amount') == 0:
                        decision_billing['result'][count] = 'OK_0'
                        # оплачено и сквитировано, но сумма 0
                    elif billing.get('chargePaymentStatus') == '3':
                        decision_billing['result'][count] = '?????'
                        # оплачено, но не сквитировано
                    else:
                        pass
                else:
                    pass
                # print(f"{ii['_id']}\tgmpStatus:{billing['gmpStatus']}\t"
                #       f"amount:{billing.get('charge').get('amount')}\t"
                #       f"chargePaymentStatus:{billing.get('chargePaymentStatus')}")
                count += 1
            decision_list['billing'].append(decision_billing)
        # print(decision_list)
        return decision_list

    def get_ppoz_box(self, shard):
        server_api_ppoz = CamundaAPI.CamundaAPI(shard)      # Инициализация камунд ППОЗ

    def post_arm_gmp(self, gmp_key=None, status=None):
        self.logger.put_msg(f'Class: {__name__}.{self.post_arm_gmp.__name__} start POST method', 'info')
        return_value = None
        request_post = requests.post(config.post_arm_gmp, data={'businessKey': gmp_key, 'status': status})
        if request_post.status_code == 200:
            return_value = request_post.reason
        elif request_post.status_code != 200:
            self.logger.put_msg('\tServer {} answer: {} {}'
                                .format(config.post_arm_gmp, request_post.status_code,
                                        request_post.content.decode('utf-8')), 'error')
            return_value = f"{request_post.status_code}"
        self.logger.put_msg(f'Result:{request_post.reason}', 'info')
        self.logger.put_msg(f'Class: {__name__}.{self.post_arm_gmp.__name__} finish POST method', 'info')
        return return_value

    def repair_gmp_status(self, in_bk=None, in_file=None):
        """

        :param in_bk:
        :param in_file:
        :return:
        """
        check_plus = ['OK_PRE', 'OK_0', 'OK']

        key_list1 = ['PKPVDMFC-2018-08-16-017188', 'PKPVDMFC-2018-08-16-010828', 'PKPVDMFC-2018-08-16-009927',
                     'PKPVDMFC-2018-08-16-009903', 'PKPVDMFC-2018-08-15-016541', 'PKPVDMFC-2018-08-15-011969',
                     'PKPVDMFC-2018-08-15-010042', 'PKPVDMFC-2018-08-15-007226', 'PKPVDMFC-2018-08-15-006824',
                     'PKPVDMFC-2018-08-15-006805', 'PKPVDMFC-2018-08-15-006448']
        key_list2 = ['PKPVDMFC-2018-08-15-006805']
        query1 = {'state': {'$exists': False},
                  'status': {'$in': ['awaitingPayment', 'timeouted']},
                  'lastModifiedAt': {'$lte': datetime(2018, 10, 10, 00, 00)},
                  'lastModifiedAt': {'$gte': datetime(2018, 1, 1, 00, 00)}}
        query2 = {'_id': {'$in': key_list2}}

        server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
        request_cur = server_request.get_query(query=query2,
                                               limit=100)
        result_list = LogForever.LogForever(self.repair_gmp_status.__name__, 'i')
        result_error = LogForever.LogForever(self.repair_gmp_status.__name__ + '_error', 'i')
        # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]  # Инициализация камунд ППОЗ
        server_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')

        count_list = 0
        for i in request_cur:  # список БК
            req_result = server_request.get_request(i['_id'])
            # print(req_result)
            if req_result.get('status') in config.terminal_status + config.suspended_status \
                    and req_result.get('state') in ['finalized', None]:
                # print(f"log")
                continue
            gmp_result = server_gmp.get_gmp_request(in_bk=req_result['_id'])
            result_coll = self.collector_amount(gmp_result)                     # анализ запросов и начислений
            flag_result = True

            if all(elem in ['paid'] for elem in result_coll.get('statusGmp')):  # у всех запросов статус paid
                shard_index = config.shard_ppoz_name.index(i['bpmNodeId']['PPOZ'])  # server ppoz api
                for gmp_key in result_coll.get('_id'):
                    ans = self.post_arm_gmp(gmp_key=gmp_key, status='paid')
                    print(ans)
                    if str(ans) == '500' and result_coll.get('subscriptions') is None:
                        print('Пора добавлять subscriptions')
                    elif str(ans) == '500' and result_coll.get('subscriptions'):
                        pass
            else:                                                               # НЕ у всех запросов статус paid
                if i.get('requestType') == '111300001000' \
                        and i.get('status') == 'awaitingPayment':               # заявление и статус
                    for z in result_coll.get('billing'):            # если по оплате все удовлетворяет условиям
                        if all(elem in check_plus for elem in z.get('result', 'None')) is True:
                            pass
                        else:
                            flag_result = False
                pass

            if flag_result is True:
                result_list.put_msg(f"{i['_id']}\t{i['status']}\t"
                                    f"{i['requestType']}\t"
                                    f"flag:{flag_result}\t"
                                    f"{result_coll}", 'scr')
            else:
                result_error.put_msg(f"{i['_id']}\t{i['status']}\t"
                                     f"{i['requestType']}\t"
                                     f"flag:{flag_result}\t"
                                     f"{result_coll}", 'scr')
            count_list += 1
            # print(count_list, i, flag_result, result_coll)
        try:
            pass
        finally:
            server_request.client_close()
            server_gmp.client_close()
