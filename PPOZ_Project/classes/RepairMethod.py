import classes.LogForever as LogForever
import classes.MongoRequest as MongoRequest
import config
import datetime


class RepairMethod(object):

    def __init__(self):
        pass

    def medicine_gmp_status(self, in_bk=None, in_file=None):
        """

        :param in_bk:
        :param in_file:
        :return:
        """

        # in_bk = ['PKPVDMFC-2018-08-10-017885']

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
                                    'chargePaymentStatus': [], 'amount': []}
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
                    decision_billing['result'].append(None)
                    if billing.get('gmpStatus') in ['chargeImport']:
                        if billing.get('charge').get('amount') == 0:
                            decision_billing['result'][count] = 'OK_0'
                            # импорт начислений, но сумма 0 - ОК
                        elif billing.get('charge').get('amount') != 0:
                            decision_billing['result'][count] = '?'
                            # импорт начислений, но сумма не 0 - ??
                    elif billing.get('gmpStatus') in ['chargeCanceled']:
                        if billing.get('charge').get('amount') == 0:
                            decision_billing['result'][count] = 'OK_0'
                            # Истек срок ожидания, но сумма 0
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

        result_list = LogForever.LogForever(self.medicine_gmp_status.__name__, 'i')
        result_error = LogForever.LogForever(self.medicine_gmp_status.__name__ + '_error', 'i')
        # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]  # Инициализация камунд ППОЗ
        server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
        server_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')

        if in_bk is not None:
            bk_list = in_bk
        elif in_bk is None and in_file is not None:
            pass  # Обработка файла в список
        else:
            pass  # исключения
        # charged_amount_bk =[]
        for i in bk_list:  # список БК
            req_result = server_request.get_request(i)
            # print(req_result)
            if req_result.get('status') in config.terminal_status + config.suspended_status \
                    and req_result.get('state') in ['finalized', None]:
                # print(f"log")
                continue
            gmp_result = server_gmp.get_gmp_request(in_bk=req_result['_id'])
            result_coll = collector_amount(gmp_result)
            print(i, result_coll)

        try:
            pass
        finally:
            server_request.client_close()
