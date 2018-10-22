import classes.LogForever as LogForever
import classes.MongoRequest as MongoRequest
import config


class RepairMethod(object):

    def __init__(self):
        pass

    def medicine_gmp_status(self, in_bk=None, in_file=None):
        """

        :param in_bk:
        :param in_file:
        :return:
        """
        result_list = LogForever.LogForever(self.medicine_gmp_status.__name__, 'i')
        result_error = LogForever.LogForever(self.medicine_gmp_status.__name__ + '_error', 'i')
        # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]  # Инициализация камунд ППОЗ
        server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
        server_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')
        bk_list = ['Other-2018-10-12-026353']
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
            if not gmp_result:
                continue
            flag_gmp = True
            gmp_ids = []
            charged_amount = []  # выставленный счет
            pay_amount = []  # оплаченый счет
            flag_status = True
            charge_gmp_status = []
            for j in gmp_result:  # список платежек
                gmp_ids.append(j['_id'])
                if j['status'] == 'paid':
                    flag_gmp = True
                    for billing in j['billingInfo']:
                        try:
                            billing['charge']
                        except (AttributeError, KeyError):
                            flag_gmp = False
                            continue
                        charge_gmp_status.append(billing.get('gmpStatus'))
                        if billing.get('gmpStatus') == 'paid' or (billing.get('gmpStatus') == 'chargeImport'
                                                                  and billing.get('chargedAmount') == 0):
                            charged_amount.append(billing.get('chargedAmount'))
                            pay_amount.append(billing.get('charge').get('amount'))
                        elif billing.get('gmpStatus') != 'paid':
                            pass
                elif j['status'] != 'paid':
                    flag_gmp = False
            if flag_gmp is True and flag_status is True:
                result_list.put_msg(f"{req_result['_id']}\tgmp_ids:{gmp_ids}\t"
                                    f"charged_amount:{charged_amount}\tpay_amount:{pay_amount}\t"
                                    f"charge_gmp_status:{charge_gmp_status}\tflag_status:{flag_status}\t"
                                    f"flag_gmp:{flag_gmp}")
            elif flag_gmp is False:
                result_error.put_msg(f"{req_result['_id']}\tgmp_ids:{gmp_ids}\t"
                                     f"charged_amount:{charged_amount}\tpay_amount:{pay_amount}\t"
                                     f"charge_gmp_status:{charge_gmp_status}\tflag_status:{flag_status}\t"
                                     f"flag_gmp:{flag_gmp}")