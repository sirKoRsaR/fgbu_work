from datetime import datetime, timedelta
import config
import re
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import classes.LogForever as LogForever

def task01_gmp_ppoz_compare():
    delta = datetime.today() - timedelta(days=7)
    result_list = LogForever.LogForever(task01_gmp_ppoz_compare.__name__, 'i')  # Инициализация записи результата в лог
    result_error = LogForever.LogForever(task01_gmp_ppoz_compare.__name__ + '_error', 'i')
    server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)                  # Инициализация камунды ГМП
    server_mongo_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')         # Инициализация монги
    server_mongo_request = MongoRequest.MongoRequest('rrpdb', 'requests')       # Инициализация монги
    # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]  # Инициализация камунд ППОЗ

    spisok_gmp_box = ['timerPaymentStatusRetry',                                # Таймер проверки оплаты
                      'taskCheckPaymentStatus'                                  # Проверка оплаты
                      ]
                    #['ServiceTask_0iavb3s']
    result_list.put_msg(f"##########################################################################\n"
                        f"Обращения, у которых есть коробка в Api GMP,но статус в Mongo - processed \n"
                        f"и срок продления ожидания expireDate истек недлю назад\n "
                        f"##########################################################################\n"
                        f"GMP_id\tbusinessKey\tInstanse_Camunda_API\tGMP_BOX\texpireDate")
    api_result = server_api_gmp.get_activity_process(['ServiceTask_0iavb3s'], return_type='json', in_variables=None)
    for i in api_result:                    # Считываем каждую коробку, получаем список словарей (json) api_result[i]
        for j in api_result[i]:             # считываем каждый json в коробке, получаем словарь (json) j
            mongo_gmp = server_mongo_gmp.get_gmp_request(j['businessKey'])
            # j.update(temp)                  # вставили в словарь
            try:
                mongo_gmp['billingInfo'][0]['extRequestIds'][0]
            except IndexError:
                result_error.put_msg(f"Error: не найден billingInfo.extRequestIds для {mongo_gmp['_id']}\t"
                                     f"{j['id']}")
                continue
            mongo_req = server_mongo_request.get_request(mongo_gmp['billingInfo'][0]['extRequestIds'][0])
            # print(mongo_req['_id'], mongo_req['bpmNodeId']['PPOZ'])
            try:
                mongo_req['_id']
            except TypeError:
                result_error.put_msg(f"Error: не найден requests для {mongo_gmp['_id']}")
                continue
            try:
                mongo_gmp['status']
            except TypeError:
                result_error.put_msg(f"Error: не найден status в gmpRequest для {mongo_gmp['_id']}")
                continue
            try:
                mongo_req['status']
            except TypeError:
                result_error.put_msg(f"Error: не найден status в requests для {mongo_req['_id']}")
                continue
            if mongo_req['status'] == 'processed' and mongo_gmp['status'] == 'awaitingPayment':
                   # and mongo_gmp['expireDate'] < delta:
                # В апи гмп висит а обращение завершено и срок истек неделю назад
                result_list.put_msg(f"{mongo_gmp['_id']}\t{mongo_req['_id']}\t{j['id']}\t{i}\t"
                                    f"{mongo_gmp['expireDate']}")
                #print(mongo_req['_id'])
            j['MongoDB_gmp'] = mongo_gmp  # вставили в словарь с новым key
            j['MongoDB_request'] = mongo_gmp

        # print(api_result[i])

    # result_list.put_msg(f"'gmp_pid'\t'businessKey_gmp'\t"
    #                     f"'BK_id'\t"
    #                     f"'region'\t"
    #                     f"'status_request'\t"
    #                     f"'receivedDate'\t"
    #                     f"'expireDate'\t"
    #                     f"'gmpStatus'\t'"
    #                     f"'status'\t"
    #                     f"'supplierBillID'\t"
    #                     f"'Shard'\t")
    # for ii in api_result:
    #     for jj in api_result[ii]:
    #         result_list.put_msg(f"{jj['id']}\t{jj['businessKey']}\t"
    #                             f"{jj['MongoDB_request']['_id']}\t"
    #                             f"{jj['MongoDB_request']['region']}\t"
    #                             f"{jj['MongoDB_request']['status']}\t"
    #                             f"{jj['MongoDB_gmp']['receivedDate']}\t"
    #                             f"{jj['MongoDB_gmp']['expireDate']}\t"
    #                             f"{jj['MongoDB_gmp']['status']}\t"
    #                             f"{jj['MongoDB_gmp']['billingInfo'][0]['gmpStatus']}\t"
    #                             f"{jj['MongoDB_gmp']['billingInfo'][0]['charge']['supplierBillID']}\t"
    #                             f"{jj['MongoDB_request']['bpmNodeId']['PPOZ']}\t")
    try:
        pass
    finally:
        server_mongo_gmp.client_close()

def task02_gmp_error():
    """
    Задача: Поиск обращений в gmpRequest с ошибочным статусом и анализ
    :return:
    """
    result_list = LogForever.LogForever(task02_gmp_error.__name__, 'i')
    result_error = LogForever.LogForever(task02_gmp_error.__name__ + '_error', 'i')
    server_mongo_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')  # Инициализация монги
    server_mongo_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
    server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_name))]

    cursor_gmp_request = server_mongo_gmp.get_gmp_error()                  # Получаем обращения с ошибкой в gmpRequest
    count = 0
    for i in cursor_gmp_request:
        try:
            i['billingInfo'][0]['extRequestIds'][0]
        except IndexError:
            result_error.put_msg(f"Error: Not find billingInfo.extRequestIds for {i['_id']} in collection: gmpRequest")
            continue
        temp_json = server_mongo_request.get_request(i['billingInfo'][0]['extRequestIds'][0])   # json from requests
        try:
            temp_json['_id']
        except TypeError:
            result_error.put_msg(f"Error: Not find record for {i['billingInfo'][0]['extRequestIds'][0]} "
                                 f"in collection: requests")
            continue
        if temp_json['status'] in config.terminal_status:
            result_error.put_msg(f"Status for {temp_json['_id']} is '{temp_json.get('status')}' "
                                 f"and state is '{temp_json.get('state')}'")
        else:
            temp_api = server_api_ppoz[config.shard_ppoz_name.index(temp_json['bpmNodeId']['PPOZ'])].\
                get_box_api(temp_json['_id'])
            # print(temp_api)
            result_list.put_msg(f"{temp_json['_id']}\t{temp_json.get('status')}\t{temp_json.get('state')}\t"
                                f"{i['_id']}\t{temp_json['bpmNodeId']['PPOZ']}\t{temp_api}")
                                #f"{i['billingInfo'][0]['errorMessage']['error']}")
            i['API'] = temp_api
        i['Requests'] = temp_json
        # print(i)
        count += 1
    print(count)

    try:
        pass
    finally:
        server_mongo_gmp.client_close()
        server_mongo_request.client_close()


def task03_restart_inc_gmp():
    """
    Задача по подсчету инцов в коробках и при необходимосте - рестарту
    :return:
    """
    # result_list = LogForever.LogForever(task03_restart_inc_gmp.__name__, 'i')
    # result_error = LogForever.LogForever(task03_restart_inc_gmp.__name__ + '_error', 'i')
    server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)  # Инициализация камунды ГМП

    api_result = server_api_gmp.get_incident_process(return_type='json')
    print(f"Incidents:")
    for box in api_result:
        print(f"\t{box}: {len(api_result[box])}")
        for i in api_result[box]:
            print(f"\t\tprocessInstanceId: {i['processInstanceId']}, {i}")
            # if re.match('Нет данных о начислении с индексом', i['incidentMessage']):
            #     print(f"\t\tprocessInstanceId: {i['processInstanceId']}, {i}")
            #     server_api_gmp.restart_box(box, i['processInstanceId'])


def task04_get_instanse_on_gmp():
    """

    :return:
    """
    result_list = LogForever.LogForever(task04_get_instanse_on_gmp.__name__, 'i')
    result_error = LogForever.LogForever(task04_get_instanse_on_gmp.__name__ + '_error', 'i')

    server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]      # Инициализация камунд ППОЗ
    server_mongo_request = MongoRequest.MongoRequest('rrpdb', 'requests')           # Инициализация монги
    api_result = {}
    for server_ppoz in server_api_ppoz:             # сервера
        api_result[server_ppoz] = server_ppoz.get_activity_process(in_activity=['call_ppoz_gmp'], return_type='json')
        # print(api_result[server_ppoz])
        for i in api_result[server_ppoz]:           # коробки
            for j in api_result[server_ppoz][i]:    # json's
                try:
                    j.get('businessKey')
                except AttributeError:
                    result_error.put_msg(f"{j}")
                    continue
                mongo_requests = server_mongo_request.get_request(j['businessKey'])
                try:
                    mongo_requests.get
                except AttributeError:
                    result_error.put_msg(f"{j}")
                    continue
                result_list.put_msg(f"businessKey:{mongo_requests.get('_id', None)}\t"
                                    f"region:{mongo_requests.get('region', None)}\t"
                                    f"status:{mongo_requests.get('status', None)}\t"
                                    f"state:{mongo_requests.get('state', None)}\t"
                                    f"instance:{j.get('id', None)}")
                j['MongoDB_requests'] = mongo_requests
        # print(f"{config.camunda_shard[server_ppoz]}: {api_result}")

    try:
        pass
    finally:
        server_mongo_request.client_close()