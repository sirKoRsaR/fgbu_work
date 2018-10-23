from datetime import datetime, timedelta
import config
import re
import threading
import classes.MongoRequest as MongoRequest
import classes.RepairMethod as RepairMethod
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
                                # f"{i['billingInfo'][0]['errorMessage']['error']}")
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


def task04_get_instance_on_gmp():
    """

    :return:
    """
    result_list = LogForever.LogForever(task04_get_instance_on_gmp.__name__, 'i')
    result_error = LogForever.LogForever(task04_get_instance_on_gmp.__name__ + '_error', 'i')

    server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]      # Инициализация камунд ППОЗ
    server_mongo_request = MongoRequest.MongoRequest('rrpdb', 'requests')           # Инициализация монги
    api_result = {}
    for server_ppoz in server_api_ppoz:             # сервера
        api_result[server_ppoz] = server_ppoz.get_activity_process(in_activity=['call_ppoz_gmp'], return_type='count')
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


def task05_not_ans_gmp_to_ppoz():
    """
    Получаем все обращения из ППОЗ со статусом awaitingPayment и добавляем коробки в ППОЗ,
    инфу из Монги по gmpRequest
    :return:
    """
    in_bk = 'PKPVDMFC-2018-07-09-013075'
    gmp_projection = {
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
            }

    result_list = LogForever.LogForever(task05_not_ans_gmp_to_ppoz.__name__, 'i')
    result_error = LogForever.LogForever(task05_not_ans_gmp_to_ppoz.__name__ + '_error', 'i')
    server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]  # Инициализация камунд ППОЗ
    server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
    server_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')

    status_request = server_request.get_query({"status": "awaitingPayment"})
    for i in status_request:
        # print(i)
        try:
            i['_id']
        except KeyError:
            result_error.put_msg(f"None BK")
            continue
        if i.get('status') is None:
            result_error.put_msg(f"BK:{i.get('_id')}\tStatus:None\tgmpRequest:None")
            continue
        shard_index = config.shard_ppoz_name.index(i['bpmNodeId']['PPOZ'])
        api_result = server_api_ppoz[shard_index].get_box_api(i['_id'])
        box_ids = []
        for j in api_result:
            box_ids.append(j['definitionId'])
        box_names = server_api_ppoz[shard_index].get_box_by_definition_id(box_ids)
        status_gmp = server_gmp.get_query({'billingInfo.extRequestIds': i['_id']}, gmp_projection)
        gmp_result = {'_id': [], 'receivedDate': [], 'expireDate': [], 'status': [], 'billingInfo': [],
                      'lastUpdated': []}
        for n in status_gmp:
            gmp_result['_id'].append(n['_id'])
            gmp_result['status'].append(n['status'])
            gmp_result['receivedDate'].append(n['receivedDate'])
            gmp_result['expireDate'].append(n['expireDate'])
            gmp_result['billingInfo'].append(n['billingInfo'])
            try:
                gmp_result['lastUpdated'].append(n['lastUpdated'])
            except KeyError:
                pass
        if status_gmp is None:
            result_error.put_msg(f"BK:{i['_id']}\tStatus:{i['status']}\tgmpRequest:{i['gmpServiceRequestNumber']}\tNone")
            continue
        result_list.put_msg(f"BK:{i['_id']}\tStatus:{i['status']}\tgmpRequest:{gmp_result['_id']}\t"
                            f"State:{i.get('state')}\trequestType:{i.get('requestType')}\t"
                            f"Status_gmp:{gmp_result['status']}\tOn_ppoz_box:{box_names}\t"
                            f"lastUpdated:{gmp_result['lastUpdated']}")
        status_flag = True
        # print(gmp_result)
        for m in gmp_result['status']:
            if m not in ['paid', 'timeouted']:
                status_flag = False
        if status_flag is True:
            for mm in range(len(gmp_result['_id'])):
                print(gmp_result['_id'][mm], gmp_result['status'][mm])


def task_medicine_gmp_status():
    task_runner = RepairMethod.RepairMethod()
    server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги

    request_cur = server_request.get_query(in_query={'status': 'awaitingPayment', 'requestType': '111300001000'},
                                           in_limit=100000)
    keys_list = []
    for bk in request_cur:
        keys_list.append(bk['_id'])
    # print(keys_list)
    task_runner.medicine_gmp_status(in_bk=keys_list)




def test_threading():
    import concurrent.futures as futures
    server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]

    with futures.ThreadPoolExecutor(1) as executor:
        # api_result = []
        #for i in server_api_ppoz:
        api_result0 = executor.submit(server_api_ppoz[0].test_threading())
        api_result1 = executor.submit(server_api_ppoz[1].test_threading())
        futures.wait(api_result0, api_result1)
        print(api_result0)


def test_get_instance():
    bk = ['PKPVDMFC-2018-10-16-070420', 'US-2018-08-14-125540', 'PKPVDMFC-2018-10-04-115340',
          'PKPVDMFC-2018-09-21-093762', 'PKPVDMFC-2018-10-19-097194', 'PKPVDMFC-2018-10-18-138570']
    server_api05 = CamundaAPI.CamundaAPI(config.camunda_shard[2])
    print(server_api05.serverName)
    for i in bk:
        print(server_api05.get_process_instance(i))


    #     f1 = executor.submit(someClass.doSomething)
    #     f2 = executor.submit(someClass.doSomethingElse)
    #     futures.wait((f1, f2))
    #     f3 = executor.submit(someClass.doSomethingElser, f1.result(), f2.result())
    #     result = f3.result()
    # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]
    # # print(len(config.shard_ppoz_name))
    # threads0 = threading.Thread(name=config.camunda_shard[0],
    #                             target=server_api_ppoz[0].get_list_act_box_from_shard)
    # threads1 = threading.Thread(name=config.camunda_shard[1],
    #                             target=server_api_ppoz[1].get_list_act_box_from_shard)
    # api_result0 = threads0.start()
    # api_result1 = threads1.start()
    # print("All threads are started")

    # t = threading.Thread(target=BankAccount.execute_virement, args=(my_account, 5000, 'Customer %d' % (num_thread,)))
    # t.start()







    # for bk_count in in_bk:
    #     bd_request = server_request.get_request(in_bk=bk_count)                     # сведения монго.реквест
    #     print(bd_request)
    #     if bd_request is None:
    #         result_error.put_msg(f"Error: Not find {bk_count} in MongoDB.requests")
    #         continue
    #     if bd_request['gmpServiceRequestNumber'] is None:
    #         result_error.put_msg(f"Error: Not find gmpServiceRequestNumber for {bk_count} in MongoDB.requests")
    #         continue
    #     bd_gmp = server_gmp.get_gmp_request(bd_request['gmpServiceRequestNumber'])  # сведения монго.гмпРеквест
    #     if bd_gmp is None:
    #         result_error.put_msg(f"Error: Not find data un MongoDB.gmpRequest for {bk_count}")
    #         continue
    #     print(bd_gmp)
    #     if bd_gmp['status'] in ['timeouted', 'awaitingPayment'] \
    #             and bd_request in ['timeouted', 'awaitingPayment'] \
    #             and bd_gmp['expireDate'] < (datetime.today() - timedelta(days=1)):  # если статус таймаут и дата вышла
    #         shard_index = config.shard_ppoz_name.index(bd_request['bpmNodeId']['PPOZ'])
    #         api_result = server_api_ppoz[shard_index].\
    #             get_box_api(in_key=bd_request['_id'])                               # получаем инстансы камунды
    #         print(api_result)
    #         box_ids = []
    #         for i in api_result:
    #             box_ids.append(i['definitionId'])                                   # собрали все бизнес-ид
    #         box_names = server_api_ppoz[shard_index].\
    #             get_box_by_definition_id(box_ids)                                   # перевели в бизнес-имя
    #         if 'ppoz_gmp' in box_names:                                             # если есть ppoz_gmp
    #             # server_api_ppoz[shard_index].restart_box(in_activity='ppoz_gmp', in_instance=)
    #             # 5ceb7f10-a20a-11e8-96c6-fa163e92a0fc
    #             print(bd_request['_id'])
    #         # print(bd_request)
            # print(bd_gmp)
            # print(api_result)

    # try:
    #     pass
    # finally:
    #     server_request.client_close()
    #     server_gmp.client_close()


  # if param_list['MongoDB_req'] == 'test':
    #     server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_num))]
    #     server_api_ppoz.api_req_starter()
    # elif param_list['MongoDB_req']:
    #     request_in_mongodb = MongoRequest.MongoRequest(param_list)
    #     request_in_mongodb.mongo_req_starter()
    # chat_room.get_final()

    # server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)  # Инициализация камунды ГМП
    # server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]
    # server_api_shard06 = CamundaAPI.CamundaAPI(config.camunda_shard[6])
    # server_mongo = MongoRequest.MongoRequest('rrgmp', 'gmpRequest') # Инициализация монги

    # spisok_gmp_box = ['timerPaymentStatusRetry',    #таймер проверки оплаты
    #                   'taskCheckPaymentStatus'      #проверка оплаты
    #                   ]
    # spisok_shard_box = [#'notificationSettingsTask',
    #                     #'kuvdFixation',
    #                     #'senderSystem',
    #                     'infoMessage']
    # return_type = 'count'
    # spisok_variable = ['incident']
    # result_api_shard = []

    # for i in server_api_ppoz:
    #     result_api_shard = result_api_shard + [i.get_activity_process(spisok_shard_box, return_type, spisok_variable)]

    # mm = server_api_shard06.get_activity_process(spisok_shard_box, return_type='json', in_variables=None)
    # print(mm)

    # выод списка инцидентов по камунде гмп
    # gmp_inc = server_api_gmp.get_activity_process(spisok_gmp_box, return_type='count')
    # #print(gmp_inc)
    #
    # for nn in gmp_inc:
    #     print(nn)
    #     print(gmp_inc[nn]['count'])
    #   #  for mm in gmp_inc[nn]:
    #         # if mm['incidentMessage'] == 'Ошибка СМЭВ: SMEV-1: Внутренняя ошибка сервиса':
    #         #     server_api_gmp.restart_box(mm['activityId'], mm['processInstanceId'])
    #         #     print(mm['activityId'], mm['processInstanceId'])
    #         #print(mm)
    #         #pass
    #     print(len(gmp_inc[nn]))

    # todo вынести в конфиг
    # Лечение по коробкам
    # if same_debug:
    # result_api_shard06 = server_api_shard06.get_activity_process(in_activity=None,  return_type='json',
    #                                                              in_variables=None)
    # for ii in result_api_shard06:
    #     for jj in result_api_shard06[ii]:
    #         print(jj['id'], ii, jj['incident'])
    #             # server_api_shard06.restart_box(ii, jj['id'])

    # Вывести список - шард - коробка - кол-во
    # for nn in range(len(result_api_shard)):
    #     print(config.shard_ppoz_name[nn], result_api_shard[nn])

    # result_api_gmp = server_api_gmp.get_activity_process(spisok_gmp_box, return_type, spisok_variable)
    # for ii in result_api_gmp:
    #     for jj in result_api_gmp[ii]:
    #         # print(jj['id'], jj['incident'], ii)
    #         if jj['id'] == 'ef991b59-c22f-11e8-8cb0-fa163eb984f4' and not jj['incident']:
    #             print(jj['id'], jj['incident'], ii)
    #             server_api_gmp.restart_box(ii, jj['id'])

    # count2 = 0
    # count = 0
    # if return_type == 'count':
    #     for i in result_api_gmp:
    #         print('Кол-во процессов на ' + i + ':' + str(result_api_gmp[i]['count']))
    # elif return_type == 'json':
    #     for i in result_api_gmp:
    #         print('\tСчитываем сведения для ' + i)
    #         count = 0
    #         for j in result_api_gmp[i]:
    #             # print(j['businessKey'])
    #             # print(server_mongo.get_info_mongo_gmp(j['businessKey']))
    #             count += 1
    #             if count % 1000 == 0:
    #                 print('\t\tОбработано ' + str(count) + ' bk')
    #
    #             print(j)
    #         print('Кол-во процессов на ' + i + ':' + str(count))
    #         count2 = count2 + count
    #     print('Всего процессов:' + str(count2))
