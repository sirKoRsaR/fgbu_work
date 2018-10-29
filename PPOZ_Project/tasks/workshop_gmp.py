from datetime import datetime, timedelta
import config
import re
import threading
import classes.MongoRequest as MongoRequest
import classes.RepairMethod as RepairMethod
import classes.CamundaAPI as CamundaAPI
import classes.LogForever as LogForever


def task01_gmp_ppoz_compare():
    server_request = MongoRequest.MongoRequest('rrpdb', 'requests')  # Инициализация монги
    server_gmp = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')
    query1 = {'state': {'$exists': False},
              'status': {'$in': ['awaitingPayment', 'timeouted']},
              'lastModifiedAt': {'$lte': datetime(2018, 10, 10, 00, 00)},
              'lastModifiedAt': {'$gte': datetime(2018, 1, 1, 00, 00)}}
    query2 = {'_id': 'PKPVDMFC-2018-08-16-017188'}
    request_cur = server_request.get_query(query=query1, limit=20000)
    print('begin')
    for i in request_cur:
        key_gmp = i.get('gmpServiceRequestNumber', None)
        if key_gmp is not None:
            gmp_result1 = server_gmp.get_gmp_request(in_gmp=key_gmp)
            #gmp_result2 = server_gmp.get_gmp_request(in_bk=i.get('_id'))
            if gmp_result1.count() == 0:
                print(f"YES {i.get('_id')} {i.get('gmpServiceRequestNumber')} None")
            else:
                # for j in gmp_result1:
                #     print(f"NO {i.get('_id')} {i.get('gmpServiceRequestNumber')} {j.get('_id')}")
                pass
        else:
            #print(f"NO  {i.get('_id')} {i.get('gmpServiceRequestNumber')} None")
            pass
    try:
            pass
    finally:
        server_gmp.client_close()
        server_request.client_close()

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


def task_repair_gmp_status():
    init = RepairMethod.RepairMethod()
    init.repair_gmp_status()
    del init


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
    #
    # "receivedDate": {
    #     "$gte": datetime.datetime(2018, 9, 1, 00, 00),
    #     "$lte": datetime.datetime(2018, 9, 30, 23, 59)
    #     # "$gte": 'ISODate("2018-09-25T21:00:00.001+0000")',
    #     # "$lte": 'ISODate("2018-09-26T21:00:00.001+0000")'
    # }
    #


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
