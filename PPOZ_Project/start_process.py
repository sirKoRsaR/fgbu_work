import classes.ChatRoom as ChatRoom
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys
import classes.LogForever as LogForever

param_list = {'MongoDB_req': ''}


def task01_gmp_ppoz_compare():

    from datetime import datetime, timedelta

    delta = datetime.today() - timedelta(days=7)

    result_list = LogForever.LogForever(task01_gmp_ppoz_compare.__name__, 'i')                     # Инициализация записи результата в лог
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
    api_result = server_api_gmp.get_activity_process(spisok_gmp_box, return_type='json', in_variables=None)
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



if __name__ == '__main__':
    args = sys.argv
    logger = LogForever.LogForever('project')
    # ChatRoom.get_initial_param(param_list)

    task01_gmp_ppoz_compare()




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
