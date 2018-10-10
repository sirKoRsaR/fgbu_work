import classes.ChatRoom as ChatRoom
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys
import classes.LogForever as LogForever

param_list = {'MongoDB_req': ''}


if __name__ == '__main__':
    args = sys.argv
    logger = LogForever.LogForever('project')
    ChatRoom.get_initial_param(param_list)
    # if param_list['MongoDB_req'] == 'test':
    #     server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_num))]
    #     server_api_ppoz.api_req_starter()
    # elif param_list['MongoDB_req']:
    #     request_in_mongodb = MongoRequest.MongoRequest(param_list)
    #     request_in_mongodb.mongo_req_starter()
    # chat_room.get_final()

    server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)  # Инициализация камунды ГМП
    server_api_ppoz = [CamundaAPI.CamundaAPI(i) for i in config.camunda_shard]
    server_api_shard06 = CamundaAPI.CamundaAPI(config.camunda_shard[6])
    # server_mongo = MongoRequest.MongoRequest('rrgmp', 'gmpRequest') # Инициализация монги

    spisok_gmp_box = ['taskCheckPaymentStatus']
    spisok_shard_box = ['notificationSettingsTask',
                        'kuvdFixation',
                        'senderSystem',
                        'infoMessage']
    return_type = 'count'
    spisok_variable = ['incident']
    result_api_shard = []

    # for i in server_api_ppoz:
    #     result_api_shard = result_api_shard + [i.get_activity_process(spisok_shard_box, return_type, spisok_variable)]

    # выод списка инцидентов по камунде гмп
    gmp_inc = server_api_gmp.get_incident_process(spisok_gmp_box, return_type='json')
    # print(gmp_inc)
    for nn in gmp_inc:
        for mm in gmp_inc[nn]:
            # if mm['incidentMessage'] == 'Ошибка СМЭВ: SMEV-1: Внутренняя ошибка сервиса':
            #     server_api_gmp.restart_box(mm['activityId'], mm['processInstanceId'])
            #     print(mm['activityId'], mm['processInstanceId'])
            print(mm)
            pass
        print(len(gmp_inc[nn]))

    # todo вынести в конфиг
    # Лечение по коробкам
    # if same_debub:
    #     result_api_shard06 = server_api_shard06.get_activity_process(spisok_shard_box, 'json', spisok_variable)
    #     for ii in result_api_shard06:
    #         for jj in result_api_shard06[ii]:
    #             print(jj['id'], ii, jj['incident'])
    #                 # server_api_shard06.restart_box(ii, jj['id'])

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
