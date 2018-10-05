import classes.ChatRoom as chat_room
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys
import datetime
import logging


param_list = {'MongoDB_req': ''}

if __name__ == '__main__':
    args = sys.argv

    logger = logging.getLogger("incidents_stat")
    logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, filename='api_gmp_mongo.log')
    fh = logging.FileHandler("logFile.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # chat_room.get_initial_param(param_list)
    # if param_list['MongoDB_req'] == 'test':
    #     server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_num))]
    #     server_api_ppoz.api_req_starter()
    # elif param_list['MongoDB_req']:
    #     request_in_mongodb = MongoRequest.MongoRequest(param_list)
    #     request_in_mongodb.mongo_req_starter()
    # chat_room.get_final()
    return_type = 'json'

    server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)  # Инициализация камунды ГМП
    server_mongo = MongoRequest.MongoRequest('rrgmp', 'gmpRequest')  # Инициализация монги
    result_api_gmp = server_api_gmp.get_camunda_process_on_activity([ 'exportPayments'
                                                                     # 'taskCheckPaymentStatus',
                                                                     # 'timerPaymentStatusRetry'
                                                                     ], return_type)
    # print(result_api_gmp)
    count2 = 0
    count = 0
    if return_type == 'count':
        for i in result_api_gmp:
            print('Кол-во процессов на ' + i + ':' + str(result_api_gmp[i]['count']))
    elif return_type == 'json':
        print('Начали получать сведения из MongoDB')
        for i in result_api_gmp:
            print('\tСчитываем сведения для ' + i)
            count = 0
            for j in result_api_gmp[i]:
                # print(j['businessKey'])
                # print(server_mongo.get_info_mongo_gmp(j['businessKey']))
                count += 1
                if count % 1000 == 0:
                    print('\t\tОбработано ' + str(count) + ' bk')

                # j['MongoDB_gmpRequest'] = server_mongo.get_info_mongo_gmp(j['businessKey'])
                # logging.info('\t"businessKeyGMP": "{}", "id": "{}", '
                #              '"expireDate": "{}"'.
                #              format(j['businessKey'],
                #                     j['id'],
                #                     j['expireDate']['value']
                #                     # j['MongoDB_gmpRequest']['expireDate']
                #                     ))

                print(j)
            print('Кол-во процессов на ' + i + ':' + str(count))
            count2 = count2 + count
        print('Всего процессов:' + str(count2))
