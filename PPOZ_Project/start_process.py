import classes.ChatRoom as chat_room
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys
import logging

param_list = {'MongoDB_req': ''}

if __name__ == '__main__':
    args = sys.argv

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, filename='api_gmp_mongo.log')

    # chat_room.get_initial_param(param_list)
    # if param_list['MongoDB_req'] == 'test':
    #     server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_num))]
    #     server_api_ppoz.api_req_starter()
    # elif param_list['MongoDB_req']:
    #     request_in_mongodb = MongoRequest.MongoRequest(param_list)
    #     request_in_mongodb.mongo_req_starter()
    # chat_room.get_final()
    return_type = 'count'

    server_api_gmp = CamundaAPI.CamundaAPI(config.camunda_gmp)      # Инициализация камунды ГМП
    server_mongo = MongoRequest.MongoRequest('rrpdb', 'requests')   # Инициализация монги
    result_api_gmp = server_api_gmp.get_camunda_process_on_activity([ 'exportPayments'
                                                                     # 'taskCheckPaymentStatus'
                                                                     # 'timerPaymentStatusRetry'
                                                                     ], return_type)
    #print(result_api_gmp)
    count = 0
    if return_type == 'count':
        for i in result_api_gmp:
            print('Кол-во процессов на ' + i + ':' + str(result_api_gmp[i]['count']))
    elif return_type == 'json':
        for i in result_api_gmp:
            for j in result_api_gmp[i]:
                # print(j['businessKey'])
                # print(server_mongo.get_info_mongo_gmp(j['businessKey']))
                count += 1
                # if count%1000 == 0:
                #     logging.debug('Обработано ', count)
                j['MongoDB_gmpRequest'] = server_mongo.get_info_mongo_gmp(j['businessKey'])

                # logging.info('BK: %s  , send post %s' % (n['id'], r))
                # print(j)


