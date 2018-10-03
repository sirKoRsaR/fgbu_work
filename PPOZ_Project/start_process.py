import classes.ChatRoom as chat_room
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys

param_list = {'MongoDB_req': ''}

if __name__ == '__main__':
    args = sys.argv

    chat_room.get_initial_param(param_list)
    if param_list['MongoDB_req'] == 'test':
        server_api_ppoz = [CamundaAPI.CamundaAPI(config.camunda_shard[i]) for i in range(len(config.shard_ppoz_num))]
        server_api_ppoz.api_req_starter()
    elif param_list['MongoDB_req']:
        request_in_mongodb = MongoRequest.MongoRequest(param_list)
        request_in_mongodb.mongo_req_starter()

    chat_room.get_final()

    # print(param_list)