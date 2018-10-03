import classes.ChatRoom as chat_room
import classes.MongoRequest as MongoRequest
import sys

param_list = {'MongoDB_req': ''}

if __name__ == '__main__':
    args = sys.argv

    chat_room.get_initial_param(param_list)
    if param_list['MongoDB_req']:
        request_in_mongodb = MongoRequest.MongoRequest(param_list)
        request_in_mongodb.mongo_req_starter()
    # print(param_list)