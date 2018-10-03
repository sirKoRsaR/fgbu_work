import requests
import json


class CamundaAPI:

    def __init__(self, server_name):
        self.serverName = server_name

    def api_req_starter(self):
        self.get_box_api()

    def get_request_string(self, get_api_method):
        return self.serverName + '/engine-rest/engine/default' + get_api_method

    def get_server_name(self):
        return self.serverName

    # def get_list_act_box_from_shard(self):
    # TODO сборщик актуальных коробок в камундах

    @staticmethod
    def get_json_element(in_json, in_find_element):
        for entry in in_json:
            if entry['definitionId'].find(in_find_element) == -1:
                return json.loads('{"id": "None"}')
            else:
                return json.loads('{ "id": "' + entry['id'] + '", "definitionId": "' + entry['definitionId'] + '"}')

    def get_box_api(self, in_key):
        request_str = self.get_request_string('/process-instance?businessKey=' + in_key)
        req_get_data = requests.get(request_str)
        if req_get_data.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                  req_get_data.content.decode('utf-8')))
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        return api_result
