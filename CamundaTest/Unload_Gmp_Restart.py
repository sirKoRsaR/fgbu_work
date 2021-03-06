import datetime
import json
import logging
import requests
import re
from bson import json_util
from pymongo import MongoClient

shard_ppoz = ['bpm', 'bpm_Shard_01', 'bpm_Shard_02', 'bpm_Shard_03', 'bpm_Shard_04', 'bpm_Shard_05',
              'bpm_Shard_06', 'bpm_Shard_07', 'bpm_Shard_08', 'bpm_Shard_09', 'error']
shard_ppoz_api = ["02", "04", "06", "08", "10", "12", "14", "16", "18", "20"]
camunda_shard = ['http://ppoz-process-core-' + i + '.prod.egrn:9084'
                 for i in shard_ppoz_api]
uri = '/engine-rest/engine/default/process-instance/'
engine = '/engine-rest/engine/default'
headers = {'Content-Type': 'application/json'}

reg_key = re.compile(
                '^(?:EPGU|MFC|OfSite|PKPVD|PKPVDMFC|Vedomstvo|BC|Other|US|Migr)-[0-9]{4}(?:-[0-9]{2}){2}-[0-9]{6}$')
box = 'taskCheckChargeCreationStatus'

restartBody ={"unfinished":True,"activityId":box,"startedAfter":"2018-09-05T00:00:00","startedBefore":"2018-09-09T11:00:00"}

restartBody_ = {
                "skipCustomListeners": False,
                "skipIoMappings": True,
                "instructions": [
                {
                "type": "cancel",
                "activityId": box
                },
                {
                    "type": "startBeforeActivity",
                    "activityId": box,
                    "variables":    {
                                     "rep": {"value": "retry"}
                                    }
                    }]
                }

def get_activity(uri, id, activityName=False, activityType=False, processIId=False, Id=False):
    """
    Get incidents
    :param uri: url to Camunda
    :param filter_: filter string in message
    :param activityId: activityId
    :param no_retry_activityId:
    :return: incidents
    """

    def get(g):
        if not g:
            return None
        if g['childActivityInstances']:
            return get(g['childActivityInstances'][0])
        else:
            return g['activityId'], g['activityName'],g['activityType'],g['processInstanceId'], g['id']

    request_str = uri + engine + '/process-instance/'
    request_str += id + '/activity-instances'
    r = session.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        aid, aidname, atype, pid, iid = get(result)
        res = []
        res.append(aid)
        if activityName:
            res.append(aidname)
        if activityType:
            res.append(atype)
        if processIId:
            res.append(pid)
        if Id:
            res.append(iid)
        if len(res) == 1:
            return res[0]
        else:
            return res
    else:
        return None

def post_api():
    request_str = url + engine + '/history/activity-instance/'
    r = requests.post(request_str, json=restartBody, headers=headers)
    result = json.loads(r.content.decode('utf-8'))
    return result

def load_id_from_file():
    #for i in shard_ppoz:
        file_name = 'log_gmp_' + 'bpm' + '.log'
        f = open(file_name, 'r')
        f_all = f.readlines()
        # for j in f_all:
            # print(j)

        f.close()

class CamundaApi:

    def __init__(self, serverName):
        self.serverName = serverName

    def get_request_string(self, getApiMethod):
        request_string = self.serverName
        request_string += '/engine-rest/engine/default'
        request_string += getApiMethod
        return request_string

    def get_name(self):
        return self.serverName

    def get_proc_definition_gmp(self):
        request_str = self.get_request_string('/process-definition?name=%D0%9F%D0%9F%D0%9E%D0%97:%20%D0%93%D0%9C%D0'
                                              '%9F:%20%D0%92%D0%B7%D0%B0%D0%B8%D0%BC%D0%BE%D0%B4%D0%B5%D0%B9%D1%81%D1'
                                              '%82%D0%B2%D0%B8%D0%B5%20%D1%81%20%D0%93%D0%98%D0%A1%20%D0%93%D0%9C%D0'
                                              '%9F&version=2')
        reqGetData = requests.get(request_str)
        if reqGetData.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, reqGetData.status_code,
                                                  reqGetData.content.decode('utf-8')))
        return json.loads(reqGetData.content.decode('utf-8'))

    def get_req_on_process_definition(self, in_id_def):
        request_str = self.get_request_string('/process-instance?processDefinitionId=' + in_id_def)
        reqGetData = requests.get(request_str)
        if reqGetData.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, reqGetData.status_code, reqGetData.content.decode('utf-8')))
        return json.loads(reqGetData.content.decode('utf-8'))


if __name__ == '__main__':

    level = logging.INFO
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=level, filename='restart_box_protocol.log')

    serverApiPpoz = [CamundaApi(camunda_shard[i]) for i in range(len(shard_ppoz_api))]
    result =[]
    count = 0
    load_id_from_file()
    # for j in range(len(shard_ppoz_api)):
    #     result = result + serverApiPpoz[j].get_proc_definition_gmp()
    #     # print(result[j]['id'])
    # for i in range(9): #range(len(shard_ppoz_api)):
    #     for n in serverApiPpoz[i].get_req_on_process_definition(result[i]['id']):
    #         count = count + 1
    #         if n['id'] == '0ee9ac43-c66d-11e8-9854-fa163e07674b':
    #             r = requests.post(serverApiPpoz[i].get_name() + uri +
    #                               n['id'] + '/modification', json=restartBody_, headers=headers)
    #             logging.info('BK: %s  , send post %s' % (n['id'], r))
    #             # print(n['id'], r)
    #         # else:
    #         #     logging.info('BK: %s  , send post %s' % (n['id'], 'Пропущена'))
    #         # print(r)


    #print(count)