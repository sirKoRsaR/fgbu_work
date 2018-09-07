#-------------------------------------------------------------------------------
# Name:        test_api01
# Purpose:
#
# Author:      EvstigneevKA
#
# Created:     29.08.2018
# Copyright:   (c) EvstigneevKA 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import json
import requests

#shard_num =["00", "01", "02", "03", "04","05", "06", "07", "08", "09"]
shard_ppoz = ["02", "04", "06", "08", "10", "12", "14", "16", "18", "20"]
camunda_shard = ['http://ppoz-process-core-' + i + '.prod.egrn:9084'
                    for i in shard_ppoz ]
""" список шард """

class CamundaApi:

    def __init__(self, serverName):
        self.serverName = serverName

    def get_request_string(self, getApiMethod):
        request_string = self.serverName
        request_string += '/engine-rest/engine/default'
        request_string += getApiMethod
        return request_string

    def get_process_definition(self, inMethod):
        #request_str = self.get_request_string('/process-definition')
        if inMethod == "stat":
            request_str = self.get_request_string('/statistics')
        elif inMethod in ('', 'all'):
            request_str = self.get_request_string('/process-definition')
        else:
            print('Неверно указан параметр вызова API')
        reqGetData = requests.get(request_str)
        if reqGetData.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, reqGetData.status_code, reqGetData.content.decode('utf-8')))
        return json.loads(reqGetData.content.decode('utf-8'))


    def get_process_definition(self, inMethod):
        #request_str = self.get_request_string('/process-definition')
        if inMethod == "stat":
            request_str = self.get_request_string('/process-definition/statistics')
        else:
            print('Неверно указан параметр вызова API')
        reqGetData = requests.get(request_str)
        if reqGetData.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, reqGetData.status_code, reqGetData.content.decode('utf-8')))
        return json.loads(reqGetData.content.decode('utf-8'))


    def get_inc_count_by_type (self):
        request_str = self.get_request_string('/process-definition/count')                # в доработку на отдельный метод
        print('Ведутся работы: ' + request_str)
        r = requests.get(request_str)
        if r.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, r.status_code, r.content.decode('utf-8')))
        result = json.loads(r.content.decode('utf-8'))
        print(result)

        #request_str = self.serverName + '/engine-rest/engine/default' + '/incident' #?incidentMessage'#=HTTP-307&?sortBy=incidentType&?sortOrder=asc'
        #print('Ведутся работы: ' + request_str)
        #r = requests.get(request_str)
        #result = json.loads(r.content.decode('utf-8'))
        #print(result[0:10])


        #for i in result:
        #    cou += 1
        #    print(i['id'])
        #print('общее количество - ' + str(cou))

if __name__ == '__main__':

    #server1 = CamundaApi()
    print(camunda_shard[9])

    serverApiPpoz = [CamundaApi(camunda_shard[i]) for i in range(len(shard_ppoz))]
    #result = serverApiPpoz[3].get_process_definition('')
    #print(len(serverApiPpoz))
    cou = 0
    for i in serverApiPpoz[3].get_process_definition('stat'):
        cou += 1
        print ("ID: " + i['id'] + " NAME: " + i['definition']['name'] )
        #print(i)

