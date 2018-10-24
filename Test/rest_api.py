import requests
import json
from multiprocessing import Pool
import logging

engine = '/engine-rest/engine/default'


def get_id(uri, key):
    """
    Get incidents
    :param uri: url to Camunda
    :param filter_: filter string in message
    :param activityId: activityId
    :param no_retry_activityId: 
    :return: incidents
    """
    # print('get job...')
    request_str = uri + engine + '/process-instance'
    request_str += '?businessKey=' + key
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        if not result:
            return None
        return result[0]['id']
    else:
        return None

def is_suspend(uri, pid):
    request_str = uri + engine + '/process-instance/'
    request_str += pid
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        return r.json()['suspended']
    else:
        return None

def get_id_by_aid(uri, aid):
    request_str = uri + engine + '/process-instance'
    request_str += '?activityIdIn=' + aid
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        return result
    else:
        return None


def get_id_last(uri, key):
    request_str = uri + engine + '/process-instance'
    request_str += '?businessKey=' + key
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        if result:
            return result[-1]['id']
        else:
            return None
    else:
        return None

def get_id_all(uri, key):
    request_str = uri + engine + '/process-instance'
    request_str += '?businessKey=' + key
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        if result:
            tmp = []
            for id in result:
                aid, name, atype, pid = get_activity(uri, id['id'], activityName=True, activityType=True, processIId=True)
                tmp.append([aid, name, atype, pid])
            return tmp
        else:
            return None
    else:
        return None

def check_inc(uri, id):
    """
    Get incidents
    :param uri: url to Camunda
    :param filter_: filter string in message
    :param activityId: activityId
    :param no_retry_activityId: 
    :return: incidents
    """
    # print('get job...')
    request_str = uri + engine
    request_str += '/incident?processInstanceId=' + id
    r = requests.get(request_str)
    # print('inc {} response {}'.format(id, r.content))
    if r.status_code == requests.codes.ok:
        if r.json() == []:
            return 0
        else:
            return 1
    else:
        return None


def get_activity(uri, id, activityName=False, activityType=False, processIId=False):
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
            return g['activityId'], g['activityName'],g['activityType'],g['processInstanceId']

    request_str = uri + engine + '/process-instance/'
    request_str += id + '/activity-instances'
    r = requests.get(request_str)
    if r.status_code == requests.codes.ok:
        result = json.loads(r.content.decode('utf-8'))
        aid, aidname, atype, pid = get(result)
        res = []
        res.append(aid)
        if activityName:
            res.append(aidname)
        if activityType:
            res.append(atype)
        if processIId:
            res.append(pid)
        if len(res) == 1:
            return res[0]
        else:
            return res
    else:
        return None


def modification(uri, pid, jsons):
    # print(pid)
    request_str = uri + engine + '/process-instance/' + pid + '/modification'
    r = requests.post(request_str, json=jsons)
    if r.status_code == requests.codes.ok or r.status_code == requests.codes.no_content:
        print('{} ok'.format(pid))
    else:
        print('{}. Text: {} {}'.format(pid, r.status_code, r.content.decode('utf-8')))


def get_job(uri, activityId=''):
    """
    Get incidents
    :param uri: url to Camunda
    :param filter_: filter string in message
    :param activityId: activityId
    :param no_retry_activityId: 
    :return: incidents
    """
    # print('get job...')
    request_str = uri + engine + '/job'
    if activityId:
        request_str += '?activityId=' + activityId
    r = requests.get(request_str)
    result = json.loads(r.content.decode('utf-8'))
    return result


def get_incidents(uri, filter_='', activityId='', no_retry_activityId=None):
    """
    Get incidents
    :param uri: url to Camunda
    :param filter_: filter string in message
    :param activityId: activityId
    :param no_retry_activityId: 
    :return: incidents
    """
    if no_retry_activityId is None:
        no_retry_activityId = []
    # print('get urls...')
    request_str = uri + engine + '/job?withException=true'
    if activityId:
        request_str += '&activityId=' + activityId
    r = requests.get(request_str)
    if r.status_code != 200:
        exit('Server {} answer: {} {}'.format(uri, r.status_code, r.content.decode('utf-8')))
    result = json.loads(r.content.decode('utf-8'))
    tmp2 = []
    if no_retry_activityId:
        for aid in no_retry_activityId:
            request_str = uri + engine + '/job?withException=true' + '&activityId=' + aid
            try:
                r = requests.get(request_str)
                if r.status_code != 200:
                    exit('Server {} answer: {} {}'.format(uri, r.status_code, r.content.decode('utf-8')))
                resultaid = json.loads(r.content.decode('utf-8'))
                if not resultaid: continue
            except:
                continue
            for id in resultaid:
                tmp2.append(id['id'])
    tmp = []
    # print('sorting...')
    for i in result:
        if i['id'] in tmp2: continue
        if not filter_:
            tmp.append(i)
        else:
            if str(i['exceptionMessage']).find(filter_) != -1:
                tmp.append(i)
    return tmp


def get_incidents_pid(uri, filter_='', activityId='', no_retry_activityId=None):
    res = get_incidents(uri, filter_, activityId, no_retry_activityId)
    return [i['processInstanceId'] for i in res]


def get_businessKey(uri, pid):
    request_str = uri + engine + '/process-instance?' + 'processInstanceIds=' + pid
    r = requests.get(request_str)
    if r.status_code != 200:
        exit('Server {} answer: {} {}'.format(uri, r.status_code, r.content.decode('utf-8')))
    result = json.loads(r.content.decode('utf-8'))
    return result[0]['businessKey']


def make_url_retry(uri, inc):
    result = [uri + engine + '/job/' + i['id'] + '/retries' for i in inc]
    return result


def make_url_modification(uri, pid):
    result = [uri + engine + '/process-instance/' + i + '/modification' for i in pid]
    return result


def cancel_id(uri, id):
    def add_signalName(uri, id):
        request_str = uri + engine + '/process-instance/' + id + '/variables/signalName'
        json = {"signalName": ""}
        r = requests.put(request_str, json=json)
        if r.status_code == requests.codes.no_content:
            logging.info('{} add signalName success'.format(id))
        else:
            logging.info('{} not add signalName :('.format(id))

    add_signalName(uri, id)
    request_str = uri + engine + '/process-instance/' + id
    # print(request_str)
    r = requests.delete(request_str)
    if r.status_code == requests.codes.no_content:
        logging.info('{} cancel'.format(id))
        return True
    else:
        logging.info('{} not cancel!\n {} {}'.format(id, r.status_code, r.content))
        return False


def retry_inc(url):
    r = requests.put(url, json={"retries": 1})
    print(r.status_code)


def __retry_inc_async(body, ids):
    # todo https://docs.camunda.org/manual/7.6/reference/rest/job/post-set-job-retries/ 7.6!!!
    end = 1
    if len(ids) > 100:
        end = 100
    elif len(ids) > 10:
        end = 5
    json = {
        'retries': 1,
        'jobIds': []
    }


def speed_retry(urls, speed=8):
    pool = Pool(speed)
    pool.map(retry_inc, urls)


if __name__ == '__main__':
    from pprint import pprint

    uri = 'http://ppoz-process-core-01.prod.egrn:9084/'
    inc = get_incidents(uri)
    for i in inc:
        pprint(i)
