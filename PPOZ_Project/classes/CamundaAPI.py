from typing import Dict, Any

import requests
import json
import datetime
import classes.LogForever as LogForever
from colorama import Fore, Style
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class CamundaAPI(object):
    # Реконект при недоступности сервера (в том числе превышение лимита подключений) с задержкой в пол секунды
    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    def __init__(self, server_name):
        self.serverName = server_name
        self.logger = LogForever.LogForever(self.serverName[7:-5])
        self.logger.put_msg(f'Server {self.serverName} initialize', 'info')

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
        req_get_data = self.session.get(request_str)
        if req_get_data.status_code != 200:
            self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                   req_get_data.status_code,
                                                                   req_get_data.content.decode('utf-8')),
                                'error')
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        return api_result

    def restart_box(self, in_activity=None, in_instance=None):
        if in_activity is None or in_instance is None:
            self.logger.put_msg(f'NOT RESTART BOX: activity (box) or instance (key) is empty', 'info')
            return
        # self.logger.put_msg(f'\tRestart instance = {in_instance} on BOX = {in_activity}')
        headers = {'Content-Type': 'application/json'}
        restart_body = {
            "skipCustomListeners": False,
            "skipIoMappings": True,
            "instructions": [{"type": "cancel",                 "activityId": in_activity},
                             {"type": "startBeforeActivity",    "activityId": in_activity,
                              "variables": {"rep": {"value": "retry"}}
                              }
                             ]}
        request_str = self.get_request_string('/process-instance/' + in_instance + '/modification')
        r = requests.post(request_str, json=restart_body, headers=headers)
        self.logger.put_msg(f'\tRestart instance = {in_instance} on BOX = {in_activity} is {r}')

    def get_api_variable(self, in_json, in_variable):
        """

        Метод принимает json с данными и добавляет для каждой ветки данные по переменным

        :param in_json: json список процессов
        :param in_variable: обычный список парамметров для получения
        :return: в исходный json список добавляются json с данными параметрами
        """
        if in_json is None or in_variable is None:
            self.logger.put_msg(f'NOT READ VARIABLES: Json or list of variables is empty', 'info')
            return
        self.logger.put_msg(f'\tStart read Variables')
        for i in in_json:
            for j in in_variable:
                if j != 'incident':
                    request_str = self.get_request_string('/process-instance/' + i['id'] + '/variables/' + j)
                    req_get_data = self.session.get(request_str)
                    if req_get_data.status_code == 200:
                        api_result = json.loads(req_get_data.content.decode('utf-8'))
                        i[j] = api_result
                    elif req_get_data.status_code != 200:
                        self.logger.put_msg('\tServer {} answer: {} {}'
                                            .format(self.serverName, req_get_data.status_code,
                                                    req_get_data.content.decode('utf-8')), 'error')
                    req_get_data.close()
                elif j == 'incident':
                    request_str = self.get_request_string('/incident?processInstanceId=' + i['id'])
                    req_get_data = self.session.get(request_str)
                    if req_get_data.status_code == 200:
                        api_result = json.loads(req_get_data.content.decode('utf-8'))
                        i[j] = api_result
                    elif req_get_data.status_code != 200:
                        self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                               req_get_data.status_code,
                                                                               req_get_data.content.decode('utf-8')),
                                            'error')
                    req_get_data.close()
        self.logger.put_msg(f'\tFinish read Variables')

    def get_inst_on_activity(self, in_activity):
        """

        Функция получения json со всеми истансами по activityID (коробке)

        :param in_activity: имя коробки
        :return: json со всеми инстансами на коробке
        """
        request_str = self.get_request_string('/process-instance?activityIdIn=' + in_activity)
        req_get_data = self.session.get(request_str)
        if req_get_data.status_code != 200:
            self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                   req_get_data.status_code,
                                                                   req_get_data.content.decode('utf-8')),
                                'error')
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        req_get_data.close()
        return api_result

    def get_count_on_activity(self, in_activity):
        """

        Функция получения json с кол-ом инстансов по activityID (коробке)

        :param in_activity: имя коробки
        :return: jsin с количеством инстансов на коробке
        """
        request_str = self.get_request_string('/process-instance/count?activityIdIn=' + in_activity)
        req_get_data = self.session.get(request_str)
        if req_get_data.status_code != 200:
            self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                   req_get_data.status_code,
                                                                   req_get_data.content.decode('utf-8')),
                                'error')
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        req_get_data.close()
        return api_result

    def get_activity_process(self, in_activity=None, return_type='count', in_variables=None):
        """

        Функция возвращает json  с данными по коробкам

        :param in_variables:
        :param in_activity: список имен коробок
        :param return_type: count - счетчик; json - данные
        :return: Список Коробка:Данные
        """
        self.logger.put_msg(f'Class: {__name__}', 'info')
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'f'Начали получать сведения из {self.serverName}: ')
        api_result_array = {}
        for i in range(len(in_activity)):
            self.logger.put_msg(f'Read box {in_activity[i]} start', 'info')
            print(f'{Fore.LIGHTBLUE_EX}{Style.DIM}'
                  f'\tСчитываем коробку {in_activity[i]} ...')
            if return_type == 'json':
                api_result = self.get_inst_on_activity(in_activity[i])
                self.logger.put_msg(f'\tCount activity on box: {len(api_result)}', 'info')
                if in_variables:  # добавляем значения по переменным
                    self.get_api_variable(api_result, in_variables)
                api_result_array[in_activity[i]] = api_result
            elif return_type == 'count':
                api_result = self.get_count_on_activity(in_activity[i])
                api_result_array[in_activity[i]] = api_result
                self.logger.put_msg(f'\tCount activity on box: {api_result["count"]}', 'info')
            else:
                api_result_array = None
            self.logger.put_msg(f'Read box {in_activity[i]} finish', 'info')
        return api_result_array

    def get_incident_process(self, in_activity=None, return_type='count', in_variables=None):
        """

        Функция возвращает json  с данными с инцидентами (если коробка не задана, то возвращает все инцеденты)

        :param in_variables: не используется
        :param in_activity: список имен коробок, если пусто, то все инциденты
        :param return_type: count - счетчик; json - данные
        :return: json Список Коробка:инциденты (счетчик)
        """
        self.logger.put_msg(f'Class: {__name__}', 'info')
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'f'Начали получать сведения из {self.serverName}: ')
        api_result_array = {}
        if in_activity is None:
            if return_type == 'json':
                request_str = self.get_request_string('/incident')
            elif return_type == 'count':
                request_str = self.get_request_string('/incident/count')
            req_get_data = self.session.get(request_str)
            if req_get_data.status_code != 200:
                self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                       req_get_data.status_code,
                                                                       req_get_data.content.decode('utf-8')),
                                    'error')
            api_result = json.loads(req_get_data.content.decode('utf-8'))
            req_get_data.close()
            api_result_array = api_result
            if return_type == 'json':
                self.logger.put_msg(f'\tCount incident (all): {len(api_result)}', 'info')
            elif return_type == 'count':
                self.logger.put_msg(f'\tCount incident (all): {api_result["count"]}', 'info')
        else:
            for i in range(len(in_activity)):
                self.logger.put_msg(f'Read box {in_activity[i]} start', 'info')
                print(f'{Fore.LIGHTBLUE_EX}{Style.DIM}'
                      f'\tСчитываем коробку {in_activity[i]} ...')
                if return_type == 'json':
                    request_str = self.get_request_string('/incident?activityId=' + in_activity)
                elif return_type == 'count':
                    request_str = self.get_request_string('/incident/count?activityId=' + in_activity)
                req_get_data = self.session.get(request_str)
                if req_get_data.status_code != 200:
                    self.logger.put_msg('\tServer {} answer: {} {}'.format(self.serverName,
                                                                           req_get_data.status_code,
                                                                           req_get_data.content.decode('utf-8')),
                                        'error')
                api_result = json.loads(req_get_data.content.decode('utf-8'))
                req_get_data.close()
                api_result_array[in_activity[i]] = api_result
                if return_type == 'json':
                    self.logger.put_msg(f'\tCount incident on box: {len(api_result)}', 'info')
                elif return_type == 'count':
                    self.logger.put_msg(f'\tCount incident on box: {api_result["count"]}', 'info')
                self.logger.put_msg(f'Read box {in_activity[i]} finish', 'info')
        return api_result_array

    def get_cur_inst_time(self, iid):
        """
        Функция принимает айди процесса и возращает время входа процеса в коробку. Результат возвращается если время
        входа в коробку < текущего времени на указаное в переменной td.

        :param iid: айди процесса
        :return: спсиок содержащий бизнес кей + время начала выполнения коробки
        """
        # logger = logging.getLogger('incidents_stat.camunda_report.get_cur_inst_time')
        ct = datetime.datetime.today()
        td = datetime.timedelta(hours=24)
        request_str = self.get_request_string('/history/process-instance/' + iid)
        req_get_data = self.session.get(request_str)
        ret = {}
        if req_get_data.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                  req_get_data.content.decode('utf-8')))
            # logger.error('Server {} answer: {} {}'.format(uri, r.status_code, r.content.decode('utf-8')))
        else:
            result = json.loads(req_get_data.content.decode('utf-8'))
            if result:
                it = datetime.datetime.strptime(result['startTime'], '%Y-%m-%dT%H:%M:%S')
                # logger.debug('Время инстанса - ' + str(it))
                if (ct - it) > td:
                    ret[result['businessKey']] = result['startTime']
                    # logger.debug('Объект времени - ' + str(ret))
                    req_get_data.close()
                return ret
        req_get_data.close()  # Закрываем сессию
