import requests
import json
import datetime
from colorama import Fore, Style
import config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class CamundaAPI:
    # Реконект при недоступности сервера (в том числе превышение лимита подключений) с задержкой в пол секунды
    session = requests.Session()
    retry = Retry(connect=10, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

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
        req_get_data = self.session.get(request_str)
        if req_get_data.status_code != 200:
            exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                  req_get_data.content.decode('utf-8')))
        api_result = json.loads(req_get_data.content.decode('utf-8'))
        return api_result

    def get_api_variable(self, in_json, in_variable):
        """
        :param in_json: json список процессов
        :param in_variable: обычный список парамметров для получения
        :return: в исходный json список добавляются json с данными параметрами
        """
        if in_json is None or in_variable is None:
            return
        for i in in_json:
            for j in in_variable:
                request_str = self.get_request_string('/process-instance/' + i['id'] + '/variables/' + j)
                req_get_data = self.session.get(request_str)
                if req_get_data.status_code != 200:
                    pass
                    # exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                    #                                       req_get_data.content.decode('utf-8')))
                api_result = json.loads(req_get_data.content.decode('utf-8'))
                i[j] = api_result
                req_get_data.close()

    def get_camunda_process_on_activity(self, in_activity=None, return_type='count'):
        """
        Функция возвращает json  с данными по коробкам
        :param in_activity: список имен коробок
        :param return_type: count - счетчик; json - данные
        :return: Список Коробка:Данные
        """
        api_result_array = {}
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'Начали получать сведения из Камунды ГМП: ')
        for i in range(len(in_activity)):
            print(f'{Fore.LIGHTBLUE_EX}{Style.DIM}'
                  f'\tСчитываем коробку {in_activity[i]} ...')
            if return_type == 'json':
                request_str = self.get_request_string('/process-instance?activityIdIn=' + in_activity[i])
                req_get_data = self.session.get(request_str)
                if req_get_data.status_code != 200:
                    exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                          req_get_data.content.decode('utf-8')))
                api_result = json.loads(req_get_data.content.decode('utf-8'))
                add_variable = 'true'
                if add_variable == 'true':  # добавляем значения по переменным
                    self.get_api_variable(api_result, ['expireDate'])
                api_result_array[in_activity[i]] = api_result
                req_get_data.close()
            elif return_type == 'count':
                request_str = self.get_request_string('/process-instance/count?activityIdIn=' + in_activity[i])
                req_get_data = self.session.get(request_str)
                if req_get_data.status_code != 200:
                    exit('Server {} answer: {} {}'.format(self.serverName, req_get_data.status_code,
                                                          req_get_data.content.decode('utf-8')))
                api_result = json.loads(req_get_data.content.decode('utf-8'))
                api_result_array[in_activity[i]] = api_result
                req_get_data.close()
            else:
                api_result_array = None
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
