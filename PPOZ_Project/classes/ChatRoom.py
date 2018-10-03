import config
from colorama import init, Fore, Style


def get_initial_param(param_list):
    get_param_from_config()
    if config.get_from_mongo == 'yes':
        get_list_mongo_req()
        param_list['MongoDB_req'] = input_command()


def set_init_reset():
    print(f'{Fore.RESET}{Style.NORMAL}')


def input_command():
    ans_req = input(f'\n'
                    f'{Fore.RED}{Style.NORMAL}'
                    f'Введите номер запроса: ')
    print(f'{Fore.RESET}{Style.NORMAL}')
    return ans_req

def get_list_mongo_req():
    # TODO сделать автоматическое пополнение из классов
    print('\n')
    print(f"{Fore.BLUE}{Style.DIM}"
          f"Доступные запросы из MongoDB ППОЗ: ")
    print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
          f'1.\tПолучение кол-ва обращений со статусом quittancesCreated. \t\t')


# quittancesCreated
def get_param_from_config():
    print(f"{Fore.BLUE}{Style.DIM}"
          f"Начальные параметры инициализации:")
    if config.get_from_ppoz:  # Mongo
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из MongoDB: \t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.get_from_mongo}')
    else:
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из MongoDB: \t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
    if config.get_from_ppoz:  # ППОЗ
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из Камунды ППОЗ: '
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.get_from_ppoz}')
    else:
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из Камунды ППОЗ: '
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
    if config.get_from_gmp:  # ГМП
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из Камунды ГМП: \t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.get_from_gmp}')
    else:
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tПолучение сведений из Камунды ГМП: \t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
    if config.unload_in_files:  # Выгрузка в файл
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tВыгрузка сведений в файл: \t\t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.unload_in_files}')
    else:
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tВыгрузка сведений в файл: \t\t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
    if config.unload_in_files:  # Дельта дней
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tКол-во дней от текущей даты: \t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.timedelta_days}')
    else:
        print(f'{Fore.LIGHTBLUE_EX}{Style.NORMAL}'
              f'\tКол-во дней от текущей даты: \t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
    if config.unload_in_files:  # Режим разработчика
        print(f'{Fore.LIGHTYELLOW_EX}{Style.NORMAL}'
              f'\tРежим разработчика: \t\t\t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'{config.debug_start}')
    else:
        print(f'{Fore.LIGHTYELLOW_EX}{Style.NORMAL}'
              f'\tРежим разработчика: \t\t\t\t'
              f'{Fore.LIGHTGREEN_EX}{Style.NORMAL}'
              f'!!! Нет параметра')
