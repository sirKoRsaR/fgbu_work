import logging
import datetime
import os


class LogForever(object):

    def __init__(self, in_log_type, in_format=None):
        self.log_name = datetime.datetime.now().strftime("%Y-%m-%d") + '_' + in_log_type    # _%H-%M-%S
        # logging.basicConfig(level=logging.INFO,
        #                     format=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s'))
        self.log_worker = logging.getLogger(self.log_name)
        self.log_worker.setLevel(level=logging.INFO)
        log_writer = logging.FileHandler(os.path.join(os.getcwd(), 'Logs/' + self.log_name + '.log'), 'a')
        log_writer.setLevel(logging.DEBUG)
        if in_format is None:
            log_writer.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        else:
            #TODO добавить разные варианты обработки событий
            pass
        self.log_worker.addHandler(log_writer)

    def put_msg(self, in_text, in_type='info') -> object:
        if in_type == 'info':
            self.log_worker.info(in_text)
        elif in_type == 'debug':
            self.log_worker.debug(in_text)
        elif in_type == 'warning':
            self.log_worker.warning(in_text)
        elif in_type == 'error':
            self.log_worker.error(in_text)
        elif in_type == 'critical':
            self.log_worker.critical(in_text)

# logger = logging.Logger(name)
#    logger.setLevel(logging.DEBUG)
#    handler = logging.FileHandler(os.path.join('/some/path/', name + '.log'), 'a')
#    logger.addHandler(handler)
#    return logger1

# Сообщение отладочное
# logging.debug(u'This is a debug message')
# # Сообщение информационное
# logging.info(u'This is an info message')
# # Сообщение предупреждение
# logging.warning(u'This is a warning')
# # Сообщение ошибки
# logging.error(u'This is an error message')
# # Сообщение критическое
# logging.critical(u'FATAL!!!')
#
# logger = logging.getLogger("incidents_stat")
# logger.setLevel(logging.INFO)
# logging.basicConfig(level=logging.INFO, filename='api_gmp_mongo.log')
# fh = logging.FileHandler("logFile.log")
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
# logger.addHandler(fh)
