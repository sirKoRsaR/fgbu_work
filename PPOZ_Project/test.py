import stomp
import json
import time
import logging
import rest_api as rapi

# broker = '10.128.229.30'
broker = 'ppoz-bus-command-01.prod.egrn'
port = 61613
url = 'http://ppoz-gmp-process-01.prod.egrn:9080'


def sendMessage(brokerHost, brokerPort, destination, msg, aheaders):
    conn = stomp.Connection([(brokerHost, brokerPort)], auto_content_length=False)
    conn.start()
    conn.connect()
    tx = conn.begin()
    headers = dict(aheaders)
    headers['persistent'] = 'true'
    conn.send(body=msg, destination=destination, headers=headers)
    conn.commit(tx)
    time.sleep(1)
    conn.disconnect()
    print('{} Ok'.format(msg))
    logging.info('businessKey_send_to_mq_gmp: %s ' % (format(msg)))


def main():
    level = logging.INFO
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=level, filename='send_to_mq_gmp_list_key_protocol.log')

    if __name__ == '__main__':
        file = open('gmp_BK_send.txt', 'r')
        for key in file:
            result_key_id = rapi.get_id(url, key.strip())
            if result_key_id is None:
                body = str(json.dumps({"businessKey": key.strip()}))
                sendMessage(broker, port, '/queue/gmpBpmIn', body,
                            {'command': 'ru.atc.rosreestr.cmd.StartProcessCommand'})
            else:
                logging.info('ID_camunda_bad: %s BK: %s' % (result_key_id, key))
                result_cancel = rapi.cancel_id(url, result_key_id)
                logging.info('BK: %s Result_cancel_pid_camunda: %s' % (key, result_cancel))
                body = str(json.dumps({"businessKey": key.strip()}))
                sendMessage(broker, port, '/queue/gmpBpmIn', body,
                            {'command': 'ru.atc.rosreestr.cmd.StartProcessCommand'})

        file.close()


main()