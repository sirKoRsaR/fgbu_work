import classes.ChatRoom as ChatRoom
import config
import classes.MongoRequest as MongoRequest
import classes.CamundaAPI as CamundaAPI
import sys
import tasks.workshop_gmp as tasks
import classes.LogForever as LogForever


if __name__ == '__main__':
    args = sys.argv
    # logger = LogForever.LogForever('project')
    # ChatRoom.get_initial_param(config.param_list)

    # tasks.task01_gmp_ppoz_compare
    # tasks.task02_gmp_error()
    # tasks.task03_restart_inc_gmp()
    # tasks.task04_get_instance_on_gmp()
    tasks.task05_not_ans_gmp_to_ppoz()

    print(config.camunda['server'])

