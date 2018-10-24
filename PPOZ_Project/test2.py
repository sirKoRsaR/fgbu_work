import classes.MongoRequest as MongoRequest


test = {'result': ['OK_0', 'OK', 'OK_PRE']}
check = ['OK_PRE', 'OK_0', 'OK']

result = all(elem in test['result'] for elem in check)
if result:
    print(test)