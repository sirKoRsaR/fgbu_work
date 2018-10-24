

test = {'result': ['OK_0', 'OK', 'OK_PRE']}
check = ['OK_PRE', 'OK_0', 'OK']
check1 = 'OK'


t1 = {'statusGmp': ['paid', 'paid']}
t2 = {'statusGmp': ['time', 'paid']}

r = ['paid']

if all(elem in r for elem in t2.get('statusGmp')):
    print('Success')


s = test.get('result')
print(s)
if check1 in s:
    print(test)
