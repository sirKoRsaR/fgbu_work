import json

my_dict = {'box0': '{dfgdfv}'}
array_temp = []
array_temp2 = [array_temp]

my_array1 = [
                {'instance': 'instance0', 'box': 'box0', 'id': 'id0'},
                {'instance': 'instance1', 'box': 'box1', 'id': 'id1'},
                {'instance': 'instance2', 'box': 'box2', 'id': 'id2'},
                {'instance': 'instance3', 'box': 'box1', 'id': 'id3'}
             ]

ii = []
my_dict = {}
for i in my_array1:
    ii3 = []
    if i['box'] in ii:
        print(str(i['box']) + ' in ii')
        ii3 = my_dict[i['box']]
        ii3.append(i)
        my_dict[i['box']] = ii3
    else:
        ii.append(i['box'])
        ii3.append(i)
        my_dict[i['box']] = ii3
print(my_dict)
for tt, rr in my_dict.items():
    print(tt + '\t' + str(rr))

