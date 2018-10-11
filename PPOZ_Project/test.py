import json

my_dict = {'box0': '{dfgdfv}'}
array_temp = []

my_array1 = [
                {'instance': 'instance0', 'box': 'box0', 'id': 'id0'},
                {'instance': 'instance1', 'box': 'box1', 'id': 'id1'},
                {'instance': 'instance2', 'box': 'box2', 'id': 'id2'},
                {'instance': 'instance3', 'box': 'box1', 'id': 'id3'}
             ]

if 'box0' in my_dict:
    #print('yes')
    #print(my_dict['box0'])
    #array_temp = my_dict['box0']
    array_temp = ['dfdf']
    array_temp = array_temp + ['sdfsdfdsffdgfh']
    my_dict['box0'] = array_temp
    print(array_temp)
    #my_dict.update({'box0': {my_dict['box0'] {dfdf}'})
else:
    #print('No')
    my_dict['box0'] = my_array1[1]
    #print(my_array)


# for i in my_array1:
#     if i['box'] in my_dict:
#         pass
#     else:
#         my_dict[i['box']] = i

    # print(i['box'])

    #my_array[i['box']] = my_array[i['box']].update(i)

print(my_dict)
#print (array_temp)