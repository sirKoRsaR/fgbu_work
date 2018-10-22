import classes.MongoRequest as MongoRequest


server_mongo = MongoRequest.MongoRequest('dictionaries', 'requestTypesItems')
result_cur = server_mongo.get_query(in_projection={'_id': 1, 'value': 1})
req_type = []
for i in result_cur:
    req_type.append(i)
    print(i)
print(req_type)