# import os
# import time
# import faker
# from random import choice

# from faker.providers import user_agent

# os.environ['TZ'] = 'Asia/Kolkata'

# fak = faker.Faker()

# def str_time_prop(start, end, format, prop):
#     stime = time.mktime(time.strptime(start, format))
#     etime = time.mktime(time.strptime(end, format))
#     ptime = stime + prop * (etime - stime)
#     return time.strftime(format, time.localtime(ptime))

# def random_date(start, end, prop):
#     return str_time_prop(start, end, '%d/%b/%Y:%I:%M:%S %z', prop)

# dictionary = {
#     'request': [
#         'GET', 'POST', 'PUT', 'DELETE'
#     ], 
#     'statuscode': [
#         '303', '404', '500', 
#         '403', '502', '304',
#         '200'
#     ],
#     'referrer' : [
#         '-', fak.uri()
#     ]
# }


from faker import Faker

fake = Faker()

for _ in range(1, 100):
    # Get the user agent string
    user_agent = fake.user_agent()
    print(user_agent)
    # print('%s - - [%s] "%s %s HTTP/1.0" %s %s\n' % 
    #     (
    #         fak.ipv4(),
    #         random_date("01/Jan/2018:12:00:00 +0530", "01/Jan/2020:12:00:00 +0530", 1), 
    #         choice(dictionary['request']),
    #         choice(dictionary['statuscode']),    
    #         choice(dictionary['referrer']),
    #         random.randint(1,5000)
    #     )
    # )