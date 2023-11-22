import time
from faker import Faker
from random import choice


fake = Faker()

# data dictionary for request and statuscode
dictionary = {
    'request': [
        'GET', 'POST', 'PUT', 'DELETE'
    ], 
    'statuscode': [
        '303', '404', '500', 
        '403', '502', '304',
        '200'
    ]
}

# Generate sample data
for _ in range(1, 100):
    # Get the user agent string
    user_agent = fake.user_agent()
    ip_addr = fake.ipv4()
    print(
        f"{ip_addr} - {user_agent} - {choice(dictionary['request'])} - {choice(dictionary['statuscode'])}\n"
    )
    time.sleep(1)
