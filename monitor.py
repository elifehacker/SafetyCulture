import pandas as pd
import json
from datetime import datetime

MESSAGE_BUS_PATH = 'message_bus.json'
DATABASE_PATH = 'database.csv'
THIRD_PARTY_PATH = '3rd_party_data.json'
ERROR_LOG_PATH = 'error.txt'

def save_old_msg(old_data):
    # todo save to a database
    print('save old message to a database')

def print_latest_msg(latest_data):
    for k in latest_data.keys():
        print(k)
        print(latest_data[k])

def print_third_party_data(third_party_data):
    for line in third_party_data:
        print(line)

def write_to_error_log(line, error):
    print('write error to the log', error)
    with open(ERROR_LOG_PATH, "a") as f:
        f.write(line)
        f.write(str(datetime.now()))
        f.write(' ')
        f.write(str(error))
        f.write('\n')

def update_msg(latest_data, msg):
    user_id = msg['user_id']
    print('update data with user_id', user_id)
    if user_id in latest_data.keys():
        save_old_msg(latest_data[user_id])
    latest_data[user_id] = msg

def get_data_from_uid(df, msg):
    # todo replace this with actual db query/cmd
    return df[df['user_id'] == msg['user_id']]

def get_3rd_party_data_from_email(email, third_party_data):
    # todo replace this with actual request to 3rd party api
    for jdata in third_party_data:
        if jdata['user_email'] == email:
            return jdata
    return 'not found'

def prompt_for_input(latest_msgs, msg, df, third_party_data):
    while True:
        print('enter: proceed\n\
            1: check latest message of each user\n\
            2: print database\n\
            3: print 3rd party data\n\
            4: print database record of the latest message\n\
            5: print 3rd party data of the latest message\n')
        i = input()
        print('input was', i)
        if i == '':
            break
        elif i == '1':
            print_latest_msg(latest_msgs)
        elif i == '2':
            print(df)
        elif i == '3':
            print_third_party_data(third_party_data)
        elif i == '4':
            print(get_data_from_uid(df, msg))
        elif i == '5':
            data = get_data_from_uid(df, msg)
            email = data['email'].to_string(index=False).strip()
            #print(email)
            print(get_3rd_party_data_from_email(email, third_party_data))


if __name__ == '__main__':
    with open(THIRD_PARTY_PATH, "r") as f:
        third_party_data = list()
        while True:
            line = f.readline() 
            if not line:
                break
            third_party_data.append(json.loads(line))

    df = pd.read_csv(DATABASE_PATH)
    latest_msgs = dict()
    with open(MESSAGE_BUS_PATH, "r") as f:
        # simulate message coming in
        while True:
            line = f.readline() 
            if not line:
                break
            # keep processing if a message is bogus
            try:
                msg = json.loads(line)
                msg['timestamp'] = datetime.now()
                update_msg(latest_msgs, msg)
                prompt_for_input(latest_msgs, msg, df, third_party_data)
            except Exception as e:
                write_to_error_log(line, e)
                continue
