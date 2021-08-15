import json
import pandas as pd
import time
from datetime import datetime
from multiprocessing import Process, Lock, Queue

MESSAGE_BUS_PATH = 'message_bus.json'
DATABASE_PATH = 'database.csv'
THIRD_PARTY_PATH = '3rd_party_data.json'
SENDER_ERROR_LOG_PATH = 'sender_error.txt'
PROCESSOR_ERROR_LOG_PATH = 'processor_error.txt'
SENDER_TIME_INTERVAL_SEC = 10
PROCESSOR_TIME_INTERVAL_SEC = 1

def save_old_msg(old_data):
    # todo save to an actual database
    print('save old message to a database')

def print_latest_msg(latest_data):
    # imagine this being the backend of the data viz tool
    for k in latest_data.keys():
        print(k)
        print(latest_data[k])

def print_third_party_data(third_party_data):
    for line in third_party_data:
        print(line)

def write_to_error_log(line, error, path):
    print('write error to the log', error)
    with open(path, "a") as f:
        f.write(line)
        f.write(str(datetime.now()))
        f.write(' ')
        f.write(str(error))
        f.write('\n')

def update_msg(latest_data, msg):
    # this function could be refactored to send data to the data viz tool
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


def stream_sender(lock, backlog):
    with open(MESSAGE_BUS_PATH, "r") as f:
        # simulate message coming in
        line_count = 0
        while True:
            line = f.readline() 
            line_count += 1
            if not line:
                break
            # keep sending if a message is bogus
            try:
                msg = json.loads(line)
                msg['timestamp'] = datetime.now()
                with lock:  # always aquire lock before checking shared memory
                    if not backlog.full():
                        backlog.put(msg)
                        print('sender', line_count, msg['user_id'])
                time.sleep(SENDER_TIME_INTERVAL_SEC)
            except Exception as e:
                write_to_error_log(line, e, SENDER_ERROR_LOG_PATH)
                continue
        

def stream_processor(lock, backlog, latest_msgs, third_party_data, df):
    while True:
        # keep processing if a message is bogus
        try:
            msg = ''
            with lock:  # always aquire lock before checking shared memory
                if not backlog.empty():
                    msg = backlog.get()
                    print('processor', msg['user_id'])
            if msg != '':
                update_msg(latest_msgs, msg)
                prompt_for_input(latest_msgs, msg, df, third_party_data)
            else:
                time.sleep(PROCESSOR_TIME_INTERVAL_SEC)
        except Exception as e:
            write_to_error_log(str(msg), e, PROCESSOR_ERROR_LOG_PATH)
            continue

if __name__ == '__main__':

    backlog = Queue()
    lock = Lock()

    with open(THIRD_PARTY_PATH, "r") as f:
        third_party_data = list()
        while True:
            line = f.readline() 
            if not line:
                break
            third_party_data.append(json.loads(line))

    df = pd.read_csv(DATABASE_PATH)
    latest_msgs = dict()

    p_sender = Process(target=stream_sender, args=(lock,backlog))
    p_sender.start()
    stream_processor(lock, backlog, latest_msgs, third_party_data, df)

