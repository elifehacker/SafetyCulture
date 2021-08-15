from monitor import MESSAGE_BUS_PATH, SENDER_ERROR_LOG_PATH, write_to_error_log
from datetime import datetime
import json
import requests
import time

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
                url = 'http://127.0.0.1:5000/api/message'
                print('sending data',msg)
                resp = requests.post(
                    url=url,
                    json=json.dumps(msg, indent=4, sort_keys=True, default=str)
                )
                print(resp.status_code)
                print(resp.text)
                time.sleep(5)
            except Exception as e:
                write_to_error_log(line, e, SENDER_ERROR_LOG_PATH)
                continue