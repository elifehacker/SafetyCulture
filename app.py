from flask import Flask, request, Response
from monitor import update_msg, print_latest_msgs
import json

app = Flask(__name__, static_url_path="")
latest_msgs = dict()

@app.route('/api/message', methods=['POST'])
def process_message():
    msg = request.json
    msg = json.loads(msg)
    update_msg(latest_msgs, msg)
    print_latest_msgs(latest_msgs)
    return Response('', status=201, mimetype='application/json')
    
if __name__ == '__main__':
    app.run(debug=True)