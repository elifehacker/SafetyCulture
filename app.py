from flask import Flask, request, Response, jsonify
from monitor import update_msg, print_latest_msgs, get_data_from_uid, get_3rd_party_data_from_email, initialize
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

@app.route('/api/user_id/<string:user_id>', methods=['GET'])
def get_user_detail(user_id):
    data = get_data_from_uid(df, user_id)
    email = data['email'].to_string(index=False).strip()
    rsp = dict()
    if user_id in latest_msgs.keys():
        rsp['data message'] = latest_msgs[user_id]
    rsp['user info'] = data.to_dict()
    rsp['3rd party info'] = get_3rd_party_data_from_email(email, third_party_data)
    return jsonify(rsp)
    
if __name__ == '__main__':
    df, third_party_data = initialize()
    app.run(debug=True, host="0.0.0.0")