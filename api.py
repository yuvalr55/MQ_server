from flask import Flask, request, jsonify
from flask_cors import CORS
from client import Client
from ujson import dumps
from ListeningServer import status

app = Flask(__name__)
CORS(app)


@app.route('/post', methods=['POST'])
def post():
    try:
        json_input = request.get_json()
        client = Client(dumps(json_input))
        response = client.call()
        return jsonify({'status': str(response)[2:-1]})
    except Exception as err:
        print(err)
        return jsonify({'status': 'error'})


@app.route('/get', methods=['GET'])
def get():
    try:
        select = status.select()
        return jsonify(select)

    except Exception as err:
        print(err)
        return []


if __name__ == '__main__':
    app.run(debug=False, port=5000)
