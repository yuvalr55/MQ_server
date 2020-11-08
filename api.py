from flask import Flask, request, jsonify
from flask_cors import CORS
from client import Client
import ujson

app = Flask(__name__)
CORS(app)


@app.route('/', methods=['POST'])
def post():
    try:
        data = request.get_json()
        print(" [x] Requesting data from server")
        fibonacci_rpc = Client(ujson.dumps(data))
        response = fibonacci_rpc.call()
        print(f"response from server: {response}")
        return jsonify({'id': response})
    except Exception as err:
        print(err)


if __name__ == '__main__':
    app.run(debug=False, port=5000)
