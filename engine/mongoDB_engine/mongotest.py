from flask import Blueprint, Flask, jsonify, request
from flask_pymongo import PyMongo
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

app.config['MONGO_URI'] = 'mongodb+srv://nft:nft123@nft.qrqri.mongodb.net/rainydrop?retryWrites=true&w=majority'

mongo = PyMongo(app, retryWrites=False, connect=True)
blueprint = Blueprint('client', __name__)


mongo = PyMongo(app, retryWrites=False)

@blueprint.route('/')
def Clients():
    Clients = mongo.db.Clients
    Clients_data = Clients.find({})
    data = []
    for x in Clients_data:
        x['_id'] = str(x['_id'])
        data.append(x)
    return jsonify({'success': True, 'data': data})

@blueprint.route('/', methods=['POST'])
def ClientsPost():
    Clients = mongo.db.Clients
    data = request.json
    print('abc', data)
    Clients.insert_one({'img': data['img'],
                         'title': data['title'],
                         'author': data['author'],
                         'subscribe': data['subscribe'],
                         'return': data['return'],
                         'desc': data['desc']})
    return jsonify({'success': True, 'message': 'data inserted', 'data': data})

if __name__ == "__main__":
    app.run()