import os
import boto3
from chalice import Chalice, NotFoundError

app = Chalice(app_name='nuclei-serverless-app-dynamo')

app.debug = True
_TABLE = None


def get_table_resource():
    global _TABLE
    if _TABLE is None:
        table_name = os.environ.get('TABLE_NAME', '')
        _TABLE = boto3.resource('dynamodb').Table(table_name)
    return _TABLE


@app.route('/item/{id}', methods=['PUT'])
def create_item(id):
    record = {'id': id, **app.current_request.json_body}
    table = get_table_resource()
    table.put_item(Item=record)


@app.route('/item/{id}', methods=['GET'])
def get_item(id):
    table = get_table_resource()
    response = table.get_item(Key={'id': id}).get('Item')
    if response is not None:
        return response
    else:
        raise NotFoundError(id)