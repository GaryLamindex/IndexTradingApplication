import string
import csv
import pandas as pd
import json
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# for handle decimal serialization in JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class utils:
    # for the following functions, assume that the the parameter "result" is simply grabbed from the db query
    def query_result_to_dataframe(self,result):
        data_string = json.dumps(result,cls=DecimalEncoder)
        df = pd.read_json(data_string,orient='records')
        return df

    def query_result_to_csv(self,result):
        # the default name for the output file is result.csv
        df = self.query_result_to_dataframe(result)
        df.to_csv("result.csv")

class dynamo_db_engine(object):
    endpoint_url = ""
    dynamodb = None
    client = None

    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)
        self.client = boto3.client('dynamodb')

    def create_table(self, table_name, key_attribute):
        # format as below (for key_attribute)
        # {
        #     "partition_key": {
        #         "name": <name>,
        #         "keytype": <keytype>
        #     },
        #     "sort_key": {
        #         "name": <name>,
        #         "keytype": <keytype>
        #     }
        # }

        # for sim_data_v4
        # key_attribute = {"partition_key": {"name": "date", "keytype": "S"}, "sort_key": {"name": "timestamp", "keytype": "N"}}

        table = self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': key_attribute.get("partition_key").get("name"),
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': key_attribute.get("sort_key").get("name"),
                    'KeyType': 'RANGE'# Sort Key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': key_attribute.get("partition_key").get("name"),
                    'AttributeType': key_attribute.get("partition_key").get("keytype")
                },
                {
                    'AttributeName': key_attribute.get("sort_key").get("name"),
                    'AttributeType': key_attribute.get("sort_key").get("keytype")
                }
            ],
            BillingMode='PAY_PER_REQUEST' # mode set to on-demand
        )
        return table
    
    def get_table_info(self, table_name):
        # expected return format: an array of dictionary
        # Example below
        # [{'AttributeName': 'spec', 'AttributeType': 'S'}, {'AttributeName': 'timestamp', 'AttributeType': 'N'}]
        table = self.dynamodb.Table(table_name)
        return table.attribute_definitions

    # to be modified
    def upload_dictionary(self, table_name, dict):
        table = self.dynamodb.Table(table_name)
        for obj in json:
            table.put_item(Item=obj)

    def put_item_to_table(self, table_name, item_dict):
        table = self.dynamodb.Table(table_name)
        response = table.put_item(Item=item_dict)
        return response

    def get_item_from_table(self, table_name, key_dict):
        table = self.dynamodb.Table(table_name)

        try:
            response = table.get_item(Key=key_dict)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response['Item']

    def update_item(self, table_name, key_dict, value_dict):

        table = self.dynamodb.Table(table_name)
        expressionAttributeValues = {}
        updateExpression = 'set '
        alphabets = string.ascii_lowercase
        i = 0
        for key, value in value_dict.items():
            if updateExpression == 'set ':
                updateExpression = updateExpression+"info."+ key+"=:"+alphabets[i]
                _key = ":"+alphabets[i]
                expressionAttributeValues.update({_key:value})
                i+=1
            else:
                updateExpression = updateExpression+", info."+ key+"=:"+alphabets[i]
                _key = ":"+alphabets[i]
                expressionAttributeValues.update({_key:value})
                i+=1

        response = table.update_item(
            Key = key_dict,
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues="UPDATED_NEW"
        )
        return response

    def delete_item(self, key_dict, table_name):
        table = self.dynamodb.Table(table_name)

        try:
            response = table.delete_item(Key=key_dict)
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise
        else:
            return response

    def init_table(self, table_name, key_attribute):

        table = self.dynamodb.Table(table_name)
        response = self.dynamodb.describe_table(
            TableName=table_name
        )
        key_name = response.get("Table").get("KeySchema")[0].get("AttributeName")
        key_type = response.get("Table").get("KeySchema")[0].get("KeyType")
        key_attribute = {"name":key_name, "type":key_type}
        table.delete()
        self.create_table(table_name,key_attribute)

        return table

    # note that for ALL queries, you MUST specify a partition key

    # useful one
    def query_all_by_condition(self, table_name, partition_key_value, condition):
        """
        "condition" format: (<attribute name>,<comparison sign>,<value>), only support equality query
        Example:
        1. condition = ["timestamp","=",1508515260]
        2. condition = ["timestamp",">",1508515260]
        db.query_all_by_condition("backtest_rebalance_margin_wif_max_drawdown_control_0","0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0",condition)
        Five supported sign: i) = ii) > iii) < iv) >= v) <=
        """

        # not yet able to handle the data limit
        table = self.dynamodb.Table(table_name)
        partition_key = self.get_table_info(table_name)[0]['AttributeName']
        if (condition[1] == "="):
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(condition[0]).eq(condition[2])
            )
        elif (condition[1] == ">"):
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(condition[0]).gt(condition[2])
            )
        elif (condition[1] == "<"):
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(condition[0]).lt(condition[2])
            )
        elif (condition[1] == ">="):
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(condition[0]).gte(condition[2])
            )
        elif (condition[1] == "<="):
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(condition[0]).lte(condition[2])
            )
        return response['Items']

    def query_all_by_range(self, table_name, partition_key_value, attribute, range):
        """
        note that the partition_key and the specified attribute can be the same
        Example below
        db.query_all_by_range("backtest_rebalance_margin_wif_max_drawdown_control_0", "0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0", "timestamp", (1508515200,1508515260))
        """
        table = self.dynamodb.Table(table_name)
        partition_key = self.get_table_info(table_name)[0]['AttributeName']

        response = table.query(
            KeyConditionExpression=
                Key(partition_key).eq(partition_key_value) & Key(attribute).between(Decimal(range[0]), Decimal(range[1]))
        )

        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value) & Key(attribute).between(range[0], range[1]),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            data.extend(response['Items'])

        return data

    # not useful probably
    def query_all_by_range_and_condition(self, table_name, range, condition):
        table = self.dynamodb.Table(table_name)
        response = table.query(
            KeyConditionExpression=
                Key(condition[0]).eq(condition[1]) & Key(range[0]).between(range[1], range[2])
        )
        return response['Items']

    def query_all_data(self, table_name, partition_key_value):
        table = self.dynamodb.Table(table_name)
        partition_key = self.get_table_info(table_name)[0]['AttributeName']

        response = table.query(
            KeyConditionExpression=
                Key(partition_key).eq(partition_key_value)
        )

        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=
                    Key(partition_key).eq(partition_key_value),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            data.extend(response['Items'])

        return data

    def get_whole_table(self, table_name):
        table = self.dynamodb.Table(table_name)
        response = table.scan()

        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        
        return data

    def csv_or_excel_to_dynamodb(self,table_name,file_name):
        # better provide absolute path
        # you may need to modify amound read_csv / read_excel
        json_obj = json.loads(pd.read_csv(file_name).to_json(orient='records'), parse_float=Decimal)

        table = self.dynamodb.Table(table_name)

        print(len(json_obj))

        # upload items
        with open('SPY_uploaded.csv','w',newline='') as f:
            csv_writer = csv.DictWriter(f,fieldnames = json_obj[0].keys())
            csv_writer.writeheader()
            for record in json_obj:
                # Exception handling
                # also, write the items to a new csv for checking
                try:
                    table.put_item(Item=record)
                    csv_writer.writerow(record)
                except Exception as e:
                    with open('error.txt','w') as error_f:
                        print(e.message,file=error_f)
            

def main():
    db = dynamo_db_engine('http://dynamodb.us-west-2.amazonaws.com')
    # key_attribute = {"partition_key": {"name": "date", "keytype": "N"}, "sort_key": {"name": "timestamp", "keytype": "N"}}
    # db.csv_or_excel_to_dynamodb("backtest_rebalance_margin_wif_max_drawdown_control_0","C:/Users/85266/OneDrive - HKUST Connect/ust/year 2/winter/Fund_web/0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0.xlsx")
    # db.create_table('sim_data_v4',key_attribute)
    data = db.query_all_by_condition("backtest_rebalance_margin_wif_max_drawdown_control_0", "0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0", ["timestamp","=",1508515200])
    print(data)
    # data = db.query_all_by_range("backtest_rebalance_margin_wif_max_drawdown_control_0", "0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0", "timestamp", (1508515200,1508515260))
    # df = db.query_result_to_dataframe(data)
    # print(df)
    # print(df.iloc[0]['QQQ'])
    # info = db.get_table_info('graph_test_1')
    # condition = ("timestamp",1508515260)
    # result = db.query_all_by_condition("backtest_rebalance_margin_wif_max_drawdown_control_0","0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0",condition)
    # print(result)
    # data = db.get_all_data("backtest_rebalance_margin_wif_max_drawdown_control_0")
    # df = db.query_result_to_dataframe(data)
    # data = db.query_all_by_range("backtest_rebalance_margin_wif_max_drawdown_control_0", "0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0", "timestamp", (1508515200,1510859640))
    # my_utils = utils()
    # df = my_utils.query_result_to_dataframe(data)
    # print(df)
    # my_utils.query_result_to_csv(data)
    # db.dynamodb.Table("his_data_one_min").delete()
    # db.csv_or_excel_to_dynamodb("his_data_one_min","C:/dynamodb/dynamodb_related/pythonProject/format_conversion/SPY_test.csv")

if __name__ == "__main__":
    main()
