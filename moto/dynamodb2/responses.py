import json

from moto.core.responses import BaseResponse
from moto.core.utils import camelcase_to_underscores
from .models import dynamodb2_backend, dynamo_json_dump


GET_SESSION_TOKEN_RESULT = """
<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
 <GetSessionTokenResult>
 <Credentials>
 <SessionToken>
 AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L
 To6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3z
 rkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtp
 Z3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE
 </SessionToken>
 <SecretAccessKey>
 wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY
 </SecretAccessKey>
 <Expiration>2011-07-11T19:55:29.611Z</Expiration>
 <AccessKeyId>AKIAIOSFODNN7EXAMPLE</AccessKeyId>
 </Credentials>
 </GetSessionTokenResult>
 <ResponseMetadata>
 <RequestId>58c5dbae-abef-11e0-8cfe-09039844ac7d</RequestId>
 </ResponseMetadata>
</GetSessionTokenResponse>"""


def sts_handler():
    return GET_SESSION_TOKEN_RESULT


class DynamoHandler(BaseResponse):

    def get_endpoint_name(self, headers):
        """Parses request headers and extracts part od the X-Amz-Target
        that corresponds to a method of DynamoHandler

        ie: X-Amz-Target: DynamoDB_20111205.ListTables -> ListTables
        """
        # Headers are case-insensitive. Probably a better way to do this.
        match = headers.get('x-amz-target') or headers.get('X-Amz-Target')
        if match:
            return match.split(".")[1]

    def error(self, type_, status=400):
        return status, self.response_headers, dynamo_json_dump({'__type': type_})

    def call_action(self):
        if 'GetSessionToken' in self.body:
            return 200, self.response_headers, sts_handler()

        self.body = json.loads(self.body or '{}')
        endpoint = self.get_endpoint_name(self.headers)
        if endpoint:
            endpoint = camelcase_to_underscores(endpoint)
            response = getattr(self, endpoint)()
            if isinstance(response, basestring):
                return 200, self.response_headers, response

            else:
                status_code, new_headers, response_content = response
                self.response_headers.update(new_headers)
                return status_code, self.response_headers, response_content
        else:
            return 404, self.response_headers, ""

    def list_tables(self):
        body = self.body
        limit = body.get('Limit')
        if body.get("ExclusiveStartTableName"):
            last = body.get("ExclusiveStartTableName")
            start = dynamodb2_backend.tables.keys().index(last) + 1
        else:
            start = 0
        all_tables = dynamodb2_backend.tables.keys()
        if limit:
            tables = all_tables[start:start + limit]
        else:
            tables = all_tables[start:]
        response = {"TableNames": tables}
        if limit and len(all_tables) > start + limit:
            response["LastEvaluatedTableName"] = tables[-1]
        return dynamo_json_dump(response)

    def create_table(self):
        table = dynamodb2_backend.create_table(
            self.body['TableName'],
            self.body['KeySchema'],
            self.body["ProvisionedThroughput"],
            self.body.get("LocalSecondaryIndexes"))
        return dynamo_json_dump(table.describe)

    def delete_table(self):
        name = self.body['TableName']
        table = dynamodb2_backend.delete_table(name)
        if table:
            return dynamo_json_dump(table.describe)
        else:
            er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
            return self.error(er)

    def update_table(self):
        name = self.body['TableName']
        throughput = self.body["ProvisionedThroughput"]
        new_read_units = throughput["ReadCapacityUnits"]
        new_write_units = throughput["WriteCapacityUnits"]
        table = dynamodb2_backend.update_table_throughput(name, new_read_units, new_write_units)
        return dynamo_json_dump(table.describe)

    def describe_table(self):
        name = self.body['TableName']
        try:
            table = dynamodb2_backend.tables[name]
        except KeyError:
            er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
            return self.error(er)
        return dynamo_json_dump(table.describe)

    def put_item(self):
        name = self.body['TableName']
        item = self.body['Item']
        result = dynamodb2_backend.put_item(name, item)
        if result:
            item_dict = result.to_json()
            item_dict['ConsumedCapacityUnits'] = 1
            return dynamo_json_dump(item_dict)
        else:
            er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
            return self.error(er)

    def batch_write_item(self):
        table_batches = self.body['RequestItems']

        for table_name, table_requests in table_batches.iteritems():
            for table_request in table_requests:
                request_type = table_request.keys()[0]
                request = table_request.values()[0]

                if request_type == 'PutRequest':
                    item = request['Item']
                    dynamodb2_backend.put_item(table_name, item)
                elif request_type == 'DeleteRequest':
                    key = request['Key']
                    hash_key = key['HashKeyElement']
                    range_key = key.get('RangeKeyElement')
                    item = dynamodb2_backend.delete_item(table_name, hash_key, range_key)

        response = {
            "Responses": {
                "Thread": {
                    "ConsumedCapacityUnits": 1.0
                },
                "Reply": {
                    "ConsumedCapacityUnits": 1.0
                }
            },
            "UnprocessedItems": {}
        }

        return dynamo_json_dump(response)

    def get_item(self):
        attrs_to_get = self.body.get('AttributesToGet')
        try:
            item = dynamodb2_backend.get_item(self.body['TableName'], self.body['Key'])
        except ValueError:
            er = 'com.amazon.coral.validate#ValidationException'
            return self.error(er, status=400)
        if item:
            item_dict = item.describe_attrs(attrs_to_get)
            item_dict['ConsumedCapacityUnits'] = 0.5
            return dynamo_json_dump(item_dict)
        # Item not found
        er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
        return self.error(er, status=404)

    def batch_get_item(self):
        table_batches = self.body['RequestItems']

        results = {
            "ConsumedCapacity": [],
            "Responses": {},
            "UnprocessedKeys": {}
        }

        for table_name, table_request in table_batches.iteritems():
            items = []
            keys = table_request['Keys']
            attributes_to_get = table_request.get('AttributesToGet')
            for key in keys:
                item = dynamodb2_backend.get_item(table_name, key)
                if item:
                    items.append(item.describe_attrs(attributes_to_get)["Item"])
            results["Responses"][table_name] = items
            results["ConsumedCapacity"].append({"CapacityUnits": 1, "TableName": table_name})
        return dynamo_json_dump(results)

    def query(self):
        name = self.body['TableName']
        hash_key = self.body['HashKeyValue']
        range_condition = self.body.get('RangeKeyCondition')
        if range_condition:
            range_comparison = range_condition['ComparisonOperator']
            range_values = range_condition['AttributeValueList']
        else:
            range_comparison = None
            range_values = []

        items, last_page = dynamodb2_backend.query(name, hash_key, range_comparison, range_values)

        if items is None:
            er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
            return self.error(er)

        result = {
            "Count": len(items),
            "Items": [item.attrs for item in items],
            "ConsumedCapacityUnits": 1,
        }

        # Implement this when we do pagination
        # if not last_page:
        #     result["LastEvaluatedKey"] = {
        #         "HashKeyElement": items[-1].hash_key,
        #         "RangeKeyElement": items[-1].range_key,
        #     }
        return dynamo_json_dump(result)

    def scan(self):
        name = self.body['TableName']

        filters = {}
        scan_filters = self.body.get('ScanFilter', {})
        for attribute_name, scan_filter in scan_filters.iteritems():
            # Keys are attribute names. Values are tuples of (comparison, comparison_value)
            comparison_operator = scan_filter["ComparisonOperator"]
            comparison_values = scan_filter.get("AttributeValueList", [])
            filters[attribute_name] = (comparison_operator, comparison_values)

        items, scanned_count, last_page = dynamodb2_backend.scan(name, filters)

        if items is None:
            er = 'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException'
            return self.error(er)

        result = {
            "Count": len(items),
            "Items": [item.attrs for item in items],
            "ConsumedCapacityUnits": 1,
            "ScannedCount": scanned_count
        }

        # Implement this when we do pagination
        # if not last_page:
        #     result["LastEvaluatedKey"] = {
        #         "HashKeyElement": items[-1].hash_key,
        #         "RangeKeyElement": items[-1].range_key,
        #     }
        return dynamo_json_dump(result)

    def delete_item(self):
        item = dynamodb2_backend.delete_item(self.body['TableName'], self.body["Key"])
        if not item:
            return self.error('com.amazonaws.dynamodb.v20111205#ResourceNotFoundException')

        if self.body.get('ReturnValues', '') == 'ALL_OLD':
            item_dict = item.to_json()
        else:
            item_dict = {'Attributes': []}
        item_dict['ConsumedCapacityUnits'] = 0.5
        return dynamo_json_dump(item_dict)
