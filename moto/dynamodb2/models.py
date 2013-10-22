from collections import defaultdict
from collections import MutableMapping
import datetime
import json
import functools

try:
        from collections import OrderedDict
except ImportError:
        # python 2.6 or earlier, use backport
        from ordereddict import OrderedDict


from moto.core import BaseBackend
from .comparisons import get_comparison_func
from .utils import unix_time


class DynamoJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()


def dynamo_json_dump(dynamo_object):
    return json.dumps(dynamo_object, cls=DynamoJsonEncoder)


class DynamoType(object):
    """
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelDataTypes
    """

    def __init__(self, type_as_dict):
        self.type = type_as_dict.keys()[0]
        self.value = type_as_dict.values()[0]

    def __hash__(self):
        return hash((self.type, self.value))

    def __eq__(self, other):
        return (
            self.type == other.type and
            self.value == other.value
        )

    def __repr__(self):
        return "<DynamoType({0})>".format(self.to_json())

    def to_json(self):
        return {self.type: self.value}

    def compare(self, range_comparison, range_objs):
        """
        Compares this type against comparison filters
        """
        range_values = [obj.value for obj in range_objs]
        comparison_func = get_comparison_func(range_comparison)
        return comparison_func(self.value, *range_values)


class Item(MutableMapping):
    def __init__(self, attrs):
        self.attrs = {}
        for key, value in attrs.iteritems():
            self.attrs[key] = DynamoType(value)

    def __repr__(self):
        return "Item: {0}".format(self.to_json())

    def __getitem__(self, key):
        return self.attrs[key]

    def __setitem__(self, key, value):
        self.attrs[key] = DynamoType(value)

    def __delitem__(self, key):
        del self.attrs[key]

    def __iter__(self):
        return iter(self.attrs)

    def __len__(self):
        return len(self.attrs)

    def to_json(self):
        attributes = {}
        for attribute_key, attribute in self.attrs.iteritems():
            attributes[attribute_key] = attribute.value

        return {
            "Attributes": attributes
        }

    def describe_attrs(self, attributes):
        return {
            "Item": {k: v for k, v in self.attrs.iteritems() if not attributes or k in attributes}
        }


class Table(object):
    def __init__(self, name, schema=None, throughput=None, indexes=None):
        self.name = name
        self.schema = schema
        self.throughput = throughput
        self.indexes = indexes
        self.created_at = datetime.datetime.now()

    @property
    def describe(self):
        description = {
            "Table": {
                "CreationDateTime": unix_time(self.created_at),
                "KeySchema": self.schema,
                "ProvisionedThroughput": self.throughput,
                "TableName": self.name,
                "TableStatus": "ACTIVE",
                "ItemCount": len(self),
                "TableSizeBytes": 0,
            }
        }
        if self.indexes:
            description["Table"]["LocalSecondaryIndexes"] = self.indexes
        return description

    def query(self, hash_key, range_comparison, range_objs):
        results = []
        last_page = True # Once pagination is implemented, change this

        possible_results = list(self.all_items())
        if range_comparison:
            for result in possible_results:
                if result.range_key.compare(range_comparison, range_objs):
                    results.append(result)
        else:
            # If we're not filtering on range key, return all values
            results = possible_results
        return results, last_page

    def scan(self, filters):
        results = []
        scanned_count = 0
        last_page = True # Once pagination is implemented, change this

        for result in self.all_items():
            scanned_count += 1
            passes_all_conditions = True
            for attribute_name, (comparison_operator, comparison_objs) in filters.iteritems():
                attribute = result.attrs.get(attribute_name)

                if attribute:
                    # Attribute found
                    if not attribute.compare(comparison_operator, comparison_objs):
                        passes_all_conditions = False
                        break
                elif comparison_operator == 'NULL':
                    # Comparison is NULL and we don't have the attribute
                    continue
                else:
                    # No attribute found and comparison is no NULL. This item fails
                    passes_all_conditions = False
                    break

            if passes_all_conditions:
                results.append(result)

        return results, scanned_count, last_page


class HashKeyTable(Table):
    def __init__(self, name, schema=None, throughput=None, indexes=None):
        super(HashKeyTable, self).__init__(name, schema=schema, throughput=throughput, indexes=indexes)
        self.hash_key_attr = self.schema[0]["AttributeName"]
        self.items = {}

    def __len__(self):
        return len(self.items)

    def __nonzero__(self):
        return True

    def all_items(self):
        return self.items.values()

    def put_item(self, item_attrs):
        item = Item(item_attrs)
        self.items[DynamoType(item_attrs[self.hash_key_attr])] = item
        return item

    def get_item(self, key):
        if len(key) != 1:
            raise ValueError("Table has only a hash key, but a range key was passed into get_item")
        if self.hash_key_attr not in key:
            return None

        return self.items.get(DynamoType(key[self.hash_key_attr]))

    def delete_item(self, key):
        try:
            return self.items.pop(DynamoType(key[self.hash_key_attr]))
        except KeyError:
            return None


class RangeKeyTable(object):
    def __init__(self, name, schema=None, throughput=None, indexes=None):
        super(RangeKeyTable, self).__init__(name, schema=schema, throughput=throughput, indexes=indexes)
        self.hash_key_attr = self.schema[0]["AttributeName"]
        self.range_key_attr = self.schema[1]["AttributeName"]
        self.items = {}
        self.index_items = defaultdict(dict)
        dynamodb2_backend.tables[name] = self

    def __len__(self):
        return len(self.items)

    def __nonzero__(self):
        return True

    def all_items(self):
        return self.items.values()

    def put_item(self, item_attrs):
        key = (DynamoType(item_attrs.get(self.hash_key_attr)),
               DynamoType(item_attrs.get(self.range_key_attr)))
        item = Item(item_attrs)

        self.items[key] = item

        for index in self.indexes or []:
            key_name = index[1]["AttributeName"]
            if key_name in item_attrs:
                self.index_items[key_name][DynamoType(item_attrs[key_name])] = item

        return item

    def _get_primary_key(self, key):
        if len(key) == 1:
            raise ValueError("Table has a range key, operations must supply a range key.")

        if self.hash_key_attr not in key:
            raise ValueError("Operations must supply a value for the hash key.")
        hash_key = DynamoType(key[self.hash_key_attr])

        if self.range_key_attr in key:
            return (hash_key, DynamoType(key[self.range_key_attr]))

        for index in self.indexes:
            key_name = index[1]["AttributeName"]
            if key_name not in key:
                continue
            return self.index_items[key_name][DynamoType(key[key_name])]

        raise ValueError("Operations must supply a value for the range key or an indexed attribute.")

    def get_item(self, key):
        try:
            composite_key = self._get_primary_key(key)
            return self.items[composite_key]
        except KeyError:
            return None

    def delete_item(self, key):
        try:
            composite_key = self._get_primary_key(key)
            return self.items.pop(composite_key)
        except KeyError:
            return None


class DynamoDBBackend(BaseBackend):
    def __init__(self):
        self.tables = OrderedDict()

    def create_table(self, name, schema=None, throughput=None, indexes=None):
        table_cls = RangeKeyTable if len(schema) == 2 else HashKeyTable
        table = table_cls(name, schema=schema, throughput=throughput, indexes=indexes)
        self.tables[name] = table
        return table

    def delete_table(self, name):
        return self.tables.pop(name, None)

    def update_table_throughput(self, name, new_read_units, new_write_units):
        table = self.tables[name]
        table.read_capacity = new_read_units
        table.write_capacity = new_write_units
        return table

    def put_item(self, table_name, item_attrs):
        table = self.tables.get(table_name)
        if not table:
            return None

        return table.put_item(item_attrs)

    def get_item(self, table_name, key):
        table = self.tables.get(table_name)
        if not table:
            return None

        return table.get_item(key)

    def query(self, table_name, hash_key_dict, range_comparison, range_value_dicts):
        table = self.tables.get(table_name)
        if not table:
            return None, None

        hash_key = DynamoType(hash_key_dict)
        range_values = [DynamoType(range_value) for range_value in range_value_dicts]

        return table.query(hash_key, range_comparison, range_values)

    def scan(self, table_name, filters):
        table = self.tables.get(table_name)
        if not table:
            return None, None, None

        scan_filters = {}
        for key, (comparison_operator, comparison_values) in filters.iteritems():
            dynamo_types = [DynamoType(value) for value in comparison_values]
            scan_filters[key] = (comparison_operator, dynamo_types)

        return table.scan(scan_filters)

    def delete_item(self, table_name, key):
        table = self.tables.get(table_name)
        if not table:
            return None

        return table.delete_item(key)


dynamodb2_backend = DynamoDBBackend()
