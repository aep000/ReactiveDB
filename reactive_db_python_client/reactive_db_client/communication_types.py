import uuid

def dict_to_serialized_entry(source):
    for key in source.keys():
        source[key] = value_to_entry_value(source[key])
    return source

def create_insert_request(entry, table):
    request = dict()
    request["request_id"] = str(uuid.uuid4())
    request["query"] = {"InsertData": {"table": table, "entry": dict_to_serialized_entry(entry)}}
    return {"Query": request}

def create_search_query(query_type, table, column, key):
    assert (query_type == "FindOne" or query_type == "LessThan" or query_type == "GreaterThan")
    request = dict()
    request["request_id"] = str(uuid.uuid4())
    request["query"] = {query_type: {"table": table, "column": column, "key": value_to_entry_value(key)}}
    return {"Query": request}


def value_to_entry_value(source):
    if isinstance(source, int):
        new_dict = dict()
        new_dict["Integer"] = source
        return new_dict
    if isinstance(source, str):
        new_dict = dict()
        new_dict["Str"] = source
        return new_dict
    if isinstance(source, bool):
        new_dict = dict()
        new_dict["Bool"] = source
        return new_dict
