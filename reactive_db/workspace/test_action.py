
#from reactive_db_client import ClientAsync, create_insert_request


def test(entry):
    entry['grade']['Integer'] -= 10
    print(entry)
    return entry