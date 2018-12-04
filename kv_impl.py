kvstore = {}


def init_db():
    kvstore.clear()


def put(key, value):
    if key in kvstore:
        kvstore[key][value] = True
    else:
        kvstore[key] = {value: True}


def get(key):
    if key in kvstore:
        return kvstore.get(key).keys()

    return []


def clear(key):
    try:
        del kvstore[key]
    except KeyError:
        pass

