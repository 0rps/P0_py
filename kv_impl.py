kvstore = {}


def init_db():
    kvstore.clear()


def put(key, value):
    kvstore[key] = value


def get(key):
    return kvstore.get(key)


def clear(key):
    del kvstore[key]

