from read_write_lock import ReadWriteLock
import json


def foo():
    return None, True


class Database(object):
    def __init__(self):
        self.data = dict()
        self.locks = dict()  # locks on each data
        self.modify_lock = ReadWriteLock()  # lock on modification of data structure

    def insert(self, key, value, func=foo, args=()):
        self.modify_lock.acquire_write()
        if key in self.locks:
            self.modify_lock.release_write()
            return False
        self.locks[key] = ReadWriteLock()
        self.locks[key].acquire_write()
        self.modify_lock.release_write()


        res, success = func(*args)

        self.modify_lock.acquire_write()
        if not success:
            lock = self.locks.pop(key)
            lock.release_write()
            self.modify_lock.release_write()
            return False
        else:
            self.data[key] = value  # can safely be done because the acquire_write
            self.locks[key].release_write()
            self.modify_lock.release_write()
            return True

    def delete(self, key, func=foo, args=()):
        self.modify_lock.acquire_write()
        if key not in self.locks:
            self.modify_lock.release_write()
            return None
        lock=self.locks[key]
        self.modify_lock.release_write()
        lock.acquire_write()

        res, success = func(*args)

        self.modify_lock.acquire_write()
        if not success:
            lock.release_write()
            self.modify_lock.release_write()
            return None
        else:
            val = self.data.pop(key)  # can safely be done because the acquire_write
            lock = self.locks.pop(key)
            lock.release_write()  # because acquire_write on modify_lock, no one holds this lock so no problem
            self.modify_lock.release_write()
            return val

    def get(self, key, func=foo, args=()):
        self.modify_lock.acquire_read()
        dlock = self.locks.get(key, None)
        if dlock is None:
            self.modify_lock.release_read()
            return None
        self.modify_lock.release_read()
        dlock.acquire_read()

        res, success = func(*args)

        if not success:
            dlock.release_read()
            return None
        else:
            self.modify_lock.acquire_read()
            if key not in self.data:
                dlock.release_read()
                return None
            val = self.data.get(key)
            if res != None and res['value'] != val:
                print("check fail")
                dlock.release_read()
                return None
            dlock.release_read()
            self.modify_lock.release_read()  # don't want dlock to be deleted in case
            return val

    def update(self, key, value, func=foo, args=()):
        self.modify_lock.acquire_read()
        dlock = self.locks.get(key, None)
        if dlock is None:
            self.modify_lock.release_read()
            return False
        self.modify_lock.release_read()
        dlock.acquire_write()

        res, success = func(*args)

        if not success:
            dlock.release_write()
            return False
        else:
            self.modify_lock.acquire_read()
            if key not in self.data:
                dlock.release_write()
                return False
            self.data[key] = value
            dlock.release_write()
            self.modify_lock.release_read()  # don't want dlock to be deleted in case
            return True

    def serialize(self):
        self.modify_lock.acquire_write()
        ret_str = json.dumps(self.data)
        self.modify_lock.release_write()
        return ret_str

    def load(self, dict_string):
        self.modify_lock.acquire_write()
        self.data = json.loads(dict_string)
        for key in self.data:
            self.locks[key] = ReadWriteLock()
        self.modify_lock.release_write()
        return True

    def countkey(self):
        self.modify_lock.acquire_read()
        ret_val = len(self.data)
        self.modify_lock.release_read()
        return ret_val

    def dump(self):
        self.modify_lock.acquire_write()
        ret_val = []
        for key, value in self.data.items():
            ret_val.append([key, value])
        self.modify_lock.release_write()
        print(ret_val)
        return ret_val
