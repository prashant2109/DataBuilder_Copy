
class GlobalData(object):
    # storage for data object
    __globaldata = {}

    def __init__(self):
        pass

    def __getattr__(self, name):
        return getattr(self.__globaldata, name)

    def add(self, key, val):
        GlobalData.__globaldata[key] = val

