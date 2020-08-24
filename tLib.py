import os
import sys

class tLib():
    def __init__(self):
        pass

    def make_dirs(self, dirname):
        dirname = os.path.join(dirname, '')
        if dirname and (not os.path.exists(dirname)):
            os.makedirs(dirname)
        return

    def readFromTextFile(self, fullPath, default=[]):
        '''
            Returns Error string if invalid path..
            Returns list if success..
        '''
        if (not fullPath.strip()):
            return 'Error: Empty Path'
        outLines    = default
        if os.path.exists(fullPath):
            with open(fullPath, 'r') as fFile:
                outLines    = fFile.readlines()
        return outLines

    def writeToTextFile(self, fullPath, inpStr=''):
        '''
            Creates the file structure recursively if not exists..
            Returns Error string if invalid input..
            Returns 'done' if success..
        '''
        if (not fullPath.strip()):
            return 'Error: Empty Path'
        if (not isinstance(inpStr, str)) or (not inpStr):
            return 'Error: Input is Not String or Empty Sring'
        if not os.path.exists(fullPath):
            pPath   = fullPath.rsplit('/', 1)[0]
            self.make_dirs(pPath)
        with open(fullPath, 'w') as fFile:
            fFile.write(inpStr)
        return 'done'


if __name__ == '__main__':
    obj = tLib()
    print obj.writeToTextFile('/root/Honnegowda/MB_Demo_v3/pysrc/common/ll/tt/ff/ffcc.txt', 'hi\nHello')
    print obj.readFromTextFile('/root/Honnegowda/MB_Demo_v3/pysrc/common/ll/tt/ff/ffcc.txt')
