import os, sys
import re
import GlobalData
import baseobj
import tcrypt
import cStringIO

class decfile(object):
    def __init__(self):
        self.__dbkey = 'qNgpzW1p5GZ6eWh9NUFB3mshqVL86b3p'
        self.__dbkeysize = 32
        self.__fp = None
        self.__buf = ''
        self.__buflines = []
        self.__lineno = 0
        self.__gObj = GlobalData.GlobalData()
        self.__idatamap = self.__gObj.get('idatamap', {})
        return

    def __get_clean_filename(self, fname):
        tmp_fname = fname[:]
        tmp_fname = tmp_fname.replace('/', '#')
        tmp_fname = tmp_fname.replace('\\\\', '#')
        tmp_fname = tmp_fname.replace('\\', '#')
        tmp_fname = tmp_fname.replace(' ', '-')
        tmp_fname = re.sub('#+','#',tmp_fname)
        return tmp_fname

    def __get_ifname(self, fname):
        head, tail = os.path.split(fname)
        tmpfname = self.__idatamap.get(tail, None)
        if tmpfname: return tmpfname

        fname1 = self.__get_clean_filename(fname)
        ofs = 0
        try:
            ofs = fname1.index('input#')
        except Exception, args:
            ofs = 0

        tmpname = fname1[ofs+6:]
        fname1 = self.__idatamap.get(tmpname, fname)
        return fname1

    def open(self, ifname, mode='r'):
        #print ifname
        ifname = self.__get_ifname(ifname)
 #       print "ifname", ifname
        if not os.path.isfile(ifname):
            raise ValueError, '%s, file not found...' %(ifname)

        fp = open(ifname, 'r')
        msg = fp.read()
        fp.close()

        if self.__idatamap:
            obj = tcrypt.AESCrypt(self.__dbkey, self.__dbkeysize)
            #self.__buf = obj.decrypt(msg)
            self.__fp = cStringIO.StringIO(obj.decrypt(msg))
        else:
            #self.__buf = msg
            self.__fp = cStringIO.StringIO(msg)

        return

    def read(self):
        #return self.__buf
        return self.__fp.read()

    def readline(self):
        return self.__fp.readline()
        if not self.__buflines:
            self.__buflines = self.readlines()
        line = ''
        if self.__lineno < len(self.__buflines):
            line = self.__buflines[self.__lineno]
            self.__lineno += 1
        return line

    def readlines(self):
        #return self.__buf.splitlines()
        return self.__fp.readlines()

    def close(self):
        if self.__fp: self.__fp.close()
        self.__fp = None
        return
 
    def debug(self):
        return

