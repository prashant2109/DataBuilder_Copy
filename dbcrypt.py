import os, sys
import os.path
import sqlite3
import cPickle
import time
import tcrypt
import GlobalData

def getDBKey():
    gObj = GlobalData.GlobalData()
    dbkeysize = 32
    dbkey = gObj.get('dbkey', '')
    if dbkey:
        dbkeysize = gObj.get('dbkeysize', 0)
        if len(dbkey) != dbkeysize:
            print 'Invalid dbkey...'
            raise ValueError, 'Invalid dbkey...'
        if not dbkeysize in [32, 24, 16]:
            print 'Invalid dbkeysize...'
            raise ValueError, 'Invalid dbkeysize...'
    return dbkey, dbkeysize

def encryptIVfix(msg):
    dbkey, dbkeysize = getDBKey()
    obj = tcrypt.AESCrypt(dbkey, dbkeysize)
    return obj.encryptIV(msg)

def encrypt(msg):
    dbkey, dbkeysize = getDBKey()
    obj = tcrypt.AESCrypt(dbkey, dbkeysize)
    return obj.encrypt(msg)

def decrypt(msg):
    dbkey, dbkeysize = getDBKey()
    obj = tcrypt.AESCrypt(dbkey, dbkeysize)
    return obj.decrypt(msg)

def encrypt_col(msg):
    dbkey, dbkeysize = getDBKey()
    pdata = cPickle.dumps(msg)
    obj = tcrypt.AESCrypt(dbkey, dbkeysize)
    return obj.encrypt(pdata)

def decrypt_col(encmsg):
    dbkey, dbkeysize = getDBKey()
    obj = tcrypt.AESCrypt(dbkey, dbkeysize)
    msg = obj.decrypt(encmsg)
    data = cPickle.loads(str(msg))
    return data

def encryptdict(idata):
    pdata = cPickle.dumps(idata)
    return encrypt(pdata)

def decryptdict(msg):
    decmsg = decrypt(msg)
    data = cPickle.loads(str(decmsg))
    return data

class DBCrypt:
    def __init__(self, encflag=1):
        self.__encflag = int(encflag)
        return

    def __get_connection(self, dbname):
        conn = sqlite3.connect(dbname)
        conn.create_function('encrypt', 1, encrypt)
        conn.create_function('decrypt', 1, decrypt)
        conn.create_function('encryptcol', 1, encrypt_col)
        conn.create_function('decryptcol', 1, decrypt_col)
        cursor = conn.cursor()
        return cursor, conn

    def __drop_tables(self, conn, cur, tablenames):
        for tabname in tablenames:
            stmt = 'drop table if exists %s' %tabname
            cur.execute(stmt)
        conn.commit()
        return

    def __loaddb_blob(self, dbname, idata, itabname):
        dirname = os.path.dirname(dbname)
        if dirname and (not os.path.exists(dirname)):
            os.makedirs(dirname)

        #print 'dbname: ', dbname

        cur, conn = self.__get_connection(dbname)
        self.__drop_tables(conn, cur, [itabname])
        stmt = 'CREATE TABLE IF NOT EXISTS %s (mydata BIG blob)' %(itabname)
        cur.execute(stmt)

        #pdata = cPickle.dumps(mydata, cPickle.HIGHEST_PROTOCOL)
        pdata = cPickle.dumps(idata)
        #print pdata

        stmt0 = "INSERT INTO %s VALUES (?)" %(itabname)
        stmt1 = "INSERT INTO %s VALUES (encrypt(?))" %(itabname)

        stmt = stmt1 if self.__encflag else stmt0

        cur.execute(stmt, (pdata,))
        conn.commit()

        conn.close()

        return

    def __readdb_blob(self, dbname, itabname, default):
        data = default

        # this is due to possible network latency
        for i in range(0, 10):
            if not os.path.isfile(dbname):
               time.sleep(1)
            else:
               break

        if not os.path.isfile(dbname):
            return data

        cur, conn = self.__get_connection(dbname)
        
        # read back
        stmt0 = "select mydata from %s limit 1" %(itabname)
        stmt1 = "select decrypt(mydata) from %s limit 1" %(itabname)

        stmt = stmt1 if self.__encflag else stmt0
        cur.execute(stmt)
        #for row in cur:
        #    data = cPickle.loads(str(row[0]))

        datalst = [elm for elm in cur]
        if datalst:
            data = datalst[0][0]
            try:
                data = cPickle.loads(str(data))
            except:
                pass

        conn.close()

        return data

    def get_connection(self, dbname):
        return self.__get_connection(dbname)

    def close(self, conn):
        conn.commit()
        conn.close()
        return

    def write_to_dbcrypt(self, dbname, idata, itabname):
        #dbname = os.path.join(dbpath, '%s.db' %itabname)

        errexcp = None
        for i in range (0, 5):
            #try:
            if 1:
                self.__loaddb_blob(dbname, idata, itabname)
                errexcp = None
            #except Exception, args:
            else:
                errmsg = str(args)
                errexcp = args
                if ('no such table:' in errmsg) or ('database is locked' in errmsg):
                    print 'Error write: %s\n' %str(args)
                    time.sleep(1)
                    continue
                else:
                    raise args
            break

        if not (errexcp is None):
            raise errexcp

        return

    def read_from_dbcrypt(self, dbname, itabname, default={}):
        #dbname = os.path.join(dbpath, '%s.db' %itabname)
        #print itabname
        errexcp = None
        data = default
        for i in range (0, 5):
            try:
                data = self.__readdb_blob(dbname, itabname, default)
                errexcp = None
            except Exception, args:
                errexcp = args
                errmsg = str(args)
                #try:
                if 0:		
                    if ('no such table:' in errmsg) or ('database is locked' in errmsg):
                        print 'Error write: %s\n' %str(args)
                        time.sleep(1)
                        continue
                    else:
                         raise args
                print 'Error read: %s\n' %str(args)
            break
  
        if 0:#if not (errexcp is None):
            raise errexcp

        return data

    def get_encryptcol(self, msglst):
        tmpd = {}
        for msg in msglst:
            tmpd[msg] = encrypt_col(msg)
        return tmpd

    def test_dec(self, key, keysize):
        fname = 'binary'
        if os.path.isfile(fname):
            fp = open('binary', 'rb')
            msg = fp.read()
            fp.close()
            obj1 = tcrypt.AESCrypt(key, keysize)
            dec = obj1.decrypt(msg)
            print 'dec:', dec
            print 'decsize:', len(dec)
        return

    def debug(self):
        data = {}
        data['key0'] = 'val0'
        data['key1'] = 12345
        data['key2'] = 1234.56789

        print 'idata:', data

        # write to db
        dbname = 'testdb.db'
        self.write_to_dbcrypt(dbname, data, 'mydata')

        # read back from db
        data_read = self.read_from_dbcrypt(dbname, 'mydata')

        print 'rdata:', data_read

        return

if __name__=='__main__':
    obj = tcrypt.AESCrypt()
    obj.debug()

    obj = DBCrypt()
    obj.debug()
    obj.test_dec('QngxaW1pcGZGeWwxNUFBNmshqVLICs12', 32)

