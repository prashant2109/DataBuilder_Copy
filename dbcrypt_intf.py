import os, sys
import os.path
import tcrypt
import GlobalData

def getDBKey():
    gObj = GlobalData.GlobalData()
    dbkeysize = 32
    dbkey = gObj.get('dbkey', '')
    if not dbkey: dbkey = 'QngxaW1pcGZGeWwxNUFBNmshqVLICs12'
    if 0 and dbkey:
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


