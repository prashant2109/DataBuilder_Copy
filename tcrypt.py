import os, sys
import struct
import random
import base64
from Crypto.Cipher import AES
import time

BS = 16
#pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS) 
#unpad = lambda s : s[0:-ord(s[-1])]

pad = lambda s: s + (BS - len(s) % BS) * chr(15)
def unpad(s):
    tmpsize = len(s)
    while ord(s[tmpsize - 1]) == 15:
        tmpsize -= 1
    return s[0:tmpsize]

class AESCrypt:
    def __init__(self, key='', keysize=32):
        self.__key = self.get_key(key, keysize)
        self.__keysize = keysize
        return

    def get_key(self, keystr, size):
        tmpkey = 'QngxaW1pcGZGeWwxNUFBNmshqVLICsP3CbUTCmOHKXgTU58M1Hi4ido25u82xnsQ'
        tmpstr = keystr if keystr else tmpkey.encode('rot-13')
        aeskey = tmpstr[:size]
        return aeskey

    def random_str(self, size):
        tmplst = [chr(random.randrange(127, 255)) for i in range(0, size)]
        keystr = ''.join(tmplst)
        return keystr

    def __get_aes_iv_fixed(self, size):
        tmp = [183, 240, 175, 253, 237, 241, 234, 241, 243, 153, 248, 185, 231, 177, 160, 203]
        tmplst = [chr(tmp[i]) for i in range(0, size)]
        keystr = ''.join(tmplst)
        return keystr

    def getHex(self, msg):
        tstr = ''
        for c in msg:
            tstr += '%02x' %ord(c)
        return tstr

    def encrypt_old(self, msg):
        msg = pad(msg)
        IV = self.random_str(AES.block_size)
        obj = AES.new(self.__key, AES.MODE_CBC, IV)
        return base64.b64encode(IV + obj.encrypt(msg))

    def decrypt_old(self, msg):
        enc = base64.b64decode(msg)
        IV = enc[:AES.block_size]
        obj = AES.new(self.__key, AES.MODE_CBC, IV)
        return unpad(obj.decrypt(enc[BS:]))

    def encryptIV(self, msg):
        msg = pad(msg)
        IV = self.__get_aes_iv_fixed(AES.block_size)
        obj = AES.new(self.__key, AES.MODE_CBC, IV)
        enc = obj.encrypt(msg)
        encsize = len(enc)
        reserved = self.__get_aes_iv_fixed(12)
        data = struct.pack('16s I 12s', IV, encsize, reserved)
        data += enc
        return base64.b64encode(data)

    def encrypt(self, msg):
        msg = pad(msg)
        IV = self.random_str(AES.block_size)
        obj = AES.new(self.__key, AES.MODE_CBC, IV)
        enc = obj.encrypt(msg)
        encsize = len(enc)
        reserved = self.random_str(12)
        data = struct.pack('16s I 12s', IV, encsize, reserved)
        data += enc
        return base64.b64encode(data)

    def decrypt(self, msg):
        enc = base64.b64decode(msg)
        ttup = struct.unpack('16s I 12s', enc[:32])
        IV, bufsize, reserved = ttup
        obj = AES.new(self.__key, AES.MODE_CBC, IV)
        dec = obj.decrypt(enc[32:])
        data = unpad(dec)
        return data

    def test(self, msg):
        obj = AESCrypt()
        print 'original :', msg
        print 'original size:', len(msg)
        enc = obj.encrypt(msg)
        print 'Encrypted:', self.getHex(enc)
        obj1 = AESCrypt()
        dec = obj1.decrypt(enc)
        print 'decrypted:', dec
        print 'decrypted size:', len(dec)
        return

    def debug(self):
        self.test('This is AES Encryption !!!')
        return

if __name__=='__main__':
    obj = AESCrypt()
    obj.debug()

