import os, sys
import os.path
import shutil
import shelve
import dbcrypt

def get_file_name(opath, docid, pno, fname, isdb):
    filename = 'error.log'
    if isdb:
        filename = os.path.join(opath, str(docid), 'db', str(pno), fname)
    else:
        filename = os.path.join(opath, str(docid), 'slv', str(pno), fname)
    return filename

def get_file_name_mod(opath, docid, pno, fname, isdb):
    filename = 'error.log'
    if isdb:
        filename = os.path.join(opath, str(docid), 'db_mod', str(pno), fname)
    else:
        filename = os.path.join(opath, str(docid), 'slv', str(pno), fname)
    return filename

def make_dirs(dirname):
    dirname = os.path.join(dirname, '')
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return

def make_dirs_from_fname(fname):
    dirname = os.path.dirname(fname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return

def rmdir(dirname):
    try:
        if os.path.isdir(dirname):
            shutil.rmtree(dirname)
        elif os.path.isfile(dirname):
            os.remove(dirname)
    except OSError, (errno, strerror):
        errmsg = 'Error removing %s, %s' %(dirname, str(strerror))
        print errmsg
    return

def rmfile(fname):
    if os.path.isfile(fname):
        os.remove(fname)
    return

def write_to_shelve_fname(fname, data):
    """
    Function    : write_to_shelve_fname
    Description : write data to the shelve file
    Input       : (a) shelve file name, (b) data
    Output      : data stored to shelve file
    """
    dirname = os.path.dirname(fname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    sh = shelve.open(fname, 'n')
    sh['data'] = data
    sh.close()
    return

def read_from_shelve_fname(fname, default=None):
    """
    Function    : read_from_shelve_fname
    Description : read data from the shelve file
    Input       : (a) shelve file name
    Output      : return stored data
    """
    print fname, 'trying to open this file'
    if os.path.isfile(fname):
        sh = shelve.open(fname, 'r')
        data = sh.get('data', default)
        sh.close()
        return data
    return default

def write_to_shelve(opath, docid, pno, tabname, isenc, data):
    """
    Function    : write_to_shelve
    Description : write data to the shelve file
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) data
    Output      : data stored to shelve
    """

    fname = os.path.join(opath, str(docid), 'slv', str(pno), '%s.sh' %tabname)
    dirname = os.path.dirname(fname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    sh = shelve.open(fname, 'n')
    sh['data'] = data
    sh.close()
    return

def read_from_shelve(opath, docid, pno, tabname, isenc, default={}):
    """
    Function    : read_from_shelve
    Description : read data from the shelve file
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) default data to return on error
    Output      : return data stored in shelve
    """

    fname = os.path.join(opath, str(docid), 'slv', str(pno), '%s.sh' %tabname)
    if os.path.isfile(fname):
        sh = shelve.open(fname, 'r')
        data = sh.get('data', default)
        sh.close()
        return data
    return default

def write_to_db_fname(dbfname, tabname, isenc, data):
    """
    Function    : write_to_db_fname
    Description : write data to sqlite3 db
    Input       : (a) dbfname (b) tablename (c) isenc, (d) data
    Output      : data stored to db
    """

    dirname = os.path.dirname(dbfname)
    print dirname
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    print '\n\nDDDDDDDDDDDdd : ', dbfname
    obj = dbcrypt.DBCrypt(isenc)
    obj.write_to_dbcrypt(dbfname, data, tabname)
    return

def read_from_db_fname(dbfname, tabname, isenc, default={}):
    """
    Function    : read_from_db_fname
    Description : read data from sqlite3 db
    Input       : (a) dbfname (b) tablename (c) isenc, (d) default data to return on error
    Output      : return data stored in db
    """

    if os.path.isfile(dbfname):
        obj = dbcrypt.DBCrypt(isenc)
        return obj.read_from_dbcrypt(dbfname, tabname)
    return default


def write_to_db(opath, docid, pno, tabname, isenc, data):
    """
    Function    : write_to_db
    Description : write data to sqlite3 db
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) data
    Output      : data stored to db
    """

    fname = os.path.join(opath, str(docid), 'db', str(pno), '%s.db' %tabname)
    dirname = os.path.dirname(fname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    obj = dbcrypt.DBCrypt(isenc)
    obj.write_to_dbcrypt(fname, data, tabname)
    return

def read_from_db(opath, docid, pno, tabname, isenc, default={}):
    """
    Function    : read_from_db
    Description : read data from sqlite3 db
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) default data to return on error
    Output      : return data stored in db
    """

    fname = os.path.join(opath, str(docid), 'db', str(pno), '%s.db' %tabname)
    if os.path.isfile(fname):
        obj = dbcrypt.DBCrypt(isenc)
        return obj.read_from_dbcrypt(fname, tabname)
    return default


def write_data(opath, docid, pno, tabname, isdb, isenc, data):
    """
    Function    : write_data
    Description : write data to sqlite3 db / shelve
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) data
    Output      : data stored to db
    """
    if isdb:
        write_to_db(opath, docid, pno, tabname, isenc, data)
    else:
        write_to_shelve(opath, docid, pno, tabname, isenc, data)
    return

def read_data(opath, docid, pno, tabname, isdb, isenc, default={}):
    """
    Function    : read_data
    Description : read data from sqlite3 db / shelve
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) default data to return on error
    Output      : return data stored in db
    """
    if isdb:
        return read_from_db(opath, docid, pno, tabname, isenc, default)
    else:
        return read_from_shelve(opath, docid, pno, tabname, isenc, default)
    return default

def write_data_fname(fname, isdb, isenc, data, tabname='data'):
    """
    Function    : write_data_fname
    Description : write data to sqlite3 db / shelve
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) data
    Output      : data stored to db
    """
    if isdb:
        write_to_db_fname(fname, tabname, isenc, data)
    else:
        write_to_shelve_fname(fname, data)
    return

def read_data_fname(fname, isdb, isenc, default={}, tabname='data'):
    """
    Function    : read_data_fname
    Description : read data from sqlite3 db / shelve
    Input       : (a) opath (b) docid (c) pageno, (d) tablename (e) isenc, (f) default data to return on error
    Output      : return data stored in db
    """
    if isdb:
        return read_from_db_fname(fname, tabname, isenc, default)
    else:
        return read_from_shelve_fname(fname, default)
    return default

