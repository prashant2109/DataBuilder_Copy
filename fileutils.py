import os, sys
import os.path
import shutil

def isfile(fname):
    return os.path.isfile(fname)

def isdir(dname):
    return os.path.isdir(dname)

def ispathexists(pname):
    return os.path.exists(pname)

def getpath(*args):
    lst = [str(x) for x in args]
    p = os.path.join(*lst)
    return p

def get_dir_files(dirname):
    filelst = []
    for path, subdirs, files in os.walk(dirname):
        for name in files:
            fname = os.path.join(path, name)
            filelst.append(fname)
    return filelst

def createdir(fname):
    dirname = os.path.dirname(fname)
    if dirname and (not os.path.exists(dirname)):
        os.makedirs(dirname)
    return

def make_dirs_from_fname(fname):
    return createdir(fname)

def make_dirs(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return

def get_file_size(src):
    fsize = 0
    if os.path.isfile(src):
        fsize = os.stat(src).st_size
    return fsize

def copyfile(src, dst):
    status = None
    try:
        if os.path.isfile(src):
            createdir(dst)
            shutil.copy2(src, dst)
            status = True
    except OSError, (errno, strerror):
        status = None
        pass
    return status

def copydir(src, dst):
    status = None
    try:
        if os.path.isdir(src):
            shutil.copytree(src, dst)
            status = True
    except OSError, (errno, strerror):
        status = None
        pass
    return status

def rmdir(dirname):
    try:
        if os.path.isdir(dirname):
            shutil.rmtree(dirname)
        elif os.path.isfile(dirname):
            os.remove(dirname)
    except OSError, (errno, strerror):
        pass
    return

def rmfile(fname):
    if os.path.isfile(fname):
        os.remove(fname)
    return

def movedir(srcdir, dstdir):
    try:
        if os.path.isdir(srcdir):
            shutil.move(srcdir, dstdir)
    except OSError, (errno, strerror):
        pass
    return

def chmod(path, mode):
    mode = 0o777
    os.chmod(path, mode)
    return

def chown(path, uid, gid):
    os.chown(path, uid, gid)
    return

def chown_n(path, uname, gname):
    shutil.chown(path, uname, gname)
    return

def chmod_r(dirname, mode):
    mode = 0o777
    for path, subdirs, files in os.walk(dirname):
        for name in files:
            fname = os.path.join(path, name)
            os.chmod(fname, mode)
    return

def chown_rn(dirname, uname, gname):
    for path, subdirs, files in os.walk(dirname):
        for name in files:
            fname = os.path.join(path, name)
            shutil.chown(fname, uname, gname)
    return

def remote_filecopy(remotesrc, localdst):
        createdir(localdst)
        cmd = 'wget -q -O %s %s' %(str(remotesrc), str(localdst))
        os.system(cmd)
        return

def remote_dircopy_old(remotesrc, localdst):
        dircnt = 10
        createdir(localdst)
        cmd = 'wget -q --recursive --no-host-directories --no-parent --cut-dirs=%s --reject "index.html*" %s --directory-prefix=%s' %(str(dircnt), str(remotesrc), str(localdst))
        os.system(cmd)
        return

def remote_dircopy(remotesrc, localdst):
        def remote_dirpath_size(url):
            import urlparse
            dcnt = 0
            tmpObj = urlparse.urlparse(url)
            url_path = tmpObj.path.strip()
            if url_path:
                tlst = url_path.split(os.sep)
                tlst = [elm for elm in tlst if elm.strip()]
                dcnt = len(tlst)
            return dcnt

        dircnt = remote_dirpath_size(remotesrc)
        self.createdir(localdst)
        cmd = 'wget -q --recursive --no-host-directories --no-parent --cut-dirs=%s --reject "index.html*" %s --directory-prefix=%s' %(str(dircnt), str(remotesrc), str(localdst))
        os.system(cmd)
        return

