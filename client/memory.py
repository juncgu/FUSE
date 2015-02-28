#!/usr/bin/env python

import logging
import copy
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'
    

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
    
    def acquire_lock(self, path, op):
        if op == 'read':
            if self.files[path]['st_lock']['w_lock']!= 0:
                raise FuseOSError(ENOENT)
            self.files[path]['st_lock']['r_lock'] += 1
        elif op == 'write':
            if self.files[path]['st_lock']['r_lock']!= 0:
                raise FuseOSError(ENOENT)
            if self.files[path]['st_lock']['w_lock'] == 0:
                self.files[path]['st_lock']['w_lock'] = 1
            else:
                raise FuseOSError(ENOENT)
        else:
            print "acquire_lock: wrong op"

          
    def release_lock(self, path, op):
        if op == 'read':
            if self.files[path]['st_lock']['r_lock'] > 0:
                self.files[path]['st_lock']['r_lock'] -= 1
        elif op == 'write':
            if self.files[path]['st_lock']['w_lock'] == 1:
                self.files[path]['st_lock']['w_lock'] = 0;
        else:
            print "acquire_lock: wrong op" 
 

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
    
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),
                                st_lock=dict(r_lock=0, w_lock=0, ref_cnt=0))

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),
                                st_lock=dict(r_lock=0, w_lock=0, ref_cnt=0))

        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
  
        self.files[path]['st_lock']['ref_cnt'] += 1
        return self.fd

    def read(self, path, size, offset, fh):
        self.acquire_lock(path, 'read')
        #return self.data[path][offset:offset + size]
        tmp = copy.deepcopy(self.data[path][offset:offset + size])
        self.release_lock(path, 'read')
        
        return tmp
    
    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']

    def readlink(self, path):
        self.acquire_lock(path, 'read')
        #return self.data[path][offset:offset + size]
        tmp = copy.deepcopy(self.data[path])
        self.release_lock(path, 'read')
        
        return tmp

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source),
                                  st_lock=dict(r_lock=0, w_lock=0, ref_cnt=0))
        self.acquire_lock(target, 'write')
        self.data[target] = source
        self.release_lock(target, 'write')

    def truncate(self, path, length, fh=None):
        self.acquire_lock(path, 'write')
        self.data[path] = self.data[path][:length]
        self.release_lock(path, 'write')
        
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.acquire_lock(path, 'write')
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        self.release_lock(path, 'write')
        
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
