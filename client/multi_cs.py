#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.01

A file system that interacts with an xmlrpc HT.
Description: modified by Juncheng Gu
            multi-server
"""

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
import copy

class HtProxy:
    """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
# A hashtable supporting atomic operations, i.e., retrieval and setting
# must be done in different operations
    def __init__(self, url):
        self.rpc = []
        s_len = len(url)
        self.snum = len(url)
        for num in range(0, s_len):
            self.rpc.append(xmlrpclib.Server(url[num]))

    # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
    # there is no entry in the SimpleHT
    def __getitem__(self, key):
        rv = self.get(key)
        if rv == None:
            raise KeyError()
        return pickle.loads(rv)
    
# Stores a value in the SimpleHT
    def __setitem__(self, key, value):
        self.put(key, pickle.dumps(value))

# Sets the TTL for a key in the SimpleHT to 0, effectively deleting it
    def __delitem__(self, key):
        self.put(key, "", 0)
      
# Retrieves a value from the DHT, if the results is non-null return true,
# otherwise false
    def __contains__(self, key):
        return self.get(key) != None

# Mod function from key(string) to hash to server_num    
    def mod(self, key):
        key_hash = hash(key)
        key_mod = key_hash % self.snum
        return key_mod
        
    def get(self, key):
        key_mod = self.mod(key)
        res = self.rpc[key_mod].get(Binary(key))
        if "value" in res:
            return res["value"].data
        else:
            return None

    def put(self, key, val, ttl=10000):
        key_mod = self.mod(key)
        return self.rpc[key_mod].put(Binary(key), Binary(val), ttl)

    def read_file(self, filename):
        key_mod = self.mod(filename)
        return self.rpc[key_mod].read_file(Binary(filename))

    def write_file(self, filename):
        key_mod = self.mod(filename)
        return self.rpc[key_mod].write_file(Binary(filename))
    
    """
    acquire read lock, return: status of write lock
    """
    def acquire_r_lock(self, key, u_id):
	key_mod = self.mod(key)
        return pickle.loads(self.rpc[key_mod].acquire_r_lock(Binary(key), Binary(pickle.dumps(u_id))).data)
   
    """
    acquire write lock, return: status of read lock and write lock
    """
    def acquire_w_lock(self, key, u_id):
	key_mod = self.mod(key)
        return pickle.loads(self.rpc[key_mod].acquire_w_lock(Binary(key), Binary(pickle.dumps(u_id))).data)
    
    """
    acquire delete lock, return: combined status of read lock and write lock
    """    
    def acquire_d_lock(self, key, u_id):
	key_mod = self.mod(key)
        return pickle.loads(self.rpc[key_mod].acquire_d_lock(Binary(key), Binary(pickle.dumps(u_id))).data)
    
    """
    release read lock
    """
    def release_r_lock(self, key, u_id):
	key_mod = self.mod(key)
        return self.rpc[key_mod].release_r_lock(Binary(key), Binary(pickle.dumps(u_id)))
    
    """
    release write lock, also write back the changed contents to the server
    """
    def release_w_lock(self, key, u_id, ctx):
	key_mod = self.mod(key)
        return self.rpc[key_mod].release_w_lock(Binary(key), Binary(pickle.dumps(u_id)), Binary(pickle.dumps(ctx)))

class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    def __init__(self, ht, u_id):
        """all metadata is under [path], and data is udner [path][content],
        which means is views "content" as a kind of metadata"""
        self.files = ht
        self.fd = 0
	self.u_id = u_id
        now = time()
        if '/' not in self.files:
            self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                                   st_mtime=now, st_atime=now, st_nlink=2, contents=['/'],
                                   r_lock=0, w_lock=0)
   
    """
    acquire lock, distinguish different requests from paramenter "op" (read, write, delete)
    """   
    def acquire_lock(self, path, op):
        if op == 'read':
            r = self.files.acquire_r_lock(path, self.u_id)
            while r != 0:
                r = self.files.acquire_r_lock(path, self.u_id)
            return True
        elif op == 'write':
            re = []
            w = self.files.acquire_w_lock(path, self.u_id)
	    a = w[0] + w[1]
            while a != 0:
                w = self.files.acquire_w_lock(path, self.u_id)
		a = w[0] + w[1]                
            re.append(0)
            re.append(1)
            return re
        elif op == 'delete':
            d = self.files.acquire_d_lock(path, self.u_id)
            while d != 0:
                d = self.files.acquire_d_lock(path, self.u_id)
            return True
        else:
            print "acquire_lock: wrong op"
            return True
            
    """
    release lock, distinguish different requests from paramenter "op" (read, write)
    paramenter ctx is used to hold the data of the file for writing back.
    """            
    def release_lock(self, path, op, ctx=None):
        if op == 'read':
            self.files.release_r_lock(path, self.u_id)
        elif op == 'write':
            self.files.release_w_lock(path, self.u_id, ctx)
        else:
            print "release_lock: wrong op" 
            
            
    def chmod(self, path, mode):
        """
        read operation is protected by acquire_lock('read')
        """
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        ht['st_mode'] &= 077000
        ht['st_mode'] |= mode
        
        """
        write operation is pr
        """
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))
        
        return 0


    def chown(self, path, uid, gid):
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        if uid != -1:
            ht['st_uid'] = uid
        if gid != -1:
            ht['st_gid'] = gid
        
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))

    def create(self, path, mode):
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
        
	if path not in ht['contents']:
	    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0,st_ctime=time(), st_mtime=time(), st_atime=time(), contents='',r_lock=0, w_lock=0)
                
            ht['st_nlink'] += 1
            ht['contents'].append(path)
            re = self.acquire_lock('/', 'write')
            ht['r_lock'] = re[0]
            ht['w_lock'] = re[1]
            self.release_lock('/', 'write', copy.deepcopy(ht))

            self.fd += 1
        return self.fd	
  
    def getattr(self, path, fh=None):
    
        self.acquire_lock('/', 'read')
        tmp1 = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
              
        if path not in tmp1['contents']:
            raise FuseOSError(ENOENT)
        
        self.acquire_lock(path, 'read')
        tmp2 = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        return tmp2
  
  
    def getxattr(self, path, name, position=0):
        self.acquire_lock(path, 'read')
        tmp = copy.deepcopy(self.files[path])
        attrs = tmp.get('attrs', {})
        self.release_lock(path, 'read')
        
        try:
            return attrs[name]
        except KeyError:
            return ''    # Should return ENOATTR
  
  
    def listxattr(self, path):
        self.acquire_lock(path, 'read')
        tmp = copy.deepcopy(self.files[path])
        attrs = tmp.get('attrs', {}).keys()
        self.release_lock(path, 'read')
        
        return attrs
  
  
    def mkdir(self, path, mode):
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
	if path not in ht['content']:        
	    self.files[path] = dict(st_mode=(S_IFDIR | mode),st_nlink=2, st_size=0, st_ctime=time(), st_mtime=time(),st_atime=time(), contents=[],r_lock=0, w_lock=0)
        
            ht['st_nlink'] += 1
            ht['contents'].append(path)
        
            re = self.acquire_lock('/', 'write')
            ht['r_lock'] = re[0]
            ht['w_lock'] = re[1]
            self.release_lock('/', 'write', copy.deepcopy(ht))
        
        
    def open(self, path, flags):
        self.fd += 1
        return self.fd
  
  
    def read(self, path, size, offset, fh):
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        if 'contents' in ht:
            return ht['contents'][offset:offset + size]
        return None
  
  
    def readdir(self, path, fh):
        self.acquire_lock('/', 'read')
        tmp = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')        
        return ['.', '..'] + [x[1:] for x in tmp['contents'] if x != '/']
  
  
    def readlink(self, path):
        self.acquire_lock(path, 'read')
        tmp = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        return tmp['contents']
  
  
    def removexattr(self, path, name):
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
  
        attrs = ht.get('attrs', {})
        if name in attrs:
            del attrs[name]
            ht['attrs'] = attrs
            self.acquire_lock(path, 'write')
            self.release_lock(path, 'write', copy.deepcopy(ht))
        else:
            pass    # Should return ENOATTR
  
  
    def rename(self, old, new):
        self.acquire_lock(old, 'read')
        f = copy.deepcopy(self.files[old])
        self.release_lock(old, 'read')
  
        re = self.acquire_lock(new, 'write')
        f['r_lock'] = re[0]
        f['w_lock'] = re[1]
        self.release_lock(new, 'write', copy.deepcopy(f))
        
        self.acquire_lock(old, 'delete')
        del self.files[old]
        
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
        
        ht['contents'].append(new)
        ht['contents'].remove(old)
        
        re = self.acquire_lock('/', 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock('/', 'write', copy.deepcopy(ht))

        
        
    def rmdir(self, path):
        self.acquire_lock(path, 'delete')
        del self.files[path]
        
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
        
        ht['st_nlink'] -= 1
        ht['contents'].remove(path)
        
        re = self.acquire_lock('/', 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock('/', 'write', copy.deepcopy(ht))
        
        
    def setxattr(self, path, name, value, options, position=0):
    # Ignore options
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        attrs = ht.get('attrs', {})
        attrs[name] = value
        ht['attrs'] = attrs
        
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))
        
  
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
    def symlink(self, target, source):
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
	if target not in ht['contents']:        
            self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source), contents=source,r_lock=0, w_lock=0)
	
            ht['st_nlink'] += 1
            ht['contents'].append(target)
        
            re = self.acquire_lock('/', 'write')
            ht['r_lock'] = re[0]
            ht['w_lock'] = re[1]
            self.release_lock('/', 'write', copy.deepcopy(ht))
  
  
    def truncate(self, path, length, fh=None):
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        if 'contents' in ht:
            ht['contents'] = ht['contents'][:length]
        ht['st_size'] = length
        
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))
  
  
    def unlink(self, path):
        self.acquire_lock('/', 'read')
        ht = copy.deepcopy(self.files['/'])
        self.release_lock('/', 'read')
        
        ht['contents'].remove(path)
        
        re = self.acquire_lock('/', 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock('/', 'write', copy.deepcopy(ht))
        
        self.acquire_lock(path, 'delete')
        del self.files[path]
  
  
    def utimens(self, path, times=None):
        now = time()
        
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        atime, mtime = times if times else (now, now)
        ht['st_atime'] = atime
        ht['st_mtime'] = mtime
        
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))
  
  
    def write(self, path, data, offset, fh):
    # Get file data
        self.acquire_lock(path, 'read')
        ht = copy.deepcopy(self.files[path])
        self.release_lock(path, 'read')
        
        tmp_data = ht['contents']
        toffset = len(data) + offset
        if len(tmp_data) > toffset:
            # If this is an overwrite in the middle, handle correctly
            ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
        else:
            # This is just an append
            ht['contents'] = tmp_data[:offset] + data
        ht['st_size'] = len(ht['contents'])
        
        re = self.acquire_lock(path, 'write')
        ht['r_lock'] = re[0]
        ht['w_lock'] = re[1]
        self.release_lock(path, 'write', copy.deepcopy(ht))
        
        return len(data)

if __name__ == "__main__":
    if len(argv) < 3:
        print 'usage: %s <mountpoint> <remote hashtable>' % argv[0]
        exit(1)
    a_len = len(argv)
    url = []
    for a in range(2, a_len-1):
        url.append(argv[a])

    if len(argv) ==3:
	u_id = 0
    else:
	u_id = argv[-1]
    # Create a new HtProxy object using the URL specified at the command-line
    fuse = FUSE(Memory(HtProxy(url), u_id), argv[1], foreground=True)
