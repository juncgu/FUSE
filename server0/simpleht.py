#!/usr/bin/env python

"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
import random, copy
from datetime import datetime, timedelta
from xmlrpclib import Binary   
from threading import Lock, RLock
# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)
    random.seed()
    self.dump = 1

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):

    """Juncheng Gu:
    The simulation of StandardError
    prob = random.random()
    if prob < 0.005:
      raise StandardError
    """

    # Remove expired entries
    self.check()
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data:
      ent = self.data[key]
      now = datetime.now()
      if ent[1] > now:
        ttl = (ent[1] - now).seconds
        rv = {"value": Binary(ent[0]), "ttl": ttl}
      else:
        del self.data[key]
    return rv

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key.data] = (value.data, end)
    return True
   
  """
  acquire read lock
  """
  def acquire_r_lock(self, key, u_id):
    file = copy.deepcopy(pickle.loads(self.data[key.data][0]))
    r = file['w_lock'] 
    user = pickle.loads(u_id.data)
    print user, " enter acquire_R_lock"
    print "	", key, "pre r_lock", file['r_lock']
    print "	", key, "pre w_lock", r
    if file['w_lock'] == 0:
	file['r_lock'] = file['r_lock'] + 1 
    self.check()
    end = datetime.now() + timedelta(seconds = 10000)
    self.data[key.data] = (pickle.dumps(file), end)
    print "	", key, "aft r_lock", file['r_lock']	
    print "	", key, "aft w_lock", file['w_lock']	
    print user, " leave acquire_R_lock"	
    return Binary(pickle.dumps(r))
  
  """
  acquire write lock
  """
  def acquire_w_lock(self, key, u_id):
    file = copy.deepcopy(pickle.loads(self.data[key.data][0]))
    w = (copy.deepcopy(file['r_lock']), copy.deepcopy(file['w_lock']))
    user = pickle.loads(u_id.data)
    print user, "enter acquire_W_lock"
    print "	", key, "pre r_lock", file['r_lock']
    print "	", key, "pre w_lock", file['w_lock']
    sum = w[0] + w[1]
    if sum == 0:
	file['w_lock'] = 1
    self.check()
    end = datetime.now() + timedelta(seconds = 10000)
    self.data[key.data] = (pickle.dumps(file), end)
    print "	", key, "aft r_lock", file['r_lock']
    print "	", key, "aft w_lock", file['w_lock']
    print user, " leave acquire_W_lock"	
    return Binary(pickle.dumps(w))
  
  """
  release read lock
  """
  def release_r_lock(self, key, u_id):
    file = copy.deepcopy(pickle.loads(self.data[key.data][0]))
    user = pickle.loads(u_id.data)
    print user, "enter release_R_lock"
    print "	", key, "pre r_lock", file['r_lock']
    print "	", key, "pre w_lock", file['w_lock']
    file['r_lock'] -= 1
    self.check()
    end = datetime.now() + timedelta(seconds = 10000)
    self.data[key.data] = (pickle.dumps(file), end)
    print "	", key, "aft r_lock", file['r_lock']
    print "	", key, "aft w_lock", file['w_lock']
    print user, " leave release_R_lock"	
    return True

  """
  release write lock and write back data
  """
  def release_w_lock(self, key, u_id, ctx):
    self.check()
    end = datetime.now() + timedelta(seconds = 10000)
    self.data[key.data] = (ctx.data, end)
    file = copy.deepcopy(pickle.loads(self.data[key.data][0]))
    user = pickle.loads(u_id.data)
    print user, "enter release_W_lock"
    print "	", key, "pre r_lock", file['r_lock']
    print "	", key, "pre w_lock", file['w_lock']
    file['w_lock'] = 0
    self.check()
    end = datetime.now() + timedelta(seconds = 10000)
    self.data[key.data] = (pickle.dumps(file), end)
    print "	", key, "aft r_lock", file['r_lock']
    print "	", key, "aft w_lock", file['w_lock']
    print user, " leave release_R_lock"	
    return True

  """
  acquire delete lock
  """
  def acquire_d_lock(self, key, u_id):
    file = copy.deepcopy(pickle.loads(self.data[key.data][0]))
    user = pickle.loads(u_id.data)
    print user, " enter acquire_D_lock"	
    print "	", key, "pre r_lock", file['r_lock']
    print "	", key, "pre w_lock", file['w_lock']
    d = file['r_lock'] + file['w_lock'] 
    print "	", key, "aft r_lock", file['r_lock']	
    print "	", key, "aft w_lock", file['w_lock']	
    print user, " leave acquire_D_lock"	
    return Binary(pickle.dumps(d))
  
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
  """
  used to test the atomicity of server
  """       
  def test_atomicity(self, key): 
    print "The client ", key.data , "enter"
    time.sleep(5)
    print "The client ", key.data , "leave"
    return True
       
       
def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 9000
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.acquire_d_lock)
  file_server.register_function(sht.acquire_r_lock)
  file_server.register_function(sht.acquire_w_lock)
  file_server.register_function(sht.release_r_lock)
  file_server.register_function(sht.release_w_lock)
  file_server.register_function(sht.test_atomicity)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

  def acquire_r_lock(self, key, u_id):
    return self.caller.acquire_r_lock(Binary(key), Binary(u_id))
  def acquire_w_lock(self, key, u_id):
    return self.caller.acquire_w_lock(Binary(key), Binary(u_id))
  def acquire_d_lock(self, key, u_id):
    return self.caller.acquire_d_lock(Binary(key), Binary(u_id))
  def release_r_lock(self, key, u_id):
    return self.caller.release_r_lock(Binary(key), Binary(u_id))
  def release_w_lock(self, key, u_id):
    return self.caller.release_w_lock(Binary(key), Binary(u_id))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(9000, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:9000"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
