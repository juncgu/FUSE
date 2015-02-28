#!/usr/bin python
"""
Name: Juncheng Gu
UFID: 5191-0572

Description:
test-client.py connects to simpleht.py and does the following operations
Stores and retrieves an integer
Stores and retrieves a string
Stores and retrieves a list
Stores and retrieves a dictionary
Use the write_file and read_file functions of the simpleht to save/restore data to/from the filesystem
"""
import xmlrpclib, unittest
from xmlrpclib import Binary
import pickle

serv_proxy = xmlrpclib.ServerProxy('http://localhost:9002')

serv_proxy.test_atomicity(Binary("2"))

