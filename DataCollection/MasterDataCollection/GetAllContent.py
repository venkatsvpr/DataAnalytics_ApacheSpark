#!/usr/bin/python
import urllib2
import json
from urllib2 import HTTPError,URLError
from os import listdir
from bs4 import BeautifulSoup
import sys
fw = open(sys.path[0]+"/all.txt", "w")
file_names = [f for f in listdir(sys.path[0]+"/Travel_Data/Articles")]
for name in file_names:
    with open(sys.path[0]+"/Travel_Data/Articles/"+name,"r") as fr:
        fw.writelines(l for l in fr)

file_names = [f for f in listdir(sys.path[0]+"/Technology_Data/Articles")]
for name in file_names:
    with open(sys.path[0]+"/Technology_Data/Articles/"+name,"r") as fr:
        fw.writelines(l for l in fr)

file_names = [f for f in listdir(sys.path[0]+"/Sports_Data/Articles")]
for name in file_names:
    with open(sys.path[0]+"/Sports_Data/Articles/"+name,"r") as fr:
        fw.writelines(l for l in fr)

file_names = [f for f in listdir(sys.path[0]+"/Business_Data/Articles")]
for name in file_names:
    with open(sys.path[0]+"/Business_Data/Articles/"+name,"r") as fr:
        fw.writelines(l for l in fr)
