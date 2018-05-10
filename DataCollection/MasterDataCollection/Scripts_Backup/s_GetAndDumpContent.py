#!/usr/bin/python
import urllib2
import json
from urllib2 import HTTPError,URLError
from os import listdir
from bs4 import BeautifulSoup
import sys
if (sys.argv[1] is None):
    pass

if (sys.argv[2] is None):
    pass

matchString = sys.argv[2]

file_names = [f for f in listdir(sys.path[0]+"/"+sys.argv[1])]
count = 0
fulff = open(sys.path[0]+"/s_full.txt","w+")
for name in file_names:
    if not "json" in name:
        continue;
    abs_filepath = sys.path[0]+"/"+sys.argv[1]+"/"+name
    print ("looking into file "+abs_filepath)
    data = open(abs_filepath).read()
    frame_data = json.loads(data)
    for item in range(len(frame_data['response']['docs'])):
        if (count < 3617):
            count += 1
            continue;
        html = frame_data['response']['docs'][item]['web_url']
        if 'section_name' not in frame_data['response']['docs'][item]:
            continue;
        section  = frame_data['response']['docs'][item]['section_name']
        print ("html :",html," section:",section)
        if (matchString.lower() != section.lower()):
            print ("continuing...")
            continue;
        if ("http" not in html):
            continue;
        try: f = urllib2.urlopen(html)
        except URLError as e:
            print ("url with error "+html)
            continue;
        except HTTPError as e:
            print ("http erorr with "+url)
            continue;
        html_doc = f.read()
        soup = BeautifulSoup(html_doc, 'html.parser')
        ff = open(sys.path[0]+"/s/"+str(count)+".txt","w+")
        for article in soup.find_all('article'):
            for para in article.find_all('p'):
                ff.write(para.get_text().encode('utf-8'))
                fulff.write(para.get_text().encode('utf-8'))
        ff.close()
        count += 1
fulff.close()
