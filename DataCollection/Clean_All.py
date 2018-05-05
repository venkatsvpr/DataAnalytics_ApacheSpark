import urllib2
import json
from os import listdir
from bs4 import BeautifulSoup
import string
import sys

def getHTMLData(link):

    try:
        f_data = urllib2.urlopen(html)
        return f_data
    except:
        return None

sections = ['Business','Sports','World','Science','Health','Politics','Tech']
count = 1

file_names = [f for f in listdir("Data_Collected/JSONs")]

for name in file_names:
    data = open('Data_Collected/JSONs/' + name).read()
    frame_data = json.loads(data)
    
    for item in range(len(frame_data['response']['docs'])):
        section = frame_data['response']['docs'][item]['section_name']

        if section in sections:
            html = frame_data['response']['docs'][item]['web_url']
            f = getHTMLData(html)
            
            if(f!=None):
                ff = open("Data_Collected/"+section+"/"+str(count)+".txt","w+")
                count=count+1
                html_doc = f.read()
                soup = BeautifulSoup(html_doc, 'html.parser')
        
                for article in soup.find_all('article'):
                    for para in article.find_all('p'):
                        line = para.get_text().encode('utf-8')
                        ff.write(line)
                ff.close()