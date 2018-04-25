import csv
import time
import urllib2
import json
count = 201
while (1):
    if (count > 900):
        break;
    #dataurl = urllib2.urlopen('http://api.wunderground.com/api/e13fb09d4cd8343c/conditions/q/14214.json')
    key1 = 'f6394352039340ebb6d53343e41af8c3'
    key2 = '9995690d2daa49c098c69bdd24cd80ec'
    url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key='+key2+'&q=trump&begin_date=20180301&end_date=20180405&fl=lead_paragraph%2Cabstract%2Cheadline%2Csnippet%2Cweb_url&page='+str(count)
    print (" Count : ",count," attempting :",url)
    dataurl = urllib2.urlopen(url)
    data_string = dataurl.read()
    data_json = json.loads(data_string)
    breakflag = False
    for dest in data_json['response']['docs']:
        if dest == None:
            breakflag  =True;
            break;
    if (breakflag):
        print ("brekflag set breaking")
        break;
    with open("trump_2018_mar1_till_now/"+str(count)+".json",'w') as out_json:
        json.dump(data_json, out_json)
        out_json.close()
    dataurl.close()
    count+=1
    time.sleep(15)
