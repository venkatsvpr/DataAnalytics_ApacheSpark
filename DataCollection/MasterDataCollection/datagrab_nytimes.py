import csv
import time
import urllib2
import json
count = 0
n = 6
while (1):
    if (count > n * 950):
        break;
    #dataurl = urllib2.urlopen('http://api.wunderground.com/api/e13fb09d4cd8343c/conditions/q/14214.json')
    keys = []
    keys.append ("b460bd76fc9c4e0eba568bea68f570a2")
    keys.append ("9995690d2daa49c098c69bdd24cd80ec")
    keys.append ("f6394352039340ebb6d53343e41af8c3")
    keys.append ("87d785d705484e66aaf9d9d0f8eccc44")
    keys.append ("2d78880c8b864156a8d73e2d3ce92387")
    keys.append ("04e79ef9374b4a95a7179845c55bbfd5")
    for key in keys:
        url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?api-key='+key+'&q=business&begin_date=20160101&end_date=20160330&fl=section_name%2Cweb_url%2Clead_paragraph&page='+str(count)
        print (" Count : ",count," attempting :",url," with key",key)
        dataurl = urllib2.urlopen(url)
        data_string = dataurl.read()
        data_json = json.loads(data_string)
        breakflag = False
        for dest in data_json['response']['docs']:
            if dest == None:
                print ("breakflag setting")
                breakflag  =True;
                break;
        if (breakflag):
            print ("brekflag set breaking")
            break;
        with open("Business_Data/Month1_3/"+str(count)+".json",'w+') as out_json:
            json.dump(data_json, out_json)
            out_json.close()
        dataurl.close()
        count += 1
    print ("sleeping")
    time.sleep(15)