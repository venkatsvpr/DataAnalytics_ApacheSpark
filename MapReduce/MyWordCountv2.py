# Code referred from spark examples
# just the idea to parse unicode characs -  https://stackoverflow.com/questions/16467479/normalizing-unicode
#
from __future__ import print_function
import  nltk
nltk.download('punkt')
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from nltk.tokenize import word_tokenize
from nltk.stem import *
import re
import pandas as pd
from nltk.corpus import stopwords,wordnet
import string
import os
import unicodedata
import collections
emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]

delete_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r'[^\x00-\x7f]' #hex characters
    #r"(?:[a-z][a-z'\-_]+[a-z])" # words with - and '
    #r'(?:[\w_]+)', # other words
    #r'(?:\S)' # anything else
]

tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
delete_re = re.compile(r'('+'|'.join(delete_str)+')', re.VERBOSE | re.IGNORECASE)

def tokenize(s):
    tokens = tokens_re.findall(s)
    tokens = [token for token in tokens if not delete_re.search(token)]
    tokens2 = [stemmer.stem(w) for w in tokens]
    return set(tokens2)

punctuation = list(string.punctuation)
stop = stopwords.words('english') + punctuation + ['would','could','I','hi','tell','2016','We','want','2016','2017','2015','2014','','wa','said', 'thi','time','girl','live','citi','two','print']
CommonPath="/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/"
CategoryPaths = [CommonPath+"Sports_Data/Articles", CommonPath+"Travel_Data/Articles", CommonPath+"Technology_Data/Articles", CommonPath+"Business_Data/Articles"]

##############################
### Feature Selection
##############################
number_of_files = 3500
features_per_class = 75
stemmer = PorterStemmer()
Feature_Set= []
if __name__ == "__main__":
    #spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    conf = SparkConf().setAppName("hello").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    for path in CategoryPaths:
        #print ("Reading category : ",path)
        textRDD = sc.textFile(path)
        words = textRDD.flatMap(lambda s: word_tokenize(s))
        stems = words.map(lambda w: stemmer.stem(w))
        clean = stems.filter(lambda s: s not in stop)
        counts = clean.flatMap(lambda x: x.split(' ')) \
                        .map(lambda x: (x, 1)) \
                        .reduceByKey(add)
        sort_counts = counts.sortBy(lambda a: -a[1])
        output = sort_counts.take(features_per_class)
        for (word, count) in output:
            word2 = unicodedata.normalize('NFKD', word).encode('ascii','ignore')
            Feature_Set.append(word2)
        #print (" Category Feature Set :",Feature_Set)
    Feature_Set = list(set(Feature_Set))
    print ("Feature Set :",Feature_Set)

    feat_file = open("featureset.txt","w+")
    for word in Feature_Set:
        feat_file.write(word)
        feat_file.write("\n")
    feat_file.close()

    class_id = -1
    os.remove("dataset.txt")
    ofile = open("dataset.txt","w+")
    for path in CategoryPaths:
        class_id += 1
        file_names = [f for f in os.listdir(path)]
        file_count = -1
        for file in file_names:
            file_count += 1
            if (file_count > number_of_files):
                continue;
            #print ("Reading file ",file)
            textRDD = sc.textFile(path+"/"+file)
            words = textRDD.flatMap(lambda s: word_tokenize(s))
            stems = words.map(lambda w: stemmer.stem(w))
            clean = stems.filter(lambda s: s not in stop)
            counts = clean.flatMap(lambda x: x.split(' ')) \
                        .map(lambda x: (x, 1)) \
                        .reduceByKey(add)
            output = counts.collect()
            c = collections.Counter()
            #print ("beofre ",c)
            for (word, count) in output:
                word2 = unicodedata.normalize('NFKD', word).encode('ascii','ignore')
                c[word2] = count
            #print ("file ",path,file," counter ",c)
            ofile.write(str(class_id))
            feature_count = 0;

            for word in Feature_Set:
                feature_count += 1
                if (c[word] >0):
                    ofile.write(" "+str(feature_count)+":"+str(c[word]))
                else:
                    ofile.write(" "+str(feature_count)+":0")
            for key in c:
                c[key] = 0;
            ofile.write("\n")
    ofile.close();
    sc.stop()
