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


file = open("featureset.txt","r+")
FeatureSet = []
for line in file:
    word = line.replace("\n","")
    FeatureSet.append(word)
print ("feature ",FeatureSet)


#############################
### Feature Selection
##############################
stemmer = PorterStemmer()
if __name__ == "__main__":
    #spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    conf = SparkConf().setAppName("hello").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    os.remove("article_dataset.txt")
    ofile = open("article_dataset.txt","w+")
    textRDD = sc.textFile("article.txt")
    words = textRDD.flatMap(lambda s: word_tokenize(s))
    stems = words.map(lambda w: stemmer.stem(w))
    clean = stems.filter(lambda s: s not in stop)
    counts = clean.flatMap(lambda x: x.split(' ')) \
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(add)
    output = counts.collect()
    c = collections.Counter()
    for (word, count) in output:
        word2 = unicodedata.normalize('NFKD', word).encode('ascii','ignore')
        c[word2] = count

    ofile.write(str(0))
    feature_count = 0;
    for word in FeatureSet:
        feature_count += 1
        if (c[word] >0):
            ofile.write(" "+str(feature_count)+":"+str(c[word]))
        else:
            ofile.write(" "+str(feature_count)+":0")
    ofile.write("\n")
    ofile.close();
    sc.stop()
