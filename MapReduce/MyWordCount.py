from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession

from nltk.tokenize import word_tokenize
import re
import pandas as pd
from nltk.corpus import stopwords,wordnet
import string
import os
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
    return tokens

punctuation = list(string.punctuation)
stop = stopwords.words('english') + punctuation + ['rt','RT', 'via', 'new' , 'york' , 'one' , 'like' , 'would','also','us','ms','page','think','time', 'first','last','get','even','subscribe','days','city','mr','years','year','p','amp','man','video','live','please','love','much','could','u']

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    output_file = open("output_file.txt","w+")
    clean_file = open("cleanfile.txt","w+")
    input_file = open(sys.argv[1],"r")
    output_wc = open("output_wc","w+")
    for line in input_file:
        list_words = [term for term in tokenize(line) if not term in stop]
        clean_file.write(" ".join(list_words))

    content = spark.read.text("cleanfile.txt")
    lines = content.rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()

    WordList = []
    for (word, count) in output:
        output_wc.write("%s: %i\n" % (word, count))
        WordList.append(word)
    output_file.write(" ".join(WordList))
    os.remove("cleanfile.txt")
    spark.stop()
