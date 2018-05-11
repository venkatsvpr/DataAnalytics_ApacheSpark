#!/usr/bin/python
from nltk.tokenize import word_tokenize
import re
import pandas as pd
from nltk.corpus import stopwords,wordnet
import string

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

import sys
for line in sys.stdin:
    if (len(line) == 0):
        continue;
    line = line.strip()
    line = line.lower();
    new_line = [term for term in tokenize(line) if not term in stop]
    for word in new_line:
        print(word.lower()+"\t1")
