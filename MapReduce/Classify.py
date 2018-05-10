from __future__ import print_function
import re
import sys
from operator import add
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example on$
from pyspark.ml.classification import LogisticRegression
# $example off$
#from pyspark.sql import SparkSession
#from pyspark.ml.classification import NaiveBayes
#from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc =SparkContext()

import urllib2
import json
from urllib2 import HTTPError,URLError
from os import listdir
from bs4 import BeautifulSoup
import sys
import os
business_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Feature_Data/business_wc_sorted","r+")
sports_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Feature_Data/sports_wc_sorted","r+")
travel_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Feature_Data/travel_wc_sorted","r+")
technology_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Feature_Data/technology_wc_sorted","r+")
b_words =[]
s_words =[]
tr_words =[]
te_words =[]

wordlist = []
feature_per_set = 50
total_files = 30
count = 0;
dict = {}
for line in business_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    b_words.append(wordtokens[0])
    wordlist.append(wordtokens[0])
    count += 1

count = 0;
for line in sports_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    s_words.append(wordtokens[0])
    wordlist.append(wordtokens[0])
    count += 1


count = 0;
for line in travel_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    tr_words.append(wordtokens[0])
    wordlist.append(wordtokens[0])
    count += 1


count = 0;
for line in technology_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    te_words.append(wordtokens[0])
    wordlist.append(wordtokens[0])
    count += 1

#for index,word in enumerate(wordlist):
#    if word in business_keywords and word in technology_keywords and word in travel_keywords and word in sports_keywords:
#        wordlist.remove(word)

wordlist = set(wordlist)
print (" total features .... "+str(len(wordlist)))
os.remove("dataset.txt")
in_dataset = open("dataset.txt","w")
count = 1
flagset = True
article = 0;
file_names = [f for f in listdir("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Business_Data/Articles")]
for file in file_names:
    article += 1
    if (article > total_files):
        break;
    file = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Business_Data/Articles/"+file,"r")
    contents = file.read()
    in_dataset.write("0")
    count  =1
    for word in wordlist:
        found = re.findall(word, contents)
        total = 0
        for i in  found:
            total += 1
        if (count == 1):
            in_dataset.write(" "+str(count)+":1")
            count += 1
        if ((word in contents)):
            if (word in b_words):
                in_dataset.write(" "+str(count)+":"+str(total))
            else:
                in_dataset.write(" "+str(count)+":0")
        else:
            in_dataset.write(" "+str(count)+":0")
        count += 1
    file.close()
    in_dataset.write("\n")

article = 0
file_names = [f for f in listdir("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Technology_Data/Articles")]
for file in file_names:
    article += 1
    if (article > total_files):
        break;
    in_dataset.write("1")
    file = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Technology_Data/Articles/"+file,"r")
    contents = file.read()
    count = 1
    for word in wordlist:
        found = re.findall(word, contents)
        total = 0
        for i in  found:
            total += 1
        if (count == 1):
            in_dataset.write(" "+str(count)+":1")
            count += 1
        if ((word in contents)):
            if (word in te_words):
                in_dataset.write(" "+str(count)+":"+str(total))
            else:
                in_dataset.write(" "+str(count)+":0")
        else:
            in_dataset.write(" "+str(count)+":0")
        count += 1
    file.close()
    in_dataset.write("\n")


article = 0;
file_names = [f for f in listdir("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Travel_Data/Articles")]
for file in file_names:
    article += 1
    if (article > total_files):
        break;
    file = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Travel_Data/Articles/"+file,"r")
    contents = file.read()
    in_dataset.write("\n")
    in_dataset.write("2")
    count = 1
    for word in wordlist:
        found = re.findall(word, contents)
        total = 0
        for i in  found:
            total += 1
        if (count == 1):
            in_dataset.write(" "+str(count)+":1")
            count += 1
        if ((word in contents)):
            if (word in tr_words):
                in_dataset.write(" "+str(count)+":"+str(total))
            else:
                in_dataset.write(" "+str(count)+":0")
        else:
            in_dataset.write(" "+str(count)+":0")
        count += 1
    file.close()

article = 0
file_names = [f for f in listdir("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Sports_Data/Articles")]
for file in file_names:
    article += 1
    if (article > total_files):
        break;
    file = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/MasterDataCollection/Sports_Data/Articles/"+file,"r")
    contents = file.read()
    in_dataset.write("\n")
    in_dataset.write("3")
    count = 1
    for word in wordlist:
        found = re.findall(word, contents)
        total = 0
        for i in  found:
            total += 1
        if (count == 1):
            in_dataset.write(" "+str(count)+":1")
            count += 1
        if ((word in contents)):
            if (word in s_words):
                in_dataset.write(" "+str(count)+":"+str(total))
            else:
                in_dataset.write(" "+str(count)+":0")
        else:
            in_dataset.write(" "+str(count)+":0")
        count += 1
    file.close()
in_dataset.close()

if 0:
#if __name__ == "__main__":
    spark = SparkSession.builder.appName("LogisticRegressionSummary").getOrCreate()
    data = spark.read.format("libsvm").load("/home/hadoop/spark/dataset.txt")

    # Split the data into train and test
    splits = data.randomSplit([0.7, 0.3], 1234)
    train = splits[0]
    test = splits[1]

    # specify layers for the neural network:
    # input layer of size 4 (features), two intermediate of size 5 and 4
    # and output of size 3 (classes)
    layers = [6150, 4]

    # create the trainer and set its parameters
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)

    # train the model
    model = trainer.fit(train)

    # compute accuracy on the test set
    result = model.transform(test)
    predictionAndLabels = result.select("prediction", "label")
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
    spark.stop()
