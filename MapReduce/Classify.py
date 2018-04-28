from __future__ import print_function

import sys
from operator import add
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc =SparkContext()


business_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/wc_business_list","r+")
sports_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/wc_sports_list","r+")
travel_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/wc_travel_list","r+")
technology_keywords = open("/media/sf_SharedVm_MapReduce/DataAnalytics_ApacheSpark/DataCollection/wc_technology_list","r+")
b_words =[]
s_words =[]
tr_words =[]
te_words =[]

wordlist = []
feature_per_set = 1000
count = 0;
for line in business_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    wordlist.append(wordtokens[0])
    b_words.append(wordtokens[0])
    count += 1

count = 0;
for line in sports_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    wordlist.append(wordtokens[0])
    s_words.append(wordtokens[0])
    count += 1


count = 0;
for line in travel_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    wordlist.append(wordtokens[0])
    tr_words.append(wordtokens[0])
    count += 1


count = 0;
for line in technology_keywords:
    if (count >= feature_per_set):
        break;
    wordtokens = line.split(" ")
    wordtokens[0].replace(":","")
    wordlist.append(wordtokens[0])
    te_words.append(wordtokens[0])
    count += 1

wordlist = set(wordlist)

in_dataset = open("dataset.txt","w")
in_dataset.write("0")
count = 1
for word in wordlist:
    if word in b_words:
        in_dataset.write(" "+str(count)+":1")
    else:
        in_dataset.write(" "+str(count)+":0")
    count += 1
in_dataset.write("\n")

in_dataset.write("1")
count = 1
for word in wordlist:
    if word in s_words:
        in_dataset.write(" "+str(count)+":1")
    else:
        in_dataset.write(" "+str(count)+":0")
    count += 1
count =1
in_dataset.write("\n")

in_dataset.write("2")
for word in wordlist:
    if word in te_words:
        in_dataset.write(" "+str(count)+":1")
    else:
        in_dataset.write(" "+str(count)+":0")
    count += 1
count = 1
in_dataset.write("\n")

in_dataset.write("3")
for word in wordlist:
    if word in tr_words:
        in_dataset.write(" "+str(count)+":1")
    else:
        in_dataset.write(" "+str(count)+":0")
    count += 1
in_dataset.write("\n")
in_dataset.close()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MulticlassLogisticRegressionWithElasticNet").getOrCreate()

    # Load training data
    data = spark.read.format("libsvm").load("dataset.txt")

    # Split the data into train and test
    splits = data.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # train the model
    model = nb.fit(data)

    # select example rows to display.
    predictions = model.transform(data)
    predictions.show()
    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = " + str(accuracy))
    spark.stop ()
