from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# $example on$
from pyspark.ml.classification import LogisticRegression
# $example off$
from pyspark.sql import SparkSession

# Load training data
spark = SparkSession.builder.appName("LogisticRegressionSummary").getOrCreate()

data = spark.read.format("libsvm").load("/home/hadoop/spark/dataset.txt")
print ("dim of the datset ",data.count())
# Split the data into train and test
splits = data.randomSplit([0.9, 0.1], 123)
train = splits[0]
test = splits[1]

# specify layers for the neural network:
# input layer of size 174 (features),
# and output of size 4 (classes)
layers = [174, 4]

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=250, layers=layers, blockSize=128, seed=1234)

# train the model
model = trainer.fit(train)

# compute accuracy on the test set
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))


#test = spark.read.format("libsvm").load("/home/hadoop/spark/article_dataset.txt")
#newpred = model.transform(test)
#newpred.show()
#spark.stop()
