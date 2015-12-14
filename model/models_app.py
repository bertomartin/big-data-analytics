"""models_app.py

to run: ${SPARK_HOME}/bin/spark-submit --master "local[*]" models_app.py
"""

from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from time import time
import pdb

sc = SparkContext("local", "Models App")

training_data_file = "/Users/roberto/Desktop/final-project/model/train_noheader.csv"
training_data_raw = sc.textFile(training_data_file)

print "Data size is {}".format(training_data_raw.count())

def parse_lines(line):
    line_split = line.split(",")
    clean_line_split = line_split[1:7]
    target = line_split[7]
    return LabeledPoint(target, array([float(x) for x in clean_line_split]))

weights = [.8, .2]
seed = 99
training_data = training_data_raw.map(parse_lines)
train_data, test_data = training_data.randomSplit(weights, seed)
print "training data count: {}".format(train_data.count())
print "testing data count: {}".format(test_data.count())
print "+" * 80
# Build the model
###############Logistic Regression Model ########################
# run training algorithm to build the model
t0 = time()
logit_model = LogisticRegressionWithLBFGS.train(train_data)
tt = time() - t0
print "Logistic Classifier trained in {} seconds".format(round(tt,3))

# compute the labels and predictions
labels_and_preds = test_data.map(lambda p: (float(logit_model.predict(p.features)), p.label  ))

# Instantiate metrics
metrics = BinaryClassificationMetrics(labels_and_preds)

# Area under precision-recall curve
print "Area under PR (Logistic) = {}".format(metrics.areaUnderPR * 100.0)

# Area under ROC curve
print "Area under ROC (Logistic) = {}".format(metrics.areaUnderROC * 100.0)

# compute accuracy
test_accuracy = labels_and_preds.filter(lambda (v,p): v == p).count() / float(test_data.count())

print "Testing accuracy {}".format(round(test_accuracy, 4))
###############/Logistic Regression #########################

#################### Decision Tree ############################

t0 = time()
dt_model = DecisionTree.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=32)

tt = time() - t0
print "+" * 80
print "Decision Tree Classifier trained in {} seconds".format(round(tt,3))

predictions = dt_model.predict(test_data.map(lambda p: p.features))
labels_and_preds = test_data.map(lambda p: p.label).zip(predictions)

# Instantiate metrics
dt_metrics = BinaryClassificationMetrics(labels_and_preds)
# Area under precision-recall curve
print "Area under PR (Decision Tree) = {}".format(dt_metrics.areaUnderPR * 100.0)

# Area under ROC curve
print "Area under ROC (Decision Tree) = {}".format(dt_metrics.areaUnderROC * 100.0)


t0 = time()
test_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count() / float(test_data.count())

print "Prediction made in {} seconds. Test accuracy is {}".format(round(tt,3), round(test_accuracy,4))
#################### /Decision Tree ##########################
