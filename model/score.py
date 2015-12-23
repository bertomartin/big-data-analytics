## to run: ${SPARK_HOME}/bin/spark-submit score.py filename
## Imports
from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import *
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel, RandomForestModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from numpy import array

import sys

## Constants

APP_NAME = "scoring earnings pop"

def print_box(cnt = 45, stars="+"):
    print stars * cnt

def parse_lines(line):
    line_split = line.split(",")
    clean_line_split = line_split[1:7]
    target = line_split[7]
    return LabeledPoint(target, array([float(x) for x in clean_line_split]))

def evaluate_model(type):
    if type == 'logistic':
        model = LogisticRegressionModel.load(sc, "logit_model.model")
    elif type == 'tree':
        model = DecisionTreeModel.load(sc, "dt_model.model")
    elif type == 'rf':
        model = RandomForestModel.load(sc, "rf_model.model")



## Driver function
def main(sc, filename):
    '''
    The driver for the spark scoring application, it generates predictions for
    a given file of features and target variables
    '''

    rawDataRdd = sc.textFile(filename)
    print "Data Size: {}".format(rawDataRdd.count())

    labeledPointsRdd = rawDataRdd.map(parse_lines)

    #load models
    logit_model = LogisticRegressionModel.load(sc, "logit_model.model")
    dt_model = DecisionTreeModel.load(sc, "dt_model.model")
    rf_model = RandomForestModel.load(sc, "rf_model.model")

    #logistic predictions
    labels_and_preds = labeledPointsRdd.map(lambda p: (float(logit_model.predict(p.features)), p.label  ))
    labels_and_preds_collected = labels_and_preds.collect()
    print "\n"
    print "Predictions: Logistic Regression"
    y_true = []
    y_pred = []
    for row in labels_and_preds_collected:
        y_true.append(row[1])
        y_pred.append(row[0])
        # print "predicted: {0} - actual: {1}\n".format(row[0], row[1])


    accuracy = labels_and_preds.filter(lambda (v,p): v == p).count() / float(labeledPointsRdd.count())

    print_box()
    print "Prediction Accuracy (Logistic): {}".format(round(accuracy, 4))
    print_box()
    print "\n"

    #decision tree predictions
    predictions = dt_model.predict(labeledPointsRdd.map(lambda p: p.features))
    labels_and_preds_dt = labeledPointsRdd.map(lambda p: p.label).zip(predictions)
    labels_and_preds_dt_collected = labels_and_preds.collect()


    accuracy_dt = labels_and_preds_dt.filter(lambda (v, p): v == p).count() / float(labeledPointsRdd.count())

    print_box()
    print "Prediction Accuracy (Decision Tree): {}".format(round(accuracy_dt, 4))
    print_box()
    print "\n"

    #random forest predictions
    predictions_rf = rf_model.predict(labeledPointsRdd.map(lambda p: p.features))
    labels_and_preds_rf = labeledPointsRdd.map(lambda p: p.label).zip(predictions_rf)
    accuracy_rf = labels_and_preds_rf.filter(lambda (v, p): v == p).count() / float(labeledPointsRdd.count())
    print_box()
    print "Prediction Accuracy (Random Forest): {}".format(round(accuracy_rf, 4))
    print_box()


if __name__ == "__main__":
    # configure options
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    filename = sys.argv[1]
    main(sc, filename)
