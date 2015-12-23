
## Imports
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from time import time
import sys
from sklearn import metrics as sk_metrics

# constants
APP_NAME = "earnings pop"

def print_box(cnt = 45, stars="+"):
    print stars * cnt

def parse_lines(line):
    line_split = line.split(",")
    clean_line_split = line_split[1:7]
    target = line_split[7]
    return LabeledPoint(target, array([float(x) for x in clean_line_split]))

def get_data(sc, filename):
    weights = [.8, .2]
    data_raw = sc.textFile(filename)
    labeled_points = data_raw.map(parse_lines)
    train, test = labeled_points.randomSplit(weights)
    return train, test

def create_model(name, training):
    if name == 'logistic':
        print_box()
        print "Logistic Regression Model"
        print_box()
        model = LogisticRegressionWithLBFGS.train(training)
    elif name == 'tree':
        print_box()
        print "Decision Tree Model"
        print_box()
        model = DecisionTree.trainClassifier(training, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=32)
    elif name == 'rf':
        print_box()
        print "Random Forest Model"
        print_box()
        model = RandomForest.trainClassifier(training, numClasses=2, categoricalFeaturesInfo={},
                                    numTrees=15, featureSubsetStrategy="auto", impurity='gini', maxDepth=5, maxBins=50)

    return model

def evaluate_model(labels, predictions):
    '''
    using labels and predictions, evaluate with scikit-learn
    '''
    test_accuracy = sk_metrics.accuracy_score(labels, predictions)
    print "Testing accuracy {}".format(round(test_accuracy, 4))
    print "Confusion Matrix"
    print_box(stars="-")
    print sk_metrics.confusion_matrix(labels, predictions)
    target_names = ['not-beat:0', 'beat:1']
    print sk_metrics.classification_report(labels, predictions, target_names=target_names)
    print_box(stars="-")



def main(sc, filename):

    training, testing = get_data(sc, filename)
    model = create_model('rf', training)
    # model.save(sc, "rf_model.model")
    # model.save(sc, "rf_model.model")
    # model.save(sc, "logit_model.model")

    predictions = model.predict(testing.map(lambda p: p.features))
    labs = testing.map(lambda p: p.label)
    labels_and_preds = labs.zip(predictions)

    labels_and_predictions_collected = labels_and_preds.collect()

    # extract labels and predictions from test data
    labels = map(lambda p: p[0], labels_and_predictions_collected)
    predictions = map(lambda p: p[1], labels_and_predictions_collected)

    # evaluate model
    evaluate_model(labels, predictions)


if __name__ == "__main__":
    # configure options
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]") #can be replaced with remote master yarn
    sc = SparkContext(conf=conf)
    filename = sys.argv[1]
    main(sc, filename)
