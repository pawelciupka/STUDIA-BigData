import pandas as pd

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, LogisticRegression, NaiveBayes, MultilayerPerceptronClassifier, LinearSVC

pd.set_option("display.max_rows", None, "display.max_columns", None)

class Classification:
    training_data = None
    test_data = None

    label_col="label"
    features_col = "features"

    def __init__(self, training_data, test_data):
        self.training_data = training_data
        self.test_data = test_data

    def logistic_regression(self, maxIter=10):
        print('\nLogistic Regression')
        lr = LogisticRegression(labelCol=self.label_col, featuresCol=self.features_col, maxIter=maxIter)
        self.classify('logistic_regression', lr)

    def decision_tree_classifier(self):
        print('\nDecision tree classifier')
        dt = DecisionTreeClassifier(labelCol=self.label_col, featuresCol=self.features_col)
        self.classify('decision_tree_classifier', dt, True)
    
    def random_forest_classifier(self, num_of_tree=12):
        print('\nRandom forest classifier')
        rf = RandomForestClassifier(labelCol=self.label_col, featuresCol=self.features_col, numTrees=num_of_tree)
        self.classify('random_forest_classifier', rf, True)

    def naive_bayes(self, smoothing=1, modelType='multinomial'):
        print('\nNaive Bayes')
        nb = NaiveBayes(labelCol=self.label_col, featuresCol=self.features_col, smoothing=smoothing, modelType=modelType)
        self.classify('naive_bayes', nb)

    def gbtc(self, maxIter=10):
        print('\nGradient-boosted tree classifier')
        gbtc = GBTClassifier(labelCol=self.label_col, featuresCol=self.features_col, maxIter=maxIter)
        self.classify('gbtc', gbtc, True)

    def mlpc(self, maxIter=100, blockSize=128, seed=1234):
        print('\nMultilayer perceptron classifier')
        layers = [4, 5, 4, 2]
        # specify layers for the neural network:
        # input layer of size 4 (features), two intermediate of size 5 and 4
        # and output of size 3 (classes)
        mlpc = MultilayerPerceptronClassifier(maxIter=maxIter, layers=layers, blockSize=blockSize, seed=seed)
        self.classify('mlpc', mlpc)

    def lsvc(self, maxIter=10, regParam=0.1):
        print('\nLinear Support Vector Machine')
        lsvc = LinearSVC(maxIter=maxIter, regParam=regParam)        
        self.classify('lsvc', lsvc)


    def classify(self, name, classifier, do_importances=False):
        model = self.evaluate(classifier)

        if do_importances:
            importances = model.featureImportances
            importances_sorted = self.extract(importances, self.training_data, self.features_col)
            self.print_and_save_importances(name, importances_sorted)


    def evaluate(self, classifier):
        model = classifier.fit(self.training_data)
        predictions = model.transform(self.test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol=self.label_col, predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print("Test Error Value = %g" % (1.0 - accuracy))
        print("Accuracy Value = %g " % accuracy)
        return model


    def extract(self, feature, dataset, featuresCol):
        extracts = []
        for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
            extracts = extracts + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
        df_extracts = pd.DataFrame(extracts)
        df_extracts['score'] = df_extracts['idx'].apply(lambda x: feature[x])        
        indexNames = df_extracts[ df_extracts['score'] <= 0.01 ].index
        df_extracts.drop(indexNames, inplace=True)
        return(df_extracts.sort_values('score', ascending = False))


    def print_and_save_importances(self, name, importances, do_print=True, do_save=True):
        if do_print:
            print(importances)
        if do_save:
            self.save_to_file(name, str(importances))

    def save_to_file(self, filename, data):
        f = open('classification_results/' + filename + ".txt", "w")
        f.write(data)
        f.close()
