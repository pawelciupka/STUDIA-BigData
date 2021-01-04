import pandas as pd

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier

pd.set_option("display.max_rows", None, "display.max_columns", None)

class Classification:
    training_data = None
    test_data = None

    label_col="label"
    features_col = "features"

    def __init__(self, training_data, test_data):
        self.training_data = training_data
        self.test_data = test_data

    def decision_tree_classifier(self):
        print('\nDecision tree classifier')

        dt = DecisionTreeClassifier(labelCol=self.label_col, featuresCol=self.features_col)
        model = self.evaluate(dt)

        importances = model.featureImportances
        importances_sorted = self.extract(importances, self.training_data, self.features_col)
        print(importances_sorted)
        self.save_to_file('decision_tree_classifier', str(importances_sorted))
    
    def random_forest_classifier(self, num_of_tree=12):
        print('\nRandom forest classifier')

        rf = RandomForestClassifier(labelCol=self.label_col, featuresCol=self.features_col, numTrees=num_of_tree)
        model = self.evaluate(rf)

        importances = model.featureImportances
        importances_sorted = self.extract(importances, self.training_data, self.features_col)
        print(importances_sorted)
        self.save_to_file('decision_tree_classifier', str(importances_sorted))


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


    def save_to_file(self, filename, data):
        f = open(filename+".txt", "w")
        f.write(data)
        f.close()
