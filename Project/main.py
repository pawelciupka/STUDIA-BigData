from config import Config
from classification import Classification
from clustering import Clustering
from result_presentation import ResultPresentation
from pyspark.shell import spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder


class Main:
    classification_df = None
    clustering_df = None
    processed_data = None

    training_data = None
    test_data = None
    
    def __init__(self, filename, classification_columns, clustering_columns):
        self.classification_df = spark.read.option("delimiter", ";").csv(filename, inferSchema=True, header=True).select(classification_columns)
        self.classification_df = self.classification_df.dropna()

        self.clustering_df = spark.read.option("delimiter", ";").csv(filename, inferSchema=True, header=True).select(clustering_columns)
        self.clustering_df = self.clustering_df.dropna()

    def process_classification_data(self):
        # change categorical columns values to numbers
        labelCol = 'success'
        featuresCols = self.classification_df.columns
        featuresCols.remove(labelCol)

        columns_in = featuresCols
        columns_out = []
        for col in featuresCols:
            columns_out.append(col + '_')

        indexers = [StringIndexer(inputCol=x, outputCol=x + '_tmp')
                    for x in columns_in]

        encoders = [OneHotEncoder(dropLast=False, inputCol=x + '_tmp', outputCol=y)
                    for x, y in zip(columns_in, columns_out)]
        tmp = [[i, j] for i, j in zip(indexers, encoders)]
        tmp = [i for sublist in tmp for i in sublist]

        # prepare labeled sets
        cols_now = []
        for col in featuresCols:
            if col in columns_in:
                cols_now.append(col + '_')
            else:
                cols_now.append(col)

        assembler_features = VectorAssembler(inputCols=cols_now, outputCol='features')
        labelIndexer = StringIndexer(inputCol='success', outputCol="label")
        tmp += [assembler_features, labelIndexer]
        pipeline = Pipeline(stages=tmp)
        pipeline_df = pipeline.fit(self.classification_df).transform(self.classification_df)

        self.processed_data = pipeline_df
        self.processed_data.cache()

    def process_clustering_data(self):
        featuresCols = self.clustering_df.columns
        vecAssembler = VectorAssembler(inputCols=featuresCols, outputCol="features")
        df = vecAssembler.transform(self.clustering_df)
        self.clustering_df = df

    def split_data(self, training_part):
        self.training_data, self.test_data = self.processed_data.randomSplit([training_part, 1-training_part], seed=0)

    def classify(self):
        print("\nclassify")

        self.process_classification_data()
        self.split_data(0.7)

        c = Classification(self.training_data, self.test_data)
        c.decision_tree_classifier()
        c.random_forest_classifier()
        c.naive_bayes()
        c.logistic_regression()
        c.gbtc()
        c.lsvc()

    def clusterize(self):
        print("\nclusterize")
        
        self.process_clustering_data()

        c = Clustering(self.clustering_df)
        c.k_means(2)
        c.k_means(3)
        c.k_means(4)

    def get_columns_results(self):
        result_presentation = ResultPresentation(self.classification_df)
        result_presentation.get_value_count_for_columns(Config.detailed_columns)

    def run_all(self):
        self.classify()
        self.clusterize()



main = Main("data/global_terrorism_db_all_from_2000.csv", Config.classification_columns, Config.clustering_columns)
# main.get_columns_results()
main.run_all()

