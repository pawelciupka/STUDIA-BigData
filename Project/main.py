import classification
from pyspark.shell import spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder

class Main:
    columns = None
    df = None
    processed_data = None

    training_data = None
    test_data = None
    
    def __init__(self, filename, columns):
        self.columns = columns
        self.df = spark.read.csv(filename, inferSchema=True, header=True).select(columns)
        self.df = self.df.dropna()

    def process_classification_data(self):
        # change categorical columns values to numbers
        labelCol = 'success'
        featuresCols = self.df.columns
        featuresCols.remove(labelCol)

        columns_in = featuresCols
        columns_out = []
        for col in featuresCols:
            columns_out.append(col + '-  ')

        indexers = [StringIndexer(inputCol=x, outputCol=x + '_tmp')
                    for x in columns_in]

        encoders = [OneHotEncoder(dropLast=False, inputCol=x + "_tmp", outputCol=y)
                    for x, y in zip(columns_in, columns_out)]
        tmp = [[i, j] for i, j in zip(indexers, encoders)]
        tmp = [i for sublist in tmp for i in sublist]

        # prepare labeled sets
        cols_now = []
        for col in featuresCols:
            if col in columns_in:
                cols_now.append(col + '-  ')
            else:
                cols_now.append(col)

        assembler_features = VectorAssembler(inputCols=cols_now, outputCol='features')
        labelIndexer = StringIndexer(inputCol='success', outputCol="label")
        tmp += [assembler_features, labelIndexer]
        pipeline = Pipeline(stages=tmp)
        pipeline_df = pipeline.fit(self.df).transform(self.df)

        self.processed_data = pipeline_df
        self.processed_data.cache()

    def split_data(self, training_part):
        self.training_data, self.test_data = self.processed_data.randomSplit([training_part, 1-training_part], seed=0)

    def classify(self):
        print("\nclassifying")
        c = classification.Classification(self.training_data, self.test_data)
        c.decision_tree_classifier()
        c.random_forest_classifier()

    def clusterize(self):
        print("\nclustering")

    def run_all(self):
        self.process_classification_data()
        self.split_data(0.7)
        self.classify()
        self.clusterize()


columns = ['country_txt', 'region_txt', 'suicide', 'attacktype1_txt', 'targtype1_txt', 'natlty1_txt', 'gname', 'weaptype1_txt', 'nkill', 'success']

main = Main("global_terrorism_db.csv", columns)
main.run_all()

