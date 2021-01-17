from pyspark.ml.clustering import KMeans, BisectingKMeans


class Clustering:
    df = None

    def __init__(self, df):
        self.df = df

    def k_means(self, k):
        print('\nK-Means - ' + str(k))
        kmeans = KMeans().setK(k).setSeed(1)
        model = kmeans.fit(self.df.select('features'))

        transformed = model.transform(self.df)
        transformed.groupBy("prediction").count().show()

        centers = model.clusterCenters()
        self.print_centers(centers)

    def bisecting_k_means(self, k):
        print('\nBisecting K-Means - ' + str(k))
        kmeans = BisectingKMeans().setK(k).setSeed(1)
        model = kmeans.fit(self.df.select('features'))

        transformed = model.transform(self.df)
        transformed.groupBy("prediction").count().show()

        centers = model.clusterCenters()
        self.print_centers(centers)

    def print_centers(self, centers):
        for center in centers:
            print(center)
            

