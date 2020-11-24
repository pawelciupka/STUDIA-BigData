import itertools
import time
from pyspark import SparkConf, SparkContext
import math
import sys
from statistics import mean
from matplotlib import pyplot as plt

class Main:
    # app params
    time = None
    context = None

    # algorithm params
    points = None
    centroids = None
    k = None
    costs = None
    get_distance = None
    get_cost = None

    # input params
    iterations = None
    metric = None


    def __init__(self, points_filename, clusters_filename, iterations = 3, metric = 'euclidean'):
        print('__init__ - BEG')
        conf = SparkConf()
        self.context = SparkContext('local[4]', '', conf=conf)

        self.points = self.context.textFile(points_filename).map(lambda line: line.split(" ")).map(lambda xs: [float(x) for x in xs])
        self.centroids = self.context.textFile(clusters_filename).map(lambda line: line.split(" ")).map(
            lambda xs: [float(x) for x in xs]).collect()
        self.k = len(self.centroids)
        self.iterations = iterations
        self.metric = metric

        print('__init__ - END')

    def start_time(self, method_name):
        self.time = time.time()
        print(method_name + " - BEG") 

    def end_time(self, method_name):        
        elapsed_time = (time.time() - self.time)
        print(method_name + " - END in " + str(elapsed_time) + " s.")
        self.time = None


    
    def assign_centroids(self, point):
        self.start_time(method_name="assign_centroids")
        closest_centroid_id = 0
        currently_closest_distance = sys.maxsize
        for j in range(self.k):
            distance = self.get_distance(point, self.centroids[j])
            if distance < currently_closest_distance:
                closest_centroid_id = j
                currently_closest_distance = distance

        self.end_time(method_name="assign_centroids")
        return closest_centroid_id, point

    def euclidean_distance(self, point1, point2):
        self.start_time(method_name="euclidean_distance")
        distance = 0
        for i in range(len(point1)):
            distance += (point1[i] - point2[i]) ** 2

        self.end_time(method_name="euclidean_distance")
        return math.sqrt(distance)

    def manhattan_distance(self, point1, point2):
        self.start_time(method_name="manhattan_distance")
        distance = 0
        for i in range(len(point1)):
            distance += math.fabs(point1[i] - point2[i])

        self.end_time(method_name="manhattan_distance")
        return distance

    def euclidean_cost_for_point(self, point):
        self.start_time(method_name="euclidean_cost_for_point")
        lowest_cost_for_point = sys.maxsize
        for i in range(self.k):
            current_centroid_cost = self.euclidean_distance(point, self.centroids[i]) ** 2
            if current_centroid_cost < lowest_cost_for_point:
                lowest_cost_for_point = current_centroid_cost

        self.end_time(method_name="euclidean_cost_for_point")
        return lowest_cost_for_point

    def manhattan_cost_for_point(self, point):
        self.start_time(method_name="manhattan_cost_for_point")
        lowest_cost_for_point = sys.maxsize
        for i in range(self.k):
            current_centroid_cost = self.manhattan_distance(point, self.centroids[i])
            if current_centroid_cost < lowest_cost_for_point:
                lowest_cost_for_point = current_centroid_cost

        self.end_time(method_name="manhattan_cost_for_point")
        return lowest_cost_for_point


    def main(self):        
        if self.metric == 'euclidean':
            self.get_distance = self.euclidean_distance
            self.get_cost = self.euclidean_cost_for_point
        else:
            self.get_distance = self.manhattan_distance
            self.get_cost = self.manhattan_cost_for_point

        for i in range(self.iterations):
            index_point_pairs = self.points.map(lambda point: self.assign_centroids(point)).groupByKey().cache()

            iteration_cost = self.points.map(lambda point: self.get_cost(point)).sum()
            self.costs.append(iteration_cost)


            for j in range(self.k):
                points_for_centroid = index_point_pairs.filter(lambda pair: pair[0] == j).map(lambda x: x[1]).collect()[0]
                for n in range(len(self.centroids[j])):
                    self.centroids[j][n] = mean([row[n] for row in points_for_centroid])

        print(self.costs)
        plt.plot(range(1, self.iterations + 1), self.costs)
        plt.show()
        print()


points_filename_1 = "./3a.txt"
clusters_filename_1 = "./3b.txt"
clusters_filename_2 = "./3c.txt"
iterations = 3
# euclidean || manhattan
metric = "euclidean"

main = Main(points_filename=points_filename_1, clusters_filename=clusters_filename_1, iterations=iterations, metric=metric)
main.main()
