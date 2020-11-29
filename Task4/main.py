import itertools
import math
import sys
import time
from operator import itemgetter
from os import name, system
from statistics import mean

from matplotlib import pyplot as plt
from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD


class Main:
    # app params
    time = None
    context = None

    # algorithm params
    sessions: RDD = None
    items: RDD = None

    # input params
    n = None


    def __init__(self, filename, n=100):
        print('__init__ - BEG')
        self.context = SparkContext()

        filedata = self.context.textFile(filename)
        self.sessions = filedata.map(lambda x: self.split_line(x))
        self.items = self.sessions.flatMap(lambda x: x)

        self.n = n

        print('__init__ - END')

    def start_time(self, method_name):
        self.time = time.time()
        print(method_name + " - BEG") 

    def end_time(self, method_name):        
        elapsed_time = (time.time() - self.time)
        print(method_name + " - END in " + str(elapsed_time) + " s.")
        self.time = None
    
    def split_line(self, line):
        line_items = line.split(' ')
        while '' in line_items:
            line_items.remove('')
        return line_items

    def items_from_tuples(self, tuples) -> set:
        return set([item for t in tuples for item in t])
        
    def get_output(self, items) -> str:
        result = ""
        for i in items:
            result += '\n'
            its = ""
            for y in i[0]:
                its += "<" + str(y) + "> "
            result += its + "<" + str(i[1]) + ">"
        return result

    def write_to_file(self, filename: str, text: str):
        text_file = open(filename, "w")
        text_file.write(text)
        text_file.close()


    
    def find_items_that_occurred_more_than_n_times(self) -> RDD:
        self.start_time(method_name="find_items_that_occurred_more_than_n_times")
        item_counts = self.items.map(lambda x: (x, 1))
        item_counts = item_counts.reduceByKey(lambda x, y: x + y)

        self.end_time(method_name="find_items_that_occurred_more_than_n_times")
        return item_counts.filter(lambda x: x[1] >= self.n)

    def find_k_tuples_that_occurred_more_than_n_times(self, items: [], k: int) -> {}: 
        self.start_time(method_name="find_k_tuples_that_occurred_more_than_n_times")       
        tuples = {}
        for c in itertools.combinations(sorted(items), k):
            tuples[c] = 0

        for session in self.sessions.toLocalIterator():
            session_subset = sorted([item for item in session if item in items])
            for item in itertools.combinations(session_subset, k):
                if item in tuples:
                    tuples[item] += 1

        self.end_time(method_name="find_k_tuples_that_occurred_more_than_n_times")
        return dict(filter(lambda x: x[1] >= self.n, tuples.items()))
        
    def calculate_confidence_for_doubles(self, doubles: {}, singles: {}) -> []:
        self.start_time(method_name="calculate_confidence_for_doubles")     
        confidence = []
        for key, value in doubles.items():
            confidence.append(((key[0], key[1]), value / singles[key[0]]))
            confidence.append(((key[1], key[0]), value / singles[key[1]]))

        # Leksykalne sortowanie
        confidence.sort(key=itemgetter(0), reverse= False)
        # Sortowanie po wartościach
        confidence.sort(key=itemgetter(1), reverse=True)

        self.end_time(method_name="calculate_confidence_for_doubles")
        return confidence

    def calculate_confidence_for_triples(self, triples: {}, doubles: {}) -> []:
        self.start_time(method_name="calculate_confidence_for_triples")     
        confidence = []
        for key, value in triples.items():
            for c in itertools.combinations(key, 2):
                if c in doubles:
                    pred = list(c)
                    succ = list(set(key) - set(c))
                    confidence.append((tuple(pred + succ), value / doubles[c]))
        
        # Leksykalne sortowanie
        confidence.sort(key=itemgetter(0), reverse= False)
        # Sortowanie po wartościach
        confidence.sort(key=itemgetter(1), reverse=True)
        
        self.end_time(method_name="calculate_confidence_for_triples")
        return confidence



    def main(self):
        singles = dict(self.find_items_that_occurred_more_than_n_times().collect())
        doubles = self.find_k_tuples_that_occurred_more_than_n_times(singles, 2)
        triples = self.find_k_tuples_that_occurred_more_than_n_times(self.items_from_tuples(singles), 3)
        
        doubles_confidence = self.calculate_confidence_for_doubles(doubles, singles)
        triples_confidence = self.calculate_confidence_for_triples(triples, doubles)

        doubles_result = self.get_output(doubles_confidence[:5])
        triples_result = self.get_output(triples_confidence[:5])

        self.write_to_file("dobules.txt", doubles_result)
        self.write_to_file("triples.txt", triples_result)

        self.context.stop()


main = Main(filename="4.txt", n=100)
main.main()
