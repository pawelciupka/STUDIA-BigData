import itertools
import time
from pyspark import SparkConf, SparkContext

class Main:
    time = None
    context = None

    def __init__(self):
        print('__init__ - BEG')
        conf = SparkConf()
        self.context = SparkContext(conf=conf)

        print('__init__ - END')

    def start_time(self, method_name):
        self.time = time.time()
        print(method_name + " - BEG") 

    def end_time(self, method_name):        
        elapsed_time = (time.time() - self.time)
        print(method_name + " - END in " + str(elapsed_time) + " s.")
        self.time = None



    def load_lines(self, filename):
        self.start_time(method_name="load_lines")
        lines = self.context.textFile(filename)
        self.end_time(method_name="load_lines")
        return lines



    def convert_lines_to_user_with_friends_list(self, lines):
        self.start_time(method_name="convert_lines_to_user_with_friends_list")
        user_with_friends_list = lines.map(Main.line_to_user_with_friends_tuple)
        self.end_time(method_name="convert_lines_to_user_with_friends_list")
        return user_with_friends_list

    @staticmethod
    def line_to_user_with_friends_tuple(line):        
        splitted_line = line.split()
        user_id = int(splitted_line[0])

        if len(splitted_line) == 1:
            user_friends = []
        else:
            user_friends = list(map(lambda friend_id: int(friend_id), splitted_line[1].split(',')))

        return user_id, user_friends



    def convert_user_with_friends_list_to_friendship_level_list(self, user_with_friends_list):
        self.start_time(method_name="convert_user_with_friends_list_to_friendship_level_list")
        friendship_level_list = user_with_friends_list.flatMap(Main.user_with_friends_to_friendship_level_tuples_list)
        friendship_level_list.cache()
        self.end_time(method_name="convert_user_with_friends_list_to_friendship_level_list")
        return friendship_level_list

    @staticmethod
    def user_with_friends_to_friendship_level_tuples_list(user_with_friends):        
        user_id = user_with_friends[0]
        user_friends = user_with_friends[1]

        friendship_levels = []

        for friend_id in user_friends:
            key = (user_id, friend_id)
            if user_id > friend_id:
                key = (friend_id, user_id)
            # friends
            friendship_levels.append((key, 0))

        for friend_pair in itertools.combinations(user_friends, 2):
            friend_0 = friend_pair[0]
            friend_1 = friend_pair[1]

            key = (friend_0, friend_1)
            if friend_0 > friend_1:
                key = (friend_1, friend_0)
            # mutual friends
            friendship_levels.append((key, 1))

        return friendship_levels



    def get_mutual_friends_with_friend_counts(self, friendship_level_list):
        self.start_time(method_name="get_mutual_friends_with_friend_counts")
        mutual_friends_with_friend_counts = friendship_level_list.groupByKey() \
            .filter(lambda friendship_level: 0 not in friendship_level[1]) \
            .map(lambda friendship_level: (friendship_level[0], sum(friendship_level[1])))
        self.end_time(method_name="get_mutual_friends_with_friend_counts")
        return mutual_friends_with_friend_counts


    
    def get_friends_recomendations(self, mutual_friends_with_friend_counts):
        self.start_time(method_name="get_friends_recomendations")
        friends_recomendations = mutual_friends_with_friend_counts.flatMap(Main.mutual_friends_with_friend_counts_to_friends_recomentations) \
            .groupByKey() \
            .map(lambda m: (m[0], Main.recommendation_to_sorted_truncated(list(m[1]))))
        self.end_time(method_name="get_friends_recomendations")
        return friends_recomendations
    
    @staticmethod
    def mutual_friends_with_friend_counts_to_friends_recomentations(mutual_friend_with_friend_counts):
        friends = mutual_friend_with_friend_counts[0]
        count = mutual_friend_with_friend_counts[1]

        friend_0 = friends[0]
        friend_1 = friends[1]

        recommendation_0 = (friend_0, (friend_1, count))
        recommendation_1 = (friend_1, (friend_0, count))

        return [recommendation_0, recommendation_1]

    @staticmethod
    def recommendation_to_sorted_truncated(recomendations):
        if len(recomendations) > 1024:
            max_indices = []

            for idx in range(0, 10):
                current_max_index = 0
                for i in range(1, len(recomendations)):
                    rec = recomendations[i]
                    if rec[1] >= recomendations[current_max_index][1] and i not in max_indices:
                        current_max_index = i

                max_indices.append(current_max_index)

            recomendations = [recomendations[i] for i in max_indices]

        recomendations.sort(key=lambda x: (-x[1], x[0]))

        return list(map(lambda x: x[0], recomendations))[:10]

    def save_as_file_and_stop(self, data, catalog):
        data.saveAsTextFile(catalog)
        self.context.stop()

    def main(self):
        lines = self.load_lines("./2.txt")
        user_with_friends_list = self.convert_lines_to_user_with_friends_list(lines)
        friendship_level_list = self.convert_user_with_friends_list_to_friendship_level_list(user_with_friends_list)
        mutual_friends_with_friend_counts = self.get_mutual_friends_with_friend_counts(friendship_level_list)
        friends_recomendations = self.get_friends_recomendations(mutual_friends_with_friend_counts)
        self.save_as_file_and_stop(friends_recomendations, "result")


main = Main()
main.main()
