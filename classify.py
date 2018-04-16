from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
import os, shutil, datetime
from collections import Counter
from operator import add, itemgetter
from functools import reduce
import sys


# Init
master = "local[4]"
appName = "phase2"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Methods for reading dataset, input tweet and saving result
def takeInputAndReturnList(input_path='data/input1.txt'):
    with open(input_path, 'r') as f:
        input_tweet = f.readlines()
        input_tweet_list = [i.rstrip().split(' ') for i in input_tweet]
        print(input_tweet_list[0])
    return input_tweet_list[0]

def takeTrainingAndReturnList(input_path="data/geotweets.tsv"):
    rdd = sc.textFile(input_path)
    return rdd.map(lambda x: x.split('\t'))

def saveResultToFile(result, output_path="results/output_phase2.tsv"):
    result_rdd = sc.parallelize(result).map(lambda x: '{}\t{}'.format(x[0], x[1]))
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)
    result_rdd.coalesce(1).saveAsTextFile(output_path)

# -----------------------------------------------------------

def getTotalTweetCount(rdd):
    return rdd.count()

# Computes a tweet count per place map
def getTweetsPerPlaceCount(place_and_tweets):
    tweets_per_place_count = place_and_tweets.map(lambda x: (x[0], 1)).countByKey().items()
    return sc.parallelize(tweets_per_place_count).collectAsMap()

#Input: tweets_by_city rdd: [('place1', ['word1', 'word2', 'word3'...]), ...] and input tweet list
#Output: Tuples with place as key and a dictionary containing count for every word from input tweet
def getTweetWithWordByCityCount(tweets_by_city, input_tweet_list):
    # Helper methods used in combineByKey
    def addIfWordInTweet(word, value, tweet_list):
        if word in tweet_list:
            value += 1
        return value

    def to_dict(tweet_list):
        counter_dict = {k: 0 for k in input_tweet_list}
        return counter_dict

    def add(counter_dict, tweet_list):
        return {k: addIfWordInTweet(k, v, tweet_list) for k, v in counter_dict.items()}

    def merge(dict1, dict2):
        new = {**dict1, **dict1}
        return new

    counted_tweets_with_word_by_city = tweets_by_city.combineByKey(to_dict, add, merge)
    return counted_tweets_with_word_by_city

# Calculate probability for a single place
def calculateProbability(place, data_dict, nr_tweets_place, total_tweet_count):
    initial_value = nr_tweets_place / total_tweet_count
    probability = reduce(lambda x, value: x * value / nr_tweets_place, data_dict.values(), initial_value)
    return probability

# Calculate probabilities for all places and sort
def findProbabilitiesAndSort(counted_tweets_with_word_by_city, tweet_per_place_count_map, total_tweet_count):
    probabilities = counted_tweets_with_word_by_city\
            .map(lambda x: (x[0], calculateProbability(x[0], x[1], tweet_per_place_count_map[x[0]], total_tweet_count)))
    sorted_probabilities = sorted(probabilities.collect(), key=lambda x: x[1], reverse=True)
    return sorted_probabilities

# Wrapper method and outputs the most probable place(s)
def estimatePlaces(rdd_list, input_tweet_list):
    place_and_tweets = rdd_list.map(lambda x: (x[4], x[10]))\
                        .mapValues(lambda x: x.split(' '))

    total_tweet_count = getTotalTweetCount(rdd_list)
    tweet_per_place_count_map = getTweetsPerPlaceCount(place_and_tweets)
    tweets_by_city = place_and_tweets.map(lambda x: (x[0], list(map(str.lower, x[1]))))
    counted_tweets_with_word_by_city = getTweetWithWordByCityCount(tweets_by_city, input_tweet_list)
    sorted_probabilities = findProbabilitiesAndSort(counted_tweets_with_word_by_city, tweet_per_place_count_map, total_tweet_count)

    result = []
    top_probability = sorted_probabilities[0][1]
    if top_probability != 0:
        for place in sorted_probabilities:
            if place[1] == top_probability:
                result.append(place)
            else: break
    return result

if __name__ == "__main__":
    [training_path, input_path, output_path] = sys.argv[1:4]
    rdd_list = takeTrainingAndReturnList(training_path)
    input_tweet_list = takeInputAndReturnList(input_path)
    total_tweet_count = getTotalTweetCount(rdd_list)
    places = estimatePlaces(rdd_list, input_tweet_list)
    print(places)
    saveResultToFile(places, output_path)
