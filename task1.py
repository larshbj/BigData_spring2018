from pyspark import SparkContext, SparkConf

master = "local[4]"
appName = "task1"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

rdd = sc.textFile("data/geotweets.tsv")
sampled_rdd = rdd.sample(False, 0.1, 5)
sampled_rdd_list = sampled_rdd.map(lambda x: x.split('\t'))

number_of_lines = sampled_rdd.count()
number_of_users = sampled_rdd_list.map(lambda x: x[6]).distinct().count()
number_of_countries = sampled_rdd_list.map(lambda x: x[1]).distinct().count()
number_of_places = sampled_rdd_list.map(lambda x: x[4]).distinct().count()
number_of_languages = sampled_rdd_list.map(lambda x: x[5]).distinct().count()
minimum_latitude = sampled_rdd_list.map(lambda x: float(x[12])).reduce(lambda a, b: min(a,b))

lineLengths = sampled_rdd.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

print("Number of tweets:\t", number_of_lines)
print("Number of users:\t", number_of_users)
print("Number of countries:\t", number_of_countries)
print("Number of places:\t", number_of_places)
print("Number of languages:\t", number_of_languages)
print("Minimum latitude:\t", minimum_latitude)
