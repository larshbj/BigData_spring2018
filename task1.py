from pyspark import SparkContext, SparkConf

master = "local[4]"
appName = "task1"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

distFile = sc.textFile("data/geotweets.tsv")
lineLengths = distFile.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

print("AAAAAAAAAAAAAAAAAAAAAAAA", totalLength)
