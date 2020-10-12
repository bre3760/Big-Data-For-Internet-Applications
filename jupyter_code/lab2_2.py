import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("My app")
sc = SparkContext(conf=conf)
# spark-submit --master local --deploy-mode client lab2_2.py 'ho' "lab2/"

rdd = sc.textFile("/data/students/bigdata_internet/lab2/word_frequency.tsv")
outputPath = str(sys.argv[2])
PREFIX = str(sys.argv[1])
print('opath: ', outputPath)
print('prefix is: ', PREFIX)



somelines = rdd.take(5)
print('some lines of original rdd: ',somelines)
numlines = rdd.count()
print('total number of lines in rdd: ',numlines)

def findprefix(line, Prefix):
    PREFIX = Prefix
    # quality is good    87367
    # the 'quality' prefix should be the first 0:len(PREFIX) values
    # i also don't care what comes after the orefix, it just has to be there
    if line[0:len(PREFIX)] == PREFIX:
        return True
    else:
        return False

filtered_by_prefix_rdd = rdd.filter(lambda line: findprefix(line, PREFIX))

print('some lines of the filtered rdd: ',filtered_by_prefix_rdd.take(5))
print('number of lines in filtered rdd: ',filtered_by_prefix_rdd.count())

print('filtered_by_prefix_rdd: ',filtered_by_prefix_rdd.take(5))

def splitline(line):
    description, freq = line.split("\t")
    return int(freq)


frequencies_rdd = filtered_by_prefix_rdd.map(splitline)
print('frequencies_rdd: ',frequencies_rdd.count())

def findMax(f1, f2):
    if f1 > f2:
        return f1
    elif f2 > f1:
        return f2
    else:
        return f1


maxfreq_o = frequencies_rdd.reduce(findMax)
print('max freq: ',maxfreq_o)

def keepsome(line, Maxfreq):
    maxfreq = Maxfreq
    description, freq = line.split("\t")
    freq = int(freq)
    if freq >= 0.8 * maxfreq:
        return True
    else:
        return False


greater_than_rdd = filtered_by_prefix_rdd.filter(
    lambda line: keepsome(line, maxfreq_o))
print('number of elements found greater than 0.8*maxfreq: ',greater_than_rdd.count())
# greater_than_rdd.saveAsTextFile(outputPath)
