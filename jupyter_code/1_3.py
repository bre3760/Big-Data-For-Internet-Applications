from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("My app")
sc = SparkContext(conf=conf)
rdd = sc.textFile("/data/students/bigdata_internet/lab1/lab1_dataset.txt")
fields_rdd = rdd.map(lambda line: line.split(","))
value_rdd = fields_rdd.map(lambda l: int(l[1]))
value_sum = value_rdd.reduce(lambda v1, v2: v1 + v2)
print("The sum is:", value_sum)
# Question: In which file system are located your script and the /data/students/bigdata_internet/lab1/lab1_dataset.txt files? 
# Are they on the same file system?
# The script is in the local file system and the dataset is stored in the hdfs