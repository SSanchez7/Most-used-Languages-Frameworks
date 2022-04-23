import sys
from pyspark.sql import SparkSession

from __future__ import print_function
from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: filterData.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1] 
    fileout = sys.argv[2] 

    spark = SparkSession.builder.appName("Project4").getOrCreate()

    # para tener el archivo de la forma (tag, id)
    rdd = spark.read.textFile(filein).map(lambda t: t.split("\t")).map(lambda t: (t[1], t[0]))

    rdd.saveAsTextFIle(fileout)

    spark.stop()
    
    #if __name__ == "__main__":
    #if len(sys.argv) != 3:
    #    print("Usage: wordcount <file> <fileout>", file=sys.stderr)
    #    sys.exit(-1)
    #
    #spark = SparkSession\
    #    .builder\
    #    .appName("PythonWordCount")\
    #    .getOrCreate()
    #
    #lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    #counts = lines.flatMap(lambda x: x.split(' ')) \
    #              .map(lambda x: (x, 1)) \
    #              .reduceByKey(add)
    #              
    #counts.saveAsTextFile(sys.argv[2])
    #
    #spark.stop()