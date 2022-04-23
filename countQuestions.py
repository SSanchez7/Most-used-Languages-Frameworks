from __future__ import print_function
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: filterData.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1] 
    fileout = sys.argv[2] 

    spark = SparkSession.builder.appName("Project4").getOrCreate()

    # para tener el archivo de la forma (tag, id)
    rdd = spark.read.text(filein).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[1], t[0]))

    rdd = rdd.groupByKey().mapValues(lambda iterable: len(iterable))

    count = rdd.count()
    
    sort = rdd.sortBy(lambda line: line[1], False)

    sort.saveAsTextFile(fileout)
    
    spark.stop()

    print("Tags unicos:", count)