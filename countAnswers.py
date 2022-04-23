from __future__ import print_function
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: filterData.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein_tags = sys.argv[1]
    filein_answ = sys.argv[2] 
    fileout = sys.argv[3] 

    spark = SparkSession.builder.appName("Project4").getOrCreate()

    # para tener el archivo de la forma (id, tag)
    rdd_tags = spark.read.text(filein_tags).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[0], t[1]))
    rdd_tags.cache()

    # rdd de anwers con (par_id, id)
    rdd_answ = spark.read.text(filein_answ).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[3], t[0]))

    rdd = rdd_tags.join(rdd_answ).map(lambda t: (t[1][0], t[1][1])) # (par_id, iterable[ tag, id ])

    rdd = rdd.groupByKey().mapValues(lambda iterable: len(iterable))
    count = rdd.count()
    
    sort = rdd.sortBy(lambda line: line[1], False)

    sort.saveAsTextFile(fileout)
    spark.stop()

    print("Tags unicos:", count)
