from __future__ import print_function
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: filterData.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein_tags = sys.argv[1]   #tags
    filein_quest = sys.argv[2]  #questions 
    filein_answ = sys.argv[3]   #answers
    fileout = sys.argv[4]

    spark = SparkSession.builder.appName("Project4").getOrCreate()

    rdd_tags = spark.read.text(filein_tags).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[0], t[1])) # (id, tag)
    #rdd_tags = rdd_tags.filter(lambda t: t[1] in tagsName)
    rdd_tags.cache()

    # rdd de questions y answers con ()
    rdd_quest = spark.read.text(filein_quest).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[0], t[2])) # (id, time)
    rdd_quest = rdd_tags.join(rdd_quest).map(lambda t: ((t[1][0], t[1][1].split("T")[0]), 1)) # join -> (id, (tag, time)) : map -> ((tag, time), 1)
    rdd_quest.cache()

    rdd_answ = spark.read.text(filein_answ).rdd.map(lambda r: r[0]).map(lambda t: t.split("\t")).map(lambda t: (t[3], t[2])) # (par_id, time)
    rdd_answ = rdd_tags.join(rdd_answ).map(lambda t: ((t[1][0], t[1][1].split("T")[0]), 1)) # join -> (id, (tag, time)) : map -> ((tag, time), 1)

    rdd = rdd_quest.union(rdd_answ)

    rdd = rdd.reduceByKey(lambda a, b: a + b).map(lambda t: (t[0][0], t[1])) # (tag, count)
    rdd = rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda t: t[1], False)

    #rdd = rdd.groupByKey().sortByKey().mapValues(lambda iterable: sorted(list(iterable), key=lambda x: x[1], reverse=True)) # (time, iterable(tag, count)))

    #rdd = rdd.map(toCSVLine)

    rdd.saveAsTextFile(fileout)