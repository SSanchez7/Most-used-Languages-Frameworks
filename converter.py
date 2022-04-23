import sys
import csv
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1] #"hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv"
    fileout = sys.argv[2] #"hdfs://cm:9000/uhadoop2021/<user>/series-avg/"
    columns = sys.argv[3] 

    #csv.writer(open(fileout, 'w+'), delimiter='\t').writerows(csv.reader(open(filein, encoding = "ISO-8859-1")))

    writer = csv.writer(open(fileout, 'w+'), delimiter='\t')
    reader = csv.reader(open(filein, encoding = "ISO-8859-1"))

    for row in reader:
        writer.writerow(row[:columns])