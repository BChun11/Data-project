from pyspark import SparkContext, SparkConf
import sys
import math

class Project2_rdd:
    def run(self, inputPath, outputPath, inputPath2, k):
        conf = SparkConf().setAppName("Project2")
        sc = SparkContext(conf=conf)

        # Input files
        file = sc.textFile(inputPath)
        stop_words = sc.textFile(inputPath2)
        
        # Format file into format [(year), {term1, term2, .., termn}]
        format_words = file.map(lambda line: line.split(",")).map(lambda x: (x[0][:4], x[1].split(" ")))
        format_words = format_words.flatMap(lambda x: [((x[0], value), 1) for value in x[1]])

        # Prepare stopwords as a dictionary
        stopwords_dict = stop_words.collectAsMap()

        # Filter RDD by removing all stop words from pair(RDD)
        filtered_words = format_words.filter(lambda x: x[0][1] not in stopwords_dict)

        # Calculate TF
        tf = filtered_words.reduceByKey(lambda a, b: a + b).map(lambda x: (x[0][1], (x[0][0], x[1])))

        # Calculate IDF
        count_year = file.map(lambda line: line.split(",")).map(lambda x: x[0][:4]).distinct().count()
        idf = filtered_words.map(lambda x: (x[0][1], 1)).distinct().reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], math.log10(float(count_year) / float(x[1]))))

        # Join TF and IDF and calculate for weight
        join = tf.join(idf)
        weight = join.map(lambda x: (x[1][0][0], (x[0], round(float(x[1][0][1]) * float(x[1][1]), 6))))
        weight = weight.sortBy(lambda x: (x[0], -x[1][1], x[1][0]))

        # Group (term, weight) by year and take the first k elements
        res = weight.groupBy(lambda x: x[0]).map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: (-y[1][1], y[1][0]))[:k])).sortBy(lambda x: x[0])
        res = res.map(lambda x: (x[0], ";".join(y[1][0] + "," + str(y[1][1]) for y in x[1]))).map(lambda x: x[0] + "\t" + x[1])

        res.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()



if __name__ == "__main__":
    Project2_rdd().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
