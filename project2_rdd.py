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
        format_words= file.map(lambda line: line.split(",")).map(lambda x: (x[0], x[1].split(" ")))
        format_words = format_words.map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0][:4], x[1]))
        # flatMap RDD into format: [(year, term, 1)]
        pair = format_words.flatMap(lambda x: [((x[0], value), 1) for value in x[1]])
        # Map to form: [term, (year, 1)]
        pair = pair.map(lambda x: (x[0][1], (x[0][0], x[1])))
        
        # Take in stop words input file and map into form: (stopword, 1)
        stop_words = stop_words.map(lambda x: (x, 1))
        # Filter RDD by removing all stop words from pair(RDD)
        filtered_words = pair.subtractByKey(stop_words)
        filtered_words = filtered_words.map(lambda x: ((x[1][0], x[0]), x[1][1]))

        # Calculation for TF
        # Use reduceByKey() to sum values that have the same (year, term)
        tf = filtered_words.reduceByKey(lambda a, b: a + b)
        # Map to form: (term, (year, count))
        tf = tf.map(lambda x: (x[0][1], (x[0][0], x[1])))

        # Calculation for IDF
        # Count the number of years in the dataset
        count_year = file.map(lambda line: line.split(",")).map(lambda x: x[0][:4]).distinct().count()
        # Map and take distinct tuples for filtered_words RDD in form: (year, term) 
        idf = filtered_words.map(lambda x: (x[0])).distinct()
        # Use reduceByKey to retrieve the number of years for having t(term)
        idf = idf.map(lambda x: (x[1], (1))).reduceByKey(lambda a,b: a + b)
        idf = idf.map(lambda x: (x[0], math.log10(float(count_year) / float(x[1]))))

        # Join TF and IDF and calculate for weight
        join = tf.join(idf)
        weight = join.map(lambda x: (x[1][0][0], (x[0], round(float(x[1][0][1]) * float(x[1][1]), 6))))
        # Sort RDD: Year in ascending, Weight in desending then term alphabetically
        weight = weight.sortBy(lambda x: (x[0], -x[1][1], x[1][0]))
        # Group (term, weight) by year
        groupByYear = weight.groupByKey().map(lambda x: (x[0], list(x[1])))
        # Get the firt k elements
        getK = groupByYear.map(lambda x: (x[0], list(x[1])[:k])).sortBy(lambda x: x[0])
        res = getK.map(lambda x: (x[0], ";".join(y[0] + "," + str(y[1]) for y in x[1]))).map(lambda x: x[0] + "\t" + x[1])       
        
        res.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()



if __name__ == "__main__":
    Project2_rdd().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
