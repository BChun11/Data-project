from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.window import Window
import sys

class Project2_df:
    def run(self, inputPath, outputPath, inputPath2, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        file = spark.sparkContext.textFile(inputPath)

        # Format file into format [(year), {term1, term2, .., termn}]
        format_words = file.select(substring(col("line"), 0, 4).alias("year"),
                                   split(substring(col("line"), 6, length(col("line")) - 5), " ").alias("words"))\
                                   .withColumn("words", explode("words"))

        # Filter for length(words) to be greater or equal to 1
        wordsDF = format_words.filter(length(col("words")) >= 1)

        # Stopwords input file
        stopwords_list = spark.read.text(inputPath2).withColumnRenamed("value", "stopword").select("stopword").collect()
        stopwords_list = [row.stopword for row in stopwords_list]

        # Remove stop words using StopWordsRemover()
        remover = StopWordsRemover(inputCol="words", outputCol="term", stopWords=stopwords_list)
        filteredDF = remover.transform(wordsDF).select("year", "term")

        # Create TF dataframe
        tfDF = filteredDF.groupBy("year", "term").count().withColumnRenamed("count", "tf")

        # Create IDF dataframe
        number_years = filteredDF.select("year").distinct().count()
        idf = filteredDF.select("year", "term").distinct().groupBy("term").count().withColumnRenamed("count", "term_in_years")

        # Calculate IDF
        idf = idf.withColumn("IDF", log10(lit(number_years) / col("term_in_years"))).select("term", "IDF")

        # Join the TF and IDF dataframe 
        weightDF = tfDF.join(idf, on="term")
        weightDF = weightDF.withColumn("Weight", round(col("tf") * col("IDF"), 6)).select("year", "term", "Weight")

        # Get the top k terms using Window from pyspark.sql.window 
        window = Window.partitionBy("year").orderBy(asc("year"), desc("Weight"), asc("term"))
        weightDF = weightDF.select('*', rank().over(window).alias('rank')).filter(col("rank") <= k).select("year", "term", "Weight")

        # Format dataframe by concatenating term(,)Weight
        res = weightDF.select("year", concat(col("term"), lit(","), col("Weight")).alias("data"))
        res = res.groupBy("year").agg(concat_ws(";", collect_list("data")).alias("data"))

        res.coalesce(1).write.options(delimiter="\t").format("csv").save(outputPath)
        spark.stop()

if __name__ == "__main__":
    Project2_df().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
