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
        format_words = file.map(lambda line: line.split(",")).map(lambda x: (x[0], x[1].split(" ")))
        format_words = format_words.map(lambda x: (x[0], set(x[1]))).flatMap(lambda x: [(x[0], value) for value in x[1]])

        # Convert RDD to Data frame with columns "date" and "words"
        df_columns = ["date", "words"]
        # Filter for length(words) to be greater or equal to 1
        wordsDF = format_words.toDF(df_columns).filter(length(col("words")) >=1)
        filterDF = wordsDF.withColumn("words", array(wordsDF["words"]))

        #Stopwords input file
        stopword_file = spark.sparkContext.textFile(inputPath2)
        # Add stop words to a list
        stopwords_list = stopword_file.map(lambda x: x).collect()

        # Remove stop words using StopWordsRemover()
        remover = StopWordsRemover(inputCol = "words", outputCol = "filtered", stopWords = stopwords_list)
        # Remove words in filterDF(data frame) from stopwords list
        filteredDF = remover.transform(filterDF)
        filteredDF = filteredDF.select("date", "filtered").withColumn("filtered", concat_ws("", col("filtered")))
        # Filter for rows with empty values that were removed words 
        filteredDF = filteredDF.filter(col("filtered") != "").withColumnRenamed("filtered", "term")

        # Retrieve the year by taking the first 4 strings in date column of dataframe
        wordDF = filteredDF.withColumn("date", col("date").substr(0, 4)).withColumnRenamed("date", "year")

        # Create TF dataframe
        tfDF = wordDF.groupBy(wordDF.columns).count().orderBy("year").withColumnRenamed("count", "tf")

        # Create IDF dataframe
        # Count the number of years in dataframe
        number_years = wordDF.select("year").distinct().count()
        # Number of years that contain t(term) in a year
        idf = wordDF.select("year", "term").distinct().orderBy("year")
        # Group by term and count 
        idf = idf.groupBy("term").count().withColumnRenamed("count", "term_in_years")

        # Add constant column number_years for IDF calculation
        idf = idf.withColumn("number_years", lit(number_years))
        # Calculate for IDF
        idf = idf.withColumn("IDF", log10(idf.number_years/idf.term_in_years)).withColumnRenamed("term", "terms")
        idf = idf.select("terms", "IDF")

        # Join the TF and IDF dataframe 
        weightDF = tfDF.join(idf, tfDF.term == idf.terms, "inner")
        #Calculate for weight
        weightDF = weightDF.withColumn("Weight", round(weightDF.tf * weightDF.IDF, 6)).select("year", "term", "Weight")

        # Get the top k terms using Window from pyspark.sql.window 
        window = Window.partitionBy(weightDF["year"]).orderBy(asc(col("year")), desc(col("Weight")), asc(col("term")))
        weightDF = weightDF.select('*', rank().over(window).alias('rank')).filter(col("rank") <= k).select("year", "term", "Weight")
        
        # Format dataframe by concatenating term(,)Weight
        res = weightDF.select("year", concat(col("term"), lit(","), col("Weight")).alias("data"))
        res = res.groupBy("year").agg(concat_ws(";", collect_list("data")).alias("data"))

        res.coalesce(1).write.options(delimiter="\t").format("csv").save(outputPath)
        spark.stop()

if __name__ == "__main__":
    Project2_df().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))