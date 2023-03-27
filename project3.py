from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
import sys

class project3:
    def run(self, inputPath1, inputPath2, tau, outputPath):
        spark = SparkSession.builder.master("local").appName("project3").getOrCreate()
        # Input files
        file1 = spark.sparkContext.textFile(inputPath1)
        file2 = spark.sparkContext.textFile(inputPath2)
       
        # Stage 1: Sorting tokens by frequency
        file1 = file1.map(lambda line: line.split(" "))
        file2 = file2.map(lambda line: line.split(" "))
        # Use flatMap to map the token values 
        validValues1 = file1.flatMap(lambda x: x[1:]).filter(lambda x: len(x) >= 1)
        validValues2 = file2.flatMap(lambda x: x[1:]).filter(lambda x: len(x) >= 1)
        # Create key-value pair for each token in form: (token, 1)
        pair1 = validValues1.map(lambda x: (x, 1))
        pair2 = validValues2.map(lambda x: (x, 1))
        # Join the two RDD's together and reduce by key
        pair = pair1.union(pair2).reduceByKey(lambda x,y: x+y)
        # Take reduced (key, value) pair of tokens and sort by frequency
        token_order = token_order = pair.map(lambda x: (x[1], x[0])).sortBy(lambda x: (x[0], x[1])).map(lambda x: (x[1], x[0]))
        # Save tokens into a dictionary with corresponding frequency as value
        tokenDict = token_order.collectAsMap()

        # Stage 2: Find similar ID pairs
        # Format RDD into form of: (rid, (list of elements)) and convert into Dataframe
        format_file1 = file1.map(lambda x: (x[0], x[1:]))
        format_file2 = file2.map(lambda x: (x[0], x[1:]))
        df_columns = ["RecordId", "Elements"]
        # Create data frame for file1 and file2
        recordDF1 = format_file1.toDF(df_columns)
        recordDF2 = format_file2.toDF(df_columns)

        # Convert the record elements into a set
        recordDF1 = recordDF1.withColumn("Unique elements", array_distinct(col("Elements"))).select("RecordID", "Unique elements")
        recordDF2 = recordDF2.withColumn("Unique elements", array_distinct(col("Elements"))).select("RecordID", "Unique elements")

        # Function that sorts each row of the Dataframe based on token frequency ordering
        def sort_row(row):
            d = row.asDict()
            # list that gets the elements of each record and puts into a list
            record_list = d['Unique elements']
            
            result = {}
            # For each element in record_list search through token dictionary and add key and value into new dictionary called result
            for item in record_list:
                if item != "":   
                    result[item] = tokenDict[item]
                    # Sorts dictionary by value first then key
                    result1 = sorted(result.items(), key=lambda x: (x[1], x[0]))
                    tmp = []
                    for key, value in result1:
                        tmp.append(key)
            # Update row with the new sorted list
            d.update({'Unique elements': tmp})
            updated_row = Row(**d)
            return updated_row

        # Sort ordering of the elements for both records and convert into Dataframe
        r_DF1 = recordDF1.rdd.map(sort_row).toDF()
        r_DF2 = recordDF2.rdd.map(sort_row).toDF()
        # Length of record
        r = size(col("Unique elements"))

        # Calculate prefix length for each record using formula 
        prefixDF1 = r_DF1.withColumn("prefix length", (r-ceil(r*tau)+1))
        prefixDF2 = r_DF2.withColumn("prefix length", (r-ceil(r*tau)+1))
        # Create a new column that grabs the first "prefix length" of elements
        prefixDF1 = prefixDF1.withColumn("prefix", slice(("Unique elements"), start=1, length=("prefix length"))).select("RecordID", "Unique elements", "prefix")
        prefixDF2 = prefixDF2.withColumn("prefix", slice(("Unique elements"), start=1, length=("prefix length"))).select("RecordID", "Unique elements", "prefix")
        # Convert Data frame to RDD
        prefix_rdd1 = prefixDF1.rdd
        prefix_rdd2 = prefixDF2.rdd
        # Generate (key, value) pair for each of its prefix token
        prefix_rdd1 = prefix_rdd1.flatMap(lambda x: [(key, (x[0], x[1])) for key in x[2]])
        prefix_rdd2 = prefix_rdd2.flatMap(lambda x: [(key, (x[0], x[1])) for key in x[2]])

        # Convert RDD to Dataframe
        df_column = ['key', 'Value']
        recordDF1 = prefix_rdd1.toDF(df_column)
        recordDF2 = prefix_rdd2.toDF(df_column).withColumnRenamed("Value", "Value1").withColumnRenamed("key", "key1")
       
       # Explode the "Value" column which splits it into "rid" and list(elements)
        recordDF1 = recordDF1.select("Value.*", "*").withColumnRenamed("_1", "rid").withColumnRenamed("_2", "elements")
        recordDF1 = recordDF1.select("key", "rid", "elements")
        # Repeat the same steps for recordDF2
        recordDF2 = recordDF2.select("Value1.*", "*").withColumnRenamed("_1", "rid1").withColumnRenamed("_2", "elements1")
        recordDF2 = recordDF2.select("key1", "rid1", "elements1")

        # Create a TempView of recordDF1 and recordDF2 to run spark.sql query
        recordDF1.createOrReplaceTempView("recordDF1")
        recordDF2.createOrReplaceTempView("recordDF2")
        # Join recordDF1 and recordDF2 based on key 
        similarDF = spark.sql("""SELECT * FROM recordDF1, recordDF2
                            WHERE recordDF1.key == recordDF2.key1""")
        similarDF = similarDF.select("key", "rid", "elements", "rid1", "elements1")

        # Applying the length filter
        # Add a column that gets the length of each element list
        lengthDF = similarDF.withColumn("length", size("elements")).withColumn("length1", size("elements1"))
        # Drop rows that don't satisfy the condition: |rid|*tau <= |rid1|
        lengthDF = lengthDF.where((lengthDF.length*tau) <= lengthDF.length1)

        # Calculating for Jaccard Similarity between the two records
        # Size of the number of intersecting elements between the records
        intersect_records = size(array_intersect(lengthDF.elements, lengthDF.elements1))
        # Size of the number of union elements between the two records
        union_records = size(array_union(lengthDF.elements, lengthDF.elements1))
        JaccardDF = lengthDF.withColumn("jaccard_Similarity", intersect_records/union_records)
        JaccardDF = JaccardDF.where(JaccardDF.jaccard_Similarity >= tau)

        # Stage 3: 
        # Remove duplicate records using distinct()
        JaccardDF = JaccardDF.select("rid", "rid1", round(col("jaccard_Similarity"), 6).alias("Jaccard Similarity")).distinct()
        # Sort rid by the first record in ascending order and then the second
        res = JaccardDF.sort(col("rid").cast("int"), col("rid1").cast("int"))
        # Format Dataframe by concatenating in form:(rid, rid1)
        res = res.select(concat(lit("("),col("rid"), lit(","), col("rid1"),lit(")")).alias("rid_pair"), "Jaccard Similarity")
   
        res.coalesce(1).write.options(delimiter="\t").format("csv").save(outputPath)
        spark.stop()



if __name__ == "__main__":
    project3().run(sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4])
