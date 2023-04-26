# Data projects

This repository contains a collection of my data projects, including data analysis, and big data processing projects. 

## Project List
- atm_analysis.ipynb: Data analysis project of an ATM dataset that includes EDA and OLS, LASSO, Ridge, Elastic Net and Neural Network models.

- project1.py: MapReduce project - Performed data analytics over a dataset of online social networks using MRJob. 

Project 2 includes 2 files:  Perform text data analysis over a dataset of Australian news from ABC using both RDD and DataFrame APIs of Spark with Python. 
- project2_df.py: This is a Spark DataFrame project that processes text data to compute Term Frequency-Inverse Document Frequency (TF-IDF) weights for each term in a given dataset. The input dataset contains dates and terms, and the code processes the data to compute the TF-IDF weights of each term per year. The top 'k' terms with the highest TF-IDF weights for each year are selected and saved to an output file.
- project2_rdd.py: This is a Spark RDD-based implementation of the same project that computes the Term Frequency-Inverse Document Frequency (TF-IDF) weights for terms in a dataset. The input dataset contains dates and terms, and the code processes the data to compute the TF-IDF weights of each term per year. The top 'k' terms with the highest TF-IDF weights for each year are selected and saved to an output file.

- project3.py: The given project is an implementation of a scalable data processing pipeline using Apache Spark. The purpose of this project is to find pairs of similar records in two input datasets based on their Jaccard similarity, subject to a user-defined similarity threshold (tau). The project consists of three main stages:
Sorting tokens by frequency: In this stage, the pipeline processes the input datasets to create a dictionary that maps tokens to their frequencies in the datasets. The tokens are sorted based on their frequencies, and this order is used later in the pipeline to sort the record elements.

Finding similar ID pairs: The pipeline processes the input datasets to create two DataFrames, one for each input dataset. It then sorts the elements of each record in the DataFrames according to the token frequencies determined in the first stage. To efficiently find similar record pairs, the pipeline computes the prefix length for each record and generates key-value pairs based on the prefix tokens. The pipeline then joins the two DataFrames on the common prefix tokens and applies a length filter to remove pairs of records that do not satisfy the length filtering condition based on the user-defined tau value. Afterward, the pipeline calculates the Jaccard similarity between the remaining record pairs and filters the results to only include pairs with a Jaccard similarity greater than or equal to the tau value.

Processing the results: In this final stage, the pipeline removes duplicate record pairs and sorts the resulting DataFrame by the record IDs in ascending order. The output DataFrame is then formatted to include the record ID pairs and their corresponding Jaccard similarity values. Finally, the resulting DataFrame is saved as a tab-separated CSV file in the specified output path.



