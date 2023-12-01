# Data Projects

This repository contains a collection of my data projects, including data analysis, and big data processing projects.

## Project List

### 1. ATM Analysis
- **File:** atm_analysis.ipynb
- **Description:** Data analysis project of an ATM dataset that includes Exploratory Data Analysis (EDA) and various machine learning models such as OLS, LASSO, Ridge, Elastic Net, and Neural Network.

### 3. Australian News Text Data Analysis
- **Files:** analysetextdata_df.py, analysetextdata_rdd.py
- **Description:** Perform text data analysis over a dataset of Australian news from ABC using both RDD and DataFrame APIs of Spark with Python. The project computes Term Frequency-Inverse Document Frequency (TF-IDF) weights for each term in a given dataset. The input dataset contains dates and terms, and the code processes the data to compute the TF-IDF weights of each term per year. The top 'k' terms with the highest TF-IDF weights for each year are selected and saved to an output file.

### 4. Scalable Data Processing Pipeline with Apache Spark
- **File:** project3.py
- **Description:** The given project is an implementation of a scalable data processing pipeline using Apache Spark. The purpose of this project is to find pairs of similar records in two input datasets based on their Jaccard similarity, subject to a user-defined similarity threshold (tau). The project consists of three main stages:
  - Sorting tokens by frequency
  - Finding similar ID pairs
  - Processing the results
  The output DataFrame is saved as a tab-separated CSV file in the specified output path.

