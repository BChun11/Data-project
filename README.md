# Data Projects

This repository contains a collection of my data projects, including data analysis, and big data processing projects.

## Project List

### ATM Analysis
- **File:** atm_analysis.ipynb
- **Description:** Data analysis project of an ATM dataset that includes Exploratory Data Analysis (EDA) and various machine learning models such as OLS, LASSO, Ridge, Elastic Net, and Neural Network.

### IoT device classification cybersecurity project (Machine Learning)
- **File:** IoTdeviceclassification.ipynb
- **Description:** Machine learning project utilising IPFIX records (network level features of IoT devices) from 26 different IoT devices to classify IoT devices based on their network
  level features to enhance cybersecurity measures by idenitfying vulnerable devices on the network based on their network features. Classification models such as Logistic regression, Decision Tree model, Random Forest Model, LightGBM and XGBoost were implemented.

### Predict student performance from game play (Machine Learning)
- **File:** studentperformance.ipynb
- **Description:** Predict student performance during game-based learning in real-time. Several classification models were implemented to train on one of the largest open datasets of game logs.


### Scalable Data Processing Pipeline with Apache Spark
- **File:** processpipeline.py
- **Description:** The given project is an implementation of a scalable data processing pipeline using Apache Spark. The purpose of this project is to find pairs of similar records in two input datasets based on their Jaccard similarity, subject to a user-defined similarity threshold (tau). The project consists of three main stages:
  - Sorting tokens by frequency
  - Finding similar ID pairs
  - Processing the results
  The output DataFrame is saved as a tab-separated CSV file in the specified output path.

