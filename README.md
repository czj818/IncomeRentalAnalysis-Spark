# Using PySpark and Sparksql to Explore the Income and Rental Price Datasets

## Motivation

The quality and happiness of living in a certain geographical place are related to the proportion of rent over monthly income. The less the ratio is, the more people can spend on their interest. In this project, I will discover the relationship between household income and the rental price in different counties over the United States. Does a higher household income in a county lead to a higher rental price in that county? I will explore the US income dataset and rental price dataset using Pyspark to find out the average rental price per county and average household income. Besides, I will rank the top10 county with the highest rent-income ratio.

## Research Question
- Which county is the most expensive to live in according to the average rental price?
- Does the top five highest rental price counties have corresponding highest incomes?
- Which county has the worst cost performance in the aspect of rent?

## Data Sources

1. Zillow Rent Index Data

The data contains median estimated monthly rental prices in a certain area from 2010 to 2017. It is available on Kaggle and can be downloaded in CSV format. It includes 13,131 records and 81 variables. I decided to use all 13,131 records because I need enough keys to merge the two datasets and there is no missing data in the variables of interest. Each record shows the ​city, county, metro, state, population rank and monthly rental price from November 2010 to January 2017​. For this project, I will mainly use county, and January 2017, the most recent monthly rental price of each record.

2. US Household Income Statistics

The US Household Income Data contains 32,526 records of incomes by household and geographical location. The data is available on Kaggle and can be downloaded in CSV format. It has 19 columns including state_Name, County, City, Place, Longitude, Latitude, Zip Code, mean, median, standard deviation of household income on a neighborhood scale. For the completeness of the dataset, I will use all 32,526 records and focus on county and median income variables.

## Data Manipulation

### Step1 Preprocessing and Conversion

After the first examination, the two datasets do not have any missing value or abnormal in the columns that I need, the rent ranges from 518 to 17985 but income ranges from 0 to 300,000 and I choose to exclude those neighborhoods with 0 median income. Next, I convert CSV to JSON format for spark calculation. I open the two datasets in colab, read them using ​csv.DictReader ​and ​json.dump​ each line into new JSON files. The source code can be found in the IncomeRentalAnalysis.py.

### Step 2 Catching information and Creating RDDs in Spark

Then, I upload JSON files to cavium, write two functions to catch the information of County, rental price, median income and make the County variables in two datasets match with each other by stripping ‘ County’ in the columns in income dataset to prepare for merging. Apply ​map json.loads ​to read my datasets into RDDs and use flatMap to apply my function on RDDs.

### Step 3 Join two datasets

I decide to use spark SQL to do the join so I need to save my RDD results to tables by using registerTempTable. To make my table visually clearer, I add the column name to each column. I import StructField, StringType ​and​ StructType,​ create schema string, then parse the schema string using StructField and StringType, use StructType to create the schema and combine my RDD with schema use createDataFrame.