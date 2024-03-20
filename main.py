from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
 
# Initialize Spark session
spark = SparkSession.builder.appName("COVID Analysis").getOrCreate()
 
# Read COVID-19 dataset as a Spark DataFrame
covid_data = spark.read.csv('/Users/renukagopishetty/Desktop/untitled folder/covid.csv', header=True, inferSchema=True)
 
# Read other datasets as Spark DataFrames
workplace_visitors = spark.read.csv('/Users/renukagopishetty/Desktop/untitled folder/workplace-visitors-covid.csv', header=True, inferSchema=True)
stay_at_home = spark.read.csv('/Users/renukagopishetty/Desktop/untitled folder/stay-at-home-covid.csv', header=True, inferSchema=True)
economic_decline = spark.read.csv('/Users/renukagopishetty/Desktop/untitled folder/economic_decline.csv', header=True, inferSchema=True)
 
# Handle missing values or duplicates in all DataFrames
workplace_visitors = workplace_visitors.dropna()
stay_at_home = stay_at_home.dropna()
economic_decline = economic_decline.dropna()
covid_data = covid_data.dropna()
 
# Register DataFrames as SQL temporary views
workplace_visitors.createOrReplaceTempView("workplace_visitors")
stay_at_home.createOrReplaceTempView("stay_at_home")
economic_decline.createOrReplaceTempView("economic_decline")
covid_data.createOrReplaceTempView("covid_data")
 
# Perform necessary merging/joining operations using SQL
query = """
SELECT wv.Entity, wv.Code, wv.Day, wv.workplaces, sh.stay_home_requirements, ed.*
FROM workplace_visitors wv
LEFT JOIN stay_at_home sh ON wv.Entity = sh.Entity AND wv.Code = sh.Code AND wv.Day = sh.Day
LEFT JOIN economic_decline ed ON wv.Entity = ed.Entity AND wv.Code = ed.Code
LEFT JOIN covid_data cd ON wv.Day = cd.date AND wv.Entity = cd.location_key
"""
 
merged_data = spark.sql(query)
 
# Convert 'Day' to datetime format (assuming it's needed)
merged_data = merged_data.withColumn('Day', to_date(merged_data['Day']))
 
# Convert Spark DataFrame to Pandas DataFrame for plotting
merged_pandas = merged_data.toPandas()
# Check columns in the DataFrame
print(merged_pandas.columns)
 
# Display the first few rows of the DataFrame
print(merged_pandas.head())
 
 
# Plotting Daily COVID-19 Cases Over Time using Pandas
covid_pandas = covid_data.toPandas()
plt.figure(figsize=(12, 6))
sns.lineplot(x='date', y='new_confirmed', data=covid_pandas)
plt.title('Daily COVID-19 Cases Over Time')
plt.xlabel('Date')
plt.ylabel('Confirmed Cases')
plt.show()
 
# ... (Your existing code up to the plotting section)
 
# Check the data types and structure of the merged Pandas DataFrame
print(merged_pandas.info()) # Print information about DataFrame columns and data types
print(merged_pandas.head()) # Display the first few rows of the DataFrame
 
# Plotting Workplace Visitors Over Time using Seaborn and Pandas DataFrame
plt.figure(figsize=(14, 6))
sns.lineplot(x='Day', y='workplaces', data=merged_pandas)
plt.title('Workplace Visitors Over Time')
plt.xlabel('Entity') # Assuming 'Entity' represents countries or regions
plt.ylabel('Workplaces')
plt.legend(title='Countries', loc='upper left')
plt.show()
 
 
 
# Plotting Stay-at-Home Requirements Over Time using Seaborn and Spark DataFrame
plt.figure(figsize=(14, 6))
sns.lineplot(x='Day', y='stay_home_requirements', data=merged_pandas)
plt.title('Stay-at-Home Requirements over Time')
plt.xlabel('Date')
plt.ylabel('Stay-at-Home Requirements')
plt.show()
 
# Histogram: GDP Growth vs. Workplace Visitors using Pandas DataFrame
plt.figure(figsize=(12, 6))
sns.histplot(merged_pandas['GDP growth from previous year, 2020 Q2'], bins=20, kde=True)
plt.title('Histogram: GDP Growth from Previous Year, 2020 Q2')
plt.xlabel('GDP Growth')
plt.ylabel('Frequency')
plt.show()
 
plt.figure(figsize=(10, 6))
sns.histplot(merged_pandas['workplaces'], bins=20, kde=True)
plt.title('Histogram: Workplace Visitors')
plt.xlabel('Workplaces')
plt.ylabel('Frequency')
plt.show()
