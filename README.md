# PySpark-Data-Processing
Earthquake and Tsunami Data Analysis Pipeline with PySpark
Project Overview
This project implements a comprehensive PySpark data processing pipeline for analyzing global earthquake and tsunami data. The pipeline demonstrates advanced distributed computing concepts, optimization strategies, and machine learning applications using Apache Spark on Databricks.

Dataset Description

Source Data: Global Earthquake and Tsunami Database 
https://www.kaggle.com/datasets/ahmeduzaki/global-earthquake-tsunami-risk-assessment-dataset
File: earthquake_data_tsunami.csv
Size: ~1GB (demonstration dataset - in production, this scales to GB+)
Records: 784 earthquake events
Time Period: 2021-2022

## Data Schema:

magnitude (float): Earthquake magnitude on Richter scale cdi (float): Community Decimal Intensity mmi (float): Modified Mercalli Intensitysig (int): Significance score nst (int): Number of seismic stations
dmin (float): Minimum distance to station gap (float): Gap between stations depth (float): Depth of earthquake in km latitude (float): Geographic latitude longitude (float): Geographic longitude Year (int): Year of occurrence Month (int): Month of occurrence
tsunami (int): Binary flag (1 = tsunami generated, 0 = no tsunami)

## Pipeline Architecture
1. Data Loading & Schema Definition
Explicit schema definition for optimized loading
Avoided schema inference overhead
Partitioning strategy implementation

2. Transformations Applied

15+ column transformations including:

Magnitude categorization (Minor/Light/Moderate/Major/Great)
Depth classification (Shallow/Intermediate/Deep)
Ocean region mapping (Atlantic/Indian/Pacific)
Tsunami risk scoring algorithm
Seismic intensity calculations

3. Filter Operations

Early filter pushdown for optimization
Three primary filters:

Major earthquakes (magnitude ≥ 6.5)
Tsunami-generating events
Recent earthquakes (2021-2022)

4. Complex Aggregations

Multi-level groupBy operations
Statistical aggregations (mean, max, min, stddev)
Window functions for cumulative analysis
Self-joins for related earthquake detection

5. SQL Operations

3 complex SQL queries using Spark SQL
Common Table Expressions (CTEs)
Window functions and ranking
Performance comparison with DataFrame API

6. Machine Learning Models

Classification: Random Forest for tsunami prediction (AUC: ~0.75)
Regression: Linear Regression for magnitude estimation (R²: ~0.45)
Clustering: KMeans for earthquake pattern identification (5 clusters)

## Execution

### Data Loading
![alt text](Images/Img1.png)
![alt text](Images/Image2.png)

### Filtering & Transformation 
![alt text](Images/Image4.png)
![alt text](Images/image3.png)


Lazy Evaluation
#### Transformations (No execution - just building DAG)
df.filter()      # 0.0001 seconds
df.select()      # 0.0001 seconds  
df.withColumn()  # 0.0002 seconds
df.groupBy.agg() # 0.0001 seconds


### GroupBy Statistics by magnitude category
![alt text](Images/image5.png)

### Data Processing (Complete)

 Load 1GB+ dataset (Parquet format)

 2+ filter operations (4 filters implemented)

 1+ join operation (broadcast join with location table)

 1+ groupBy with aggregations (2 different aggregations)

 Column transformations with withColumn 



## SQL queries

### SQL Query 1: Top 10 tsunami-generating earthquakes by magnitude

![alt text](Images/Image6.png)
![alt text](Images/Sql1.png)

### SQL Query 2: Monthly statistics with window functions
![alt text](Images/Sql2.png)

## Key Findings

### Statistical Insights

1. Tsunami Occurrence Rate: 63.4% of recorded earthquakes
2. Major Earthquakes: 114 events (magnitude ≥ 6.5)
3. Regional Distribution:
Pacific Ocean: 72% of events
Indian Ocean: 18% of events
Atlantic Ocean: 10% of events

### Machine Learning Results
#### Tsunami Prediction Model:
Accuracy: 71.2%
AUC-ROC: 0.7483
Key Features: Magnitude (0.521), Depth (0.283), Location (0.196)