from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\H1bEmployerInformation1.csv"
df = spark.read.format("csv").option("header", "true").option("sep","\t").load(data)
df.show(5)
#remove special characters from header/schema
df=df.toDF(*[re.sub('[^A-Za-z0-9]','',c) for c in df.columns])
#remove special characters from original data
df=df.select(*(regexp_replace(col(c),'[^a-zA-Z0-9]','').alias(c) for c in df.columns))
df.show()
# Data cleaning: handle missing values
# Data cleaning: handle missing values
df = df.na.fill({
    "InitialApproval": 0,
    "InitialDenial": 0,
    "ContinuingApproval": 0,
    "ContinuingDenial": 0
})

# Transform data types if necessary
df = df.withColumn("FiscalYear", col("FiscalYear").cast("integer"))
#get who is in software industry, who's h1b approved more than 10
res=df.where((col("PetitionerState")=="TX") & ((col("ContinuingApproval")>10) | (col("InitialApproval")>10)))
res.show(20,truncate=False)
# Example of correcting an invalid TaxID format
df = df.withColumn("TaxID", when(col("TaxID").rlike("^[0-9]{4}$"), col("TaxID")).otherwise(None))

df.show()
# each NAICS code how many number of employers available.
industry_distribution = df.groupBy("IndustryNAICSCode").agg(count("EmployerPetitionerName").alias("NumberOfEmployers")).orderBy(col("NumberOfEmployers").desc())

industry_distribution.show(int(industry_distribution.count()))
#other python packages also u can use in pyspark

import matplotlib.pyplot as plt
import pandas as pd

# Aggregate data by state
state_distribution = df.groupBy("PetitionerState").agg(count("EmployerPetitionerName").alias("NumberOfEmployers"))

# Convert to Pandas DataFrame for plotting
state_pd = state_distribution.toPandas()

# Plotting
state_pd.plot(kind='bar', x='PetitionerState', y='NumberOfEmployers', title='H1B Employers by State')
plt.show()

# Approval and Denial Rates Calculation

# Calculate approval and denial rates
approval_denial_rates = df.groupBy("PetitionerState").agg(
    sum("InitialApproval").alias("TotalInitialApprovals"),
    sum("InitialDenial").alias("TotalInitialDenials"),
    sum("ContinuingApproval").alias("TotalContinuingApprovals"),
    sum("ContinuingDenial").alias("TotalContinuingDenials")
)

approval_denial_rates.show()
#High Denial Rate Identification, which is fraud company identify top 10
# Calculate denial rates
df = df.na.fill(0).withColumn("InitialDenialRate", col("InitialDenial") / (col("InitialApproval") + col("InitialDenial")))
df = df.na.fill(0).withColumn("ContinuingDenialRate", col("ContinuingDenial") / (col("ContinuingApproval") + col("ContinuingDenial")))

# Identify companies with highest denial rates
high_denial_companies = df.orderBy(col("InitialDenialRate").desc()).limit(20)
high_denial_companies.show()

#. Top Companies by Highest Continuing Approval Rate
# Calculate the continuing approval rate
df = df.where(col("ContinuingApproval")>10).withColumn("ContinuingApprovalRate", col("ContinuingApproval") / (col("ContinuingApproval") + col("ContinuingDenial")))

# Find the top 10 companies with the highest continuing approval rate
top_continuing_approval_companies = df.orderBy(col("ContinuingApprovalRate").desc()).select("EmployerPetitionerName", "ContinuingApprovalRate").limit(10)
top_continuing_approval_companies.show()
