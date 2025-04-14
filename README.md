# Use-Delta-Lake
# Delta Lake with Apache Spark in Azure Synapse Analytics

This project demonstrates how to use **Delta Lake** with **Apache Spark** in **Azure Synapse Analytics** to implement a Lakehouse architecture. You'll learn how to load data into Delta tables, perform updates, use time travel, handle streaming data, and query Delta tables using both Spark and Serverless SQL pools.

## 📌 Prerequisites

- An active [Azure subscription](https://portal.azure.com/)
- Basic knowledge of:
  - Apache Spark
  - Azure Synapse Studio
  - Delta Lake concepts

## 🚀 Technologies Used

- Azure Synapse Analytics
- Apache Spark (PySpark)
- Delta Lake
- Azure Data Lake Storage Gen2
- Structured Streaming
- Serverless SQL Pool

## 🧪 What You'll Learn

- How to create Delta tables in Azure Synapse using PySpark
- Perform data updates and leverage Delta Lake’s **time travel** capabilities
- Work with **external** and **managed** catalog tables
- Process **streaming data** into Delta format
- Query Delta tables using **Spark SQL** and **Serverless SQL pools**

---

## 🛠 Setup Instructions

### 1. Clone the Lab Repository

In the **Azure Cloud Shell (PowerShell)**:

```powershell
rm -r dp-203 -f
git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
cd dp-203/Allfiles/labs/07
./setup.ps1
```
 ## Choose your subscription and set a Synapse SQL pool password when prompted.

The script provisions:

-Azure Synapse Analytics workspace
-Apache Spark pool
-Azure Data Lake Storage
-Uploads the products.csv file
## 📊 Explore and Transform Data
### Load CSV into DataFrame
```python
df = spark.read.load('abfss://files@datalakexxxxxxx.dfs.core.windows.net/products/products.csv', format='csv', header=True)
display(df.limit(10))
```
### Save as Delta Table
```python
delta_table_path = "/delta/products-delta"
df.write.format("delta").save(delta_table_path)
```
### Update Delta Table
```python
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.update("ProductID == 771", { "ListPrice": "ListPrice * 0.9" })
```
### Time Travel
```python
df_version0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
df_version0.show()
```
### View Table History
```python
deltaTable.history().show(truncate=False)
```

## 📁 Create Catalog Tables
### External Tables
```python
spark.sql("CREATE DATABASE AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '/delta/products-delta'")
```
### Managed Table
```python
df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
```
### Query with SQL
```sql
%%sql
USE AdventureWorks;
SELECT * FROM ProductsManaged;
```
## 🔄 Delta Lake with Streaming
### Simulate IoT Data Stream
```python
from notebookutils import mssparkutils
from pyspark.sql.types import *

inputPath = '/data/'
mssparkutils.fs.mkdirs(inputPath)

jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
])

iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)
```
### Write stream to Delta Table
```python
delta_stream_table_path = '/delta/iotdevicedata'
checkpointpath = '/delta/checkpoint'

deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
```
### Create Catalog Table for Stream
```python
spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '/delta/iotdevicedata'")
```
## 🧾 Query Delta Table using Serverless SQL Pool
```sql
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<your-storage-account>.dfs.core.windows.net/files/delta/products-delta/',
    FORMAT = 'DELTA'
) AS result;
```
You can also query catalog tables:
```sql
USE AdventureWorks;
SELECT * FROM Products;
```
## 📚 Resources
-Delta Lake Documentation
-Azure Synapse Analytics Documentation
-Structured Streaming Guide







