# -Dmart-analysis-using-pyspark
Sales Data Analysis using PySpark
This project performs data cleaning, transformation, and analytical queries on a sales dataset using Apache Spark (PySpark). The data is assumed to be stored in a Databricks workspace under the default schema.

🗂️ Datasets Used
Three tables are read from the workspace:

workspace.default.product: Contains product details.

workspace.default.sales: Contains transactional sales data.

workspace.default.customer: Contains customer demographic and geographic information.

⚙️ Steps Performed
1. Load Data
Data is loaded from Spark tables into PySpark DataFrames:

python
Copy
Edit
product_df = spark.table("workspace.default.product")
sales_df = spark.table("workspace.default.sales")
customer_df = spark.table("workspace.default.customer")
2. Inspect Data
Display a sample of each DataFrame using .show().

3. Clean Column Names
Column names with spaces are replaced with underscores for easier referencing:

python
Copy
Edit
def clean_columns(df):
    return df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])
4. Type Casting
Cast key numerical fields to appropriate types:

Sales, Discount, Profit → FloatType

Quantity, Age → IntegerType

5. Join DataFrames
Merge all three DataFrames into a unified dataset for analysis:

Join sales_df with product_df on Product_ID

Join the result with customer_df on Customer_ID

🔍 Analytical Queries
1. Total Sales by Product Category
sql
Copy
Edit
GROUP BY Category
2. Customer with the Most Purchases
sql
Copy
Edit
GROUP BY Customer_ID
ORDER BY count(*) DESC
LIMIT 1
3. Average Discount on Sales
sql
Copy
Edit
SELECT AVG(Discount)
4. Unique Products Sold in Each Region
sql
Copy
Edit
GROUP BY Region
5. Total Profit by State
sql
Copy
Edit
GROUP BY State
ORDER BY sum(Profit) DESC
6. Top-Selling Product Sub-Category
sql
Copy
Edit
GROUP BY Sub_Category
ORDER BY sum(Sales) DESC
LIMIT 1
7. Average Customer Age by Segment
sql
Copy
Edit
GROUP BY Segment
8. Order Count by Shipping Mode
sql
Copy
Edit
GROUP BY Ship_Mode
9. Total Quantity Sold per City
sql
Copy
Edit
GROUP BY City
10. Most Profitable Customer Segment
sql
Copy
Edit
GROUP BY Segment
ORDER BY sum(Profit) DESC
LIMIT 1
💡 Requirements
Databricks or Apache Spark environment

Python (with PySpark installed)

