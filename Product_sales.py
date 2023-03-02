from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Assignment').getOrCreate()

df_products = spark.read.option("Header", True).csv('products.csv', inferSchema=True)
df_sales = spark.read.option("Header", True).csv('sales.csv', inferSchema=True)
df_sellers = spark.read.option("Header", True).csv('sellers.csv', inferSchema=True)

a = df_sales.alias("sal").join(df_products.alias("pro"),
                               col("sal.product_id") == col("pro.product_id"),
                               "inner").drop(col("sal.product_id"))
a.show()
#Multiplying num of pieces sold and price to get total sales
from pyspark.sql.functions import col
df_ts = a.withColumn("Total_sales",
                     col("sal.num_pieces_sold") * col("pro.price"))
df_ts.show()
df_ts['product_id','product_name','Total_sales'].show()

#Total sales in descending order
from pyspark.sql.functions import desc
df_dsc = df_ts.orderBy(desc(col("Total_sales")))
df_dsc['product_id', 'product_name', 'Total_sales'].show(1)
