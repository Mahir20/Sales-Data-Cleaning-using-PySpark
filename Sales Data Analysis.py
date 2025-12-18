
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *



# File uploaded to /FileStore/tables/sales_csv.txt
# File uploaded to /FileStore/tables/menu_csv.txt
schema1 = StructType([
    StructField('Product_id',IntegerType(), True),
    StructField('Customer_id',StringType(), True),
    StructField('Order_date',DateType(), True),
    StructField('Location',StringType(), True),
    StructField('Source_order',StringType(), True)
])

schema2 = StructType([
    StructField('Product_id', IntegerType(), True),
    StructField('Product_name', StringType(), True),
    StructField('Price', StringType(), True)
    
])

df1 = spark.read.format('csv').option('inferschema', True).schema(schema1).load('/FileStore/tables/sales_csv.txt')
df2 = spark.read.format('csv').option('inferschema', True).schema(schema2).load('/FileStore/tables/menu_csv.txt')



df1.display()
df2.display()

# COMMAND ----------

sales_df = df1.withColumn('order_year',year(col("Order_date")))
sales_df = sales_df.withColumn('order_month',month(col("Order_date")))
sales_df = sales_df.withColumn('order_quarter',quarter(col("Order_date")))
sales_df.display()
df2.display()



# Total amount spent by each customers
total_amount_spent = (sales_df.join(df2,'Product_id').groupBy('Customer_id').agg(sum('price').alias('total')).orderBy('Customer_id'))
total_amount_spent.display()




# Total amount spent by each category
total_amount_spent_category = sales_df.join(df2,'Product_id').groupBy('Product_name').agg(sum('price')).orderBy('Product_name')
total_amount_spent_category.display()



# Total amount spent in each month
total_spent_month = sales_df.join(df2,'Product_id').groupBy('order_month').agg(sum('price').alias('total_spent')).orderBy('order_month')
total_spent_month.display()



# Total amount spent in each year
total_spent_year = sales_df.join(df2,'Product_id').groupBy('order_year').agg(sum('price').alias('total_spent')).orderBy('order_year')
total_spent_year.display()



# Total amount spent in each quaterly
total_spent_quater = sales_df.join(df2,'Product_id').groupBy('order_quarter').agg(sum('price').alias('total_spent')).orderBy('order_quarter')
total_spent_quater.display()

# COMMAND ----------

# how many time each item purches
total_order_catagory = (
    sales_df.join(df2,'Product_id')
    .groupBy('Product_name')
    .agg(count('product_id').alias('item_cnt'))
    .orderBy(col('item_cnt').asc())
    )
total_order_catagory.display()



    # top 5 ordered item
top_five_item = (
    sales_df
    .join(df2, "Product_id")
    .groupBy("Product_name",'Product_id')
    .agg(count("Product_id").alias("product_count"))
    .orderBy(col("product_count").desc())
    .limit(5)
)
top_five_item.display()


# frequncy of customer visited
frequncy_customer_visited = sales_df.groupBy(col('Customer_id'),col('Location')).agg(count('Customer_id')).orderBy(col("Customer_id"))
frequncy_customer_visited.display()


# COMMAND ----------

# Top orderd item
top_ordered_item = (sales_df.join(df2,'product_id')
    .groupBy('Product_id','Product_name')
    .agg(count('Product_id').alias('total'))
    .orderBy(col('total').desc())
    .limit(1)
    
)
top_ordered_item.display()



# Total sales by each country
total_sale_country = (sales_df.join(df2,'Product_id')
                      .groupBy('Location')
                      .agg(sum('price').alias('total'))
                      .orderBy(col('total').desc())
                      )
total_sale_country.display()



# Total sales by each order_source
total_sale_order_source = (
    sales_df.join(df2,'Product_id')
    .groupBy('Source_order')
    .agg(sum('price').alias('total'))
    .orderBy(col('total').desc())
    )
total_sale_order_source.display()
