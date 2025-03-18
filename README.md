# Amazon-Pyspark-Project

#importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,count,desc,length,when,col


#creating df1 by amazon.csv
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/atharvashinde003@gmail.com/amazon.csv")


#Display in spark
df1.display()


df1.printSchema()


#to select only the product name
df1.select('product_name').display()


#top product by rating on amazon
top_rated_product = df1.groupBy('product_id','product_name').agg(avg('rating').alias('avg_rating')).orderBy(desc('avg_rating')).limit(10)
top_rated_product.display()


#most reviewed product
top_reviewed_product = df1.groupBy('product_id','product_name').count().orderBy(desc('count')).limit(10)
top_reviewed_product.display()


#discount analysis
discount_analysis = df1.groupBy('category').agg(avg('discount_percentage').alias('avg_discount'))
discount_analysis.display()


#User Engagement
user_engagement = df1.groupBy('product_id').agg(avg('rating').alias('avg_rating'),count('rating').alias('rating_count'))
user_engagement.display()


#creating the temp table from df1
df1.createOrReplaceTempView('amazon_sales_table')


%sql
select count(*) from amazon_sales_table


%sql
select * from amazon_sales_table order by product_id desc limit 10
