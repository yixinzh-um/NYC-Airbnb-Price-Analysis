from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
sc = SparkContext(appName="PySparksi618f19project1")
sqlContext=SQLContext(sc)
df_airbnb=sqlContext.read.option("multiline", "true").option("quote", '"').option("escape", "\\").option("escape", '"').csv('project1/listings.csv', header=True)
df_airbnb.registerTempTable('airbnb')
df_airbnb_p=sqlContext.sql("select neighbourhood_group_cleansed, room_type,price from airbnb where neighbourhood_group_cleansed is not null and price is not null and room_type is not null")
df_airbnb_p_rdd=df_airbnb_p.rdd.map(lambda x: (x[0].strip(),x[1],x[2][1:].strip()))


df = spark.createDataFrame(df_airbnb_p_rdd).toDF("neighbourhood_group_cleansed", "room_type","price")
df.registerTempTable("nrp")

#####################################

df_census=sqlContext.read.option("quote", '"').option("escape", "\\").option("escape", '"').csv('project1/nyc_census_tracts.csv',header=True)
df_census.registerTempTable('census')

df_bi=sqlContext.sql('''select Borough, TotalPop, IncomePerCap*TotalPop as Income, employed from census 
                        where Borough is not null and TotalPop is not null and IncomePerCap is not null and Unemployment is not null
''')
df_bi.registerTempTable('bi')
df_bi=sqlContext.sql('''select Borough, SUM(Income)/SUM(TotalPop) as IncomePerCap, SUM(employed)/SUM(TotalPop) as employment from bi 
                        group by Borough''')
df_bi.registerTempTable('bi')
########################
df_nrp=sqlContext.sql("""select neighbourhood_group_cleansed, room_type, mean(cast(price as float)) as avg_price from nrp
                group by neighbourhood_group_cleansed,room_type
                order by neighbourhood_group_cleansed,room_type
""")
df_np=sqlContext.sql("""select neighbourhood_group_cleansed, mean(cast(price as float)) as avg_price from nrp
                group by neighbourhood_group_cleansed
                order by avg_price""")
df_nrp.registerTempTable("nrp")
df_np.registerTempTable("np")
df=sqlContext.sql("""select nrp.neighbourhood_group_cleansed, nrp.room_type, bi.IncomePerCap, nrp.avg_price
                    from nrp 
                    join bi on bi.Borough == nrp.neighbourhood_group_cleansed
                    order by nrp.neighbourhood_group_cleansed, nrp.room_type
""") 

df2=sqlContext.sql("""select np.neighbourhood_group_cleansed, bi.employment, np.avg_price
                    from np 
                    join bi on bi.Borough == np.neighbourhood_group_cleansed
""") 
df2_rdd=df2.rdd.sortBy(lambda x:(x[1],x[2]),ascending=False)
df2 = spark.createDataFrame(df2_rdd).toDF("neighborhood","employment","avg_price")
df.write.format('csv').option('delimiter','\t').save('project1_3')


