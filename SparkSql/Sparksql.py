# -*- coding: utf-8 -*-
"""si618_hw6_czj.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/102p6tcTa5nj7amka6K3ggI93mMWFTxwV
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext(appName="lecture6")
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

review = sqlContext.read.json('hdfs:///data/umsi618f20/hw6/review.json').select('business_id','stars')
review.registerTempTable('review')
business = sqlContext.read.json('hdfs:///data/umsi618f20/hw6/business.json').select('business_id','stars','city')

business.registerTempTable('business')
q1 = sqlContext.sql('select b.business_id, (r.stars-b.stars) as star from business b right join review r on b.business_id = r.business_id order by star desc')
q1.show()
q1.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('hw6q1')

q1.registerTempTable('q1_tbl') 
q2 = sqlContext.sql('select business_id, avg(star) as rating from q1_tbl group by business_id order by rating')
q2.show()
q2.write.format('csv').option('delimiter','\t').save('hw6_output_2')

q2.registerTempTable('q2_tbl')

q3 = sqlContext.sql('select b.city, avg(q.rating) from business b right join q2_tbl q on b.business_id = q.business_id group by b.city order by avg(q.rating) Desc') 
q3.show()
q3.write.format('csv').option('delimiter','\t').save('hw6_output_3')

#spark-submit --master yarn --num-executors 16 --executor-memory 1g --executor-cores 8 si618_hw6_czj.py