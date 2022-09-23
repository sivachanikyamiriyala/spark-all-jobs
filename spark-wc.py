from pyspark.sql import SparkSession
#creation of sparkdriver process
import getpass
import sys
username=getpass.getuser()
sparkdriver=SparkSession.builder.master('yarn').appName(f'${username} | siva application').\
                                                enableHiveSupport().getOrCreate()
rdd1=sparkdriver.sparkContext.textFile(sys.argv[1])
rdd2=rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd4.saveAsTextFile(sys.argv[2])
sparkdriver.stop()
