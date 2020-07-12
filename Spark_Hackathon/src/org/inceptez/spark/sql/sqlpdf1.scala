package org.inceptez.spark.sql
import org.apache.spark._;
import org.apache.spark.sql._;
object sqlpdf1 {
  def main(args:Array[String])
  {
   val conf=new SparkConf().setAppName("spark sql").setMaster("local[*]")
   val sc=new SparkContext(conf)
   sc.setLogLevel("ERROR");
   val sqlc=new SQLContext(sc);
   val rdd1=sc.textFile("file:///home/hduser/hive/data/txns")
   val rdd2=rdd1.map(_.split(",")).map(x=>(x(0),x(1),x(3)))
   val df1=sqlc.createDataFrame(rdd2)
   df1.createOrReplaceTempView("txnsdata");
   sqlc.sql("select * from txnsdata").show;
   //import sqlc().implicits;
   //rdd1.take(10).foreach(println)
   //sprintln(rdd1.count())
   
  }
}

