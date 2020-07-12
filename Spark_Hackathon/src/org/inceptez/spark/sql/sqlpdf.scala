package org.inceptez.spark.sql

import org.apache.spark._;
import org.apache.spark.sql._;

object sqlpdf {
  case class prof(age:Int,profession:String);
def main(args:Array[String])
{
  //spark 1.x or 2.x
  val sparkconf=new SparkConf()
  .setMaster("local[*]")
  .setAppName("Spark SQL learning");
  
  val sc =new SparkContext(sparkconf);
  
  val sqlc=new SQLContext(sc);
  //val sqlc1=new SQLContext(sc);
  
  import sqlc.implicits;

  //version 2.0.x    
  val sparksess=org.apache.spark.sql.SparkSession
  .builder().enableHiveSupport().appName("Spark sql sesion")
  .master("local[*]").getOrCreate();
  
  // manually create objects (sparkcontext, sqlcontext by using new keyword)
  // initialize sparkconf, create sparkcontext object using new kv in the name of sc, 
  // create sqlcontext object using new keyword in the name of abc
  // all above manual tasks are takencare by sparksession (singleton object)
  // reuse if exist otherwise create using getorcreate and builder. 

  // 1. Create dataframe from RDD using toDF or createdataframe function from reflection
  //    - customization
  // 2. Create df using collections or programatically (structype/structfield)
  // more options of datatypes, performance is good with the spark sql 
  // additional datatypes,
  // 3. Create df using existing modules csv (version 2x), json, orc, parquet etc.
  // 4. Create df using jdbc.
  // 5. Create df using hive.
  
  //rdd - notepad
  //df - xls
  //table - table

  val filerdd=sparksess.sparkContext.textFile("file:///home/hduser/hive/data/custs");
   
  val schemardd=filerdd.map(_.split(",")).filter(x=>x.length==5)
  .map(x=>prof(x(3).toInt,x(4)))
  //.map(x=>(x(0).toLong,x(1),x(2),x(3).toInt,x(4)));
  
  import sparksess.sqlContext.implicits._;
  
  val dfdf=schemardd.toDF();
  //dfdf.sele
  val df1=sparksess.sqlContext.createDataFrame(schemardd);
  
  df1.createOrReplaceTempView("profage")
  
  sqlc.sql("""select count(1) from profage""").show;
  
  
}
}






