package org.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
//case class customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
    
object objsql2 {
case class transcls (transid:String,transdt:String,custid:String,salesamt:Float,category:String,prodname:String,state:String,city:String,payment:String)
case class customer (custid:Int,firstname:String,lastname:String,city:String,age:Int,createdt:String,transactamt:Long)

  def main(args:Array[String])
  {
/*
SQLContext is a class and is used for initializing the functionalities of Spark SQL. 
SparkContext class object (sc) is required for initializing SQLContext class object.
 */
    /*val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sc.setLogLevel("ERROR");
*/
/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/
   
println("We are going to understand options to acquire and store the hetrogeneous data from hetrogenous sources and stores")
// unstructured/structured/semi struct, fs, db (RDBMS/Hive datastore), dfs, cloud, nosql, message queues, sockets, streaming files
// volume, variety, velocity

val spark=SparkSession.builder().appName("Sample sql app").master("local[*]")
.config("hive.metastore.uris","thrift://localhost:9083")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.enableHiveSupport().getOrCreate();


spark.sparkContext.setLogLevel("error")

val sqlc=spark.sqlContext;

//CSV READ/WRITE databricks
println("Reading csv format")
val dfcsv=spark.read.option("delimiter",",").option("inferschema","true").option("header","true").
option("quote","\"").option("escape","-").option("ignoreLeadingWhiteSpace","false").
option("ignoreTrailingWhiteSpace","false").option("nullValue","na").option("nanValue","0").
option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSZZ").option("dateFormat","yyyy-MM-dd").
option("maxCharsPerColumn","1000").option("mode","dropmalformed").csv("file:///home/hduser/sparkdata/sales.csv")

//xls,pdf -> 

// bigdata sv, fixed, json, xml

dfcsv.show(5,false)
/* You can set the following CSV-specific options to deal with CSV files:

    sep/delimiter (default ,): sets the single character as a separator for each field and value.
    encoding (default UTF-8): decodes the CSV files by the given encoding type.
    quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behaviour is different form com.databricks.spark.csv.
    escape (default \): sets the single character used for escaping quotes inside an already quoted value.
    comment (default empty string): sets the single character used for skipping lines beginning with this character. By default, it is disabled.
    header (default false): uses the first line as names of columns.
    inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
    ignoreLeadingWhiteSpace (default false): defines whether or not leading whitespaces from values being read should be skipped.
    ignoreTrailingWhiteSpace (default false): defines whether or not trailing whitespaces from values being read should be skipped.
    nullValue (default empty string): sets the string representation of a null value. 
                                      Since 2.0.1, this applies to all supported types including the string type.
    nanValue (default NaN): sets the string representation of a non-number" value.
    positiveInf (default Inf): sets the string representation of a positive infinity value.
    negativeInf (default -Inf): sets the string representation of a negative infinity value.
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. 
    Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format.Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
    java.sql.Timestamp.valueOf() and java.sql.Date.valueOf() or ISO 8601 format.
    maxColumns (default 20480): defines a hard limit of how many columns a record can have.
    maxCharsPerColumn (default 1000000): defines the maximum number of characters allowed for any given value being read.
    maxMalformedLogPerPartition (default 10): sets the maximum number of malformed rows Spark will log for each partition. Malformed records beyond this number will be ignored.
    mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
        PERMISSIVE : sets other fields to null when it meets a corrupted record. When a schema is set by user, it sets null for extra fields.
        DROPMALFORMED : ignores the whole corrupted records.
        FAILFAST : throws an exception when it meets corrupted records.
*/

println("Writing in csv format")
dfcsv.coalesce(1).write.mode("overwrite").option("header","true").option("timestampFormat","yyyy-MM-dd HH:mm:ss")
.option("nullValue","nullval")
.option("compression","gzip")
//.partitionBy("dt")
.csv("hdfs://localhost:54310/user/hduser/csvdataout")

spark.read.option("header","true").csv("hdfs://localhost:54310/user/hduser/csvdataout").show(5,false)

/* You can set the following CSV-specific option(s) for writing CSV files:

    sep (default ,): sets the single character as a separator for each field and value.
    quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value.
    escape (default \): sets the single character used for escaping quotes inside an already quoted value.
    escapeQuotes (default true): a flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character.
    quoteAll (default false): A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character.
    header (default false): writes the names of columns as the first line.
    nullValue (default empty string): sets the string representation of a null value.
    compression (default null): compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

//JSON READ/WRITE
println("Reading in json format")
val dfjson=spark.read.option("prefersDecimal","true").option("allowUnquotedFieldNames","true")
.option("allowSingleQuotes","true").option("allowNumericLeadingZeros","true").option("mode","permissive")
.option("timestampFormat","yyyy-MM-dd HH:mm:ss").option("dateFormat","yyyy-MM-dd").option("columnNameOfCorruptRecord", "corrupt")
.json("file:///home/hduser/sparkdata/auctiondata.json")

dfjson.show(5,false)
/* You can set the following JSON-specific options to deal with non-standard JSON files:

    primitivesAsString (default false): infers all primitive values as a string type
    prefersDecimal (default false): infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.
    allowComments (default false): ignores Java/C++ style comment in JSON records
    allowUnquotedFieldNames (default false): allows unquoted JSON field names
    allowSingleQuotes (default true): allows single quotes in addition to double quotes
    allowNumericLeadingZeros (default false): allows leading zeros in numbers (e.g. 00012)
    allowBackslashEscapingAnyCharacter (default false): allows accepting quoting of all character using backslash quoting mechanism
    mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
        PERMISSIVE : sets other fields to null when it meets a corrupted record, and puts the malformed string into a new field configured by columnNameOfCorruptRecord. When a schema is set by user, it sets null for extra fields.
        DROPMALFORMED : ignores the whole corrupted records.
        FAILFAST : throws an exception when it meets corrupted records.
    columnNameOfCorruptRecord (default is the value specified in spark.sql.columnNameOfCorruptRecord): allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

println("Writing in json format")
dfjson.coalesce(1).write.mode("overwrite").option("compression","bzip2").option("timestampFormat","yyyy-MM-dd HH:mm:ss")
.option("dateFormat","yyyy-dd-mm").json("hdfs://localhost:54310/user/hduser/jsonout")

spark.read.json("hdfs://localhost:54310/user/hduser/jsonout").show(5,false)

/* You can set the following JSON-specific option(s) for writing JSON files:

    compression (default null): compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

//JDBC READ/WRITE

import sqlc.implicits._  
println("Creating dataframe using jdbc connector for DB")

    val tbl="(select * from customer) t"
    
val dsdb = spark.read.format("jdbc")
       .option("url", "jdbc:mysql://localhost/custdb")
//       .option("driver", "com.mysql.jdbc.Driver")
       .option("dbtable", "customer")
       .option("user", "root")
       .option("password", "root")
       .option("columnname","custid")
       .option("lowerBound",1)
       .option("upperBound",10000)
       .option("numPartitions",3)
       .load().as[customer]
// cid -1 to 10000 -> 1 to 3333 (conn1), 3334 -> 6666 (conn1) , > 6666 (conn3 - 8333) -> 15000
  dsdb.show(5,false)
  val dfdb=dsdb.toDF();
dfdb.cache();
/*  url - JDBC database url of the form jdbc:subprotocol:subname.
    table - Name of the table in the external database.
    columnName - the name of a column of integral type that will be used for partitioning.
    lowerBound - the minimum value of columnName used to decide partition stride.
    upperBound - the maximum value of columnName used to decide partition stride.
    numPartitions - the number of partitions. This, along with lowerBound (inclusive), upperBound (exclusive), form partition strides for generated WHERE clause expressions used to split the column columnName evenly.
    connectionProperties - JDBC database connection arguments, a list of arbitrary string tag/value. Normally at least a "user" and "password" property should be included. "fetchsize" can be used to control the number of rows per fetch.
*/
println("Writing to mysql")

val prop=new java.util.Properties();
prop.put("user", "root")
prop.put("password", "root")

dfdb.write.mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","customerspark",prop)


//HIVE READ/WRITE
println("Writing to hive")

dfdb.createOrReplaceTempView("dfdbview")

spark.sql("drop table if exists dfdbviewhive")
//spark.sql("create external table default.dfdbviewhive(id int,name string)")
//spark.sql("")
dfdb.write.mode("overwrite").saveAsTable("default.customermysql")    
dsdb.write.mode("overwrite").partitionBy("city").saveAsTable("default.customermysqldscity")

println("Displaying hive data")

val hivedf=spark.read.table("default.customermysql")

val hivedf1=spark.sql("select * from default.customermysql");

println("Validate the count of the data loaded from RDBMS to Hive")
println("Schema of RDBMS table")
dfdb.printSchema();
println("Schema of Hive table")
hivedf.printSchema();

val auditobj=new org.inceptez.spark.sql.reusablefw.audit;
if (auditobj.reconcile(dfdb,hivedf)==1)
{
  println("Hive Data load reconcilation success, running rest of the steps")
spark.sql("select * from default.customermysql where city='chennai'").show(10,false);
hivedf.show(5,false)

spark.sql("select * from default.customermysqldscity").show(5,false)
spark.sql("describe default.customermysqldscity").show(5,false)
spark.sql("describe formatted default.customermysqldscity").show(5,false)
spark.sql("select * from default.customermysqldscity").printSchema()

spark.sql("""create external table if not exists default.customermysqldscitypart1 (custid integer
  , firstname string,lastname string, age integer,createdt date)
   partitioned by (city string)
row format delimited fields terminated by ',' 
location '/user/hduser/customermysqldscitypart'""")
dsdb.write.mode("overwrite").partitionBy("city").saveAsTable("default.customermysqldscitypart1")

//PARQUET/ORC READ/WRITE
println("Writing parquet data")
hivedf.write.mode("overwrite").option("compression","none").parquet("hdfs://localhost:54310/user/hduser/hivetoparquet")
println("Writing orc data")
hivedf.write.mode("overwrite").option("compression","none").orc("hdfs://localhost:54310/user/hduser/hivetoorc")

println("Displaying parquet data")
spark.read.option("compression","none").parquet("hdfs://localhost:54310/user/hduser/hivetoparquet").show(10,false)
println("Displaying orc data")
spark.read.option("compression","none").option("timestampFormat","yyyy-MM-dd HH:mm:ss").option("dateFormat","yyyy-dd-mm").orc("hdfs://localhost:54310/user/hduser/hivetoorc").show(10,false)
}   
else
  println("Hive Data load reconcilation failed, not running rest of the steps")
  }
 
  
}









