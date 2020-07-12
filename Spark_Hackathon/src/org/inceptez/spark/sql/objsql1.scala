package org.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql._;
case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)

object sql1  {
 //https://spark.apache.org/docs/latest/api/sql/index.html
//https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/sql/DataFrameReader.html
  def main(args:Array[String])
  {
    println("*************** Data Munging -> Validation, Cleansing, Scrubbing, De Duplication and Replacement of Data to make it in a usable format *********************")
    println("*************** Data Enrichment -> Rename, Add, Concat, Casting of Fields *********************")
    println("*************** Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")
    println("*************** Data Processing, Analysis & Summarization -> filter, transformation, Grouping, Aggregation *********************")
    println("*************** Data Wrangling -> Lookup, Join, Enrichment *********************")
    println("*************** Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")

/*
SQLContext is a class and is used for initializing the functionalities of Spark SQL. 
SparkContext class object (sc) is required for initializing SQLContext class object.
 */
    /*val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    
    //.set("spark.sql.catalogImplementation","hive")
    .set("hive.exec.dynamic.partition.mode","nonstrict")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)*/

/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/

    
val spark=SparkSession.builder()
.appName("SQL End to End App")
.master("local[*]")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.enableHiveSupport()
.getOrCreate();

spark.sparkContext.setLogLevel("ERROR");

    // Various ways of creating dataframes

    val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    .set("spark.driver.allowMultipleContexts","true")
    
    //.set("spark.sql.catalogImplementation","hive")
    .set("hive.exec.dynamic.partition.mode","nonstrict")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)


println("UseCase1 : Understanding Creation of DF and DS from a Schema RDD, and differentiting between DF and DS wrt schema bounding")
// 1.Create dataframe using case class  with toDF and createdataframe functions (Reflection) 
// 2.create dataframe from collections type such as structtype and fields        
// 3.creating dataframe using read method using csv  and other modules like json, orc, parquet etc.
// 4.Create dataframe from hive.
// 5.Create dataframe from DB using jdbc.

    
    /* Create dataframe using case class  with toDF and createdataframe functions*/
    println("Create dataframe using case class (defined outside the main method) using Reflection")
    
    //step 1: import the implicits
    import sqlc.implicits._
    
    //step 2: create rdd
    val filerdd = sc.textFile("file:/home/hduser/hive/data/custs")

    
    //step 3: create case class (outside main method) based on the structure of the data
    
val rdd1 = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => customercaseclass(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))

val rddreject1 = filerdd.map(x => x.split(",")).filter(x => x.length != 5)


println("Creating Dataframe and DataSet")

    val filedf = rdd1.toDF();
    //dataframe=dataset[row]
    //dataset=dataset[schema]
    val fileds=rdd1.toDS();
    
    val rowrdd=filedf.rdd; // sql types, type is not preserved
    val schemardd=fileds.rdd // oops scala types, type is preserved
    
    //implicits is not required for the createDataFrame function.
    val filedf1 = sqlc.createDataFrame(rdd1)
    val fileds1 = sqlc.createDataset(rdd1)
    
    filedf.printSchema()
    filedf.show(10,false)
    
   println("DSL Queries on DF")
       
    println("DSL Queries on DS")
   fileds.select("*").filter("custprofession='Pilot' and custage>35").show(10);
  
    println("Registering Dataset as a temp view and writing sql queries")
    fileds.createOrReplaceTempView("datasettempview")
    spark.sql("select * from datasettempview where custprofession='Pilot' and custage>35").show(5,false)

println("Done with the use case of understanding Creation of DF and DS from an RDD, and differentiting between DF and DS wrt schema bounding")

println("********** Usecase2: Reading multiple files in multiple folders **********")
    // Reading multiple files in multiple folders
    
/*
cd ~
mkdir sparkdir1
mkdir sparkdir2
cp ~/sparkdata/usdata.csv ~/sparkdir1/
cp ~/sparkdata/usdata.csv ~/sparkdir1/usdata1.csv
cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata2.csv
cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata3.csv
wc -l ~/sparkdir1/usdata1.csv
500 /home/hduser/sparkdir1/usdata1.csv
*/
    
    val dfmultiple=spark.read.option("header","true").option("inferSchema",true).csv("file:///home/hduser/sparkdir*/*.csv")
    //val dfmultiple=spark.read.option("header","true").option("inferSchema",true).csv("file:///home/hduser/sparkdir1/*.csv","file:///home/hduser/sparkdata/*.csv")
    println(" Four files with 500 lines with each 1 header, after romoved 499*4=1996 ")
    
    println(dfmultiple.count)

    //creating dataframe from read method using csv

    // Removal of trailer if any
    /*
cp /home/hduser/hive/data/custs /home/hduser/hive/data/custsmodified
vi /home/hduser/hive/data/custsmodified
4000000,Apache,Spark,11,
4000001,Kristina,Chung,55,Pilot
4000001,Kristina,Chung,55,Pilot
4000002,Paige,Chen,77,Actor
4000003,Sherri,Melton,34,Reporter
4000004,Gretchen,Hill,66,Musician
,Karen,Puckett,74,Lawyer
echo "trailer_data:end of file" >> /home/hduser/hive/data/custsmodified
    wc -l /home/hduser/hive/data/custsmodified
    10002
     */

println("UseCase3 : Learning of possible activities performed on the given data")

// val dfcsv1=spark.read.option("mode","dropmalformed").option("inferschema","true").csv("file:/home/hduser/hive/data/custsmodified")
//val struct=StructType(Array(StructField))
    val dfcsv1 = spark.read.format("csv")
    //.option("header",true)
    .option("mode","dropmalformed")
    //.option("delimiter",",")
    .option("inferSchema",true)
    .load("file:/home/hduser/hive/data/custsmodified")//.toDF("cid","fname","lname","ag","prof")

    println(" Final count After removing the trailer record of the original file ")
    println(dfcsv1.count)
    
    dfcsv1.sort('_c0.desc).show(1)
    
    println("Do some basic performance improvements, we will do more later in a seperate disucussion ")
    
    val dfcsv=dfcsv1.repartition(4)
    // after the shuffle happens as a part of wide transformation (groupby,join) the partition will be increased to 200
    //dfcsv2=dfcsv.repartition(4)
    dfcsv.cache()
    //default parttions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","4")

    println("Initial count of the dataframe ")
    println(dfcsv.count())

println("*************** 1. Data Munging / Wrangling -> Preprocessing, Preparation, Validation, Cleansing, Scrubbing, De Duplication and Replacement of Data to make it in a usable format *********************")
    //Writing DSL 
    println("Dropping the null records with custid is null ")
    val dfcsva=dfcsv.na.drop(Array("_c0"));
    
    println("Replace the null with default values ")
    val dfcsvb=dfcsva.na.fill("UNKNOWN", Array("_c4"));
    println("Replace the key with the respective values in the columns ")
    val map=Map("Actor"->"Stars","Reporter"->"Media person","Musician"->"Instrumentist")
    val dfcsvc=dfcsvb.na.replace(Array("_c4"), map);
    
    println("Dropping Duplicate records ")
    val dfcsvd=dfcsvc.distinct()//.dropDuplicates()
    println("De Duplicated count")
    println(dfcsvd.count())
    dfcsvd.sort("_c0").show(10);

    //Writing equivalent SQL    
    dfcsv.createOrReplaceTempView("dfcsvview")
    val dfcsvcleanse=spark.sql("""select distinct _c0,_c1,_c2,_c3,case when _c4 is null then "UNKNOWN" 
                                  when _c4="Actor" then "Stars" when _c4="Reporter" then "Media person" 
                                  when _c4="Musician" then "Instrumentist"
                                  else _c4 end as _c4 
                                  from  dfcsvview 
                                  where _c0 is not null""")
        
    println(dfcsvcleanse.count)
    dfcsvcleanse.sort("_c0").show(10)
    
    println("Current Schema of the DF")
    dfcsvd.printSchema();
    println("Current Schema of the tempview DF")
    dfcsvcleanse.printSchema();    
 
    
    import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}
 
    //placeholder for defining columns in a df ($,col(),')
    
    println("*************** 2. Data Enrichment -> Rename, Derive/Add,Remove, Merging, Casting of Fields *********************")
    
val dfcsvd1=dfcsvd.withColumnRenamed("_c0", "custid").withColumn("IsAPilot",col("_c4").contains("Pilot")).
withColumn("Typeofdata",lit("CustInfo")).withColumn("fullname",concat('_c1,lit(" "),$"_c2")).
withColumn("Stringage",$"_c3".cast("String")).drop("_c1","_c2","_c3")
    
    dfcsvd1.printSchema
    dfcsvd1.sort("custid").show(5,false)
    
    dfcsvd.createOrReplaceTempView("customer")
    
    println("Converting DSL Equivalent SQL queries")

    println("SQL EQUIVALENT to DSL") // difference between case expression in scala/sql and case class
    spark.sql(""" select _c0 as custid,_c4, case when _c4 like ('%Pilot%') then true else false end as IsAPilot,
                         'CustInfo' as Typeofdata,concat(_c1," ",_c2) as fullname,
                          cast(_c3 as string) as stringage
                          from customer """).sort("custid").show(5,false)

                          
println("Another way of renaming column names ")
// define or change column names and datatype->  Case class using -> as[], structtype(Array(structfields)) -> schema(),
//                                               col names -> toDF(all columns),withcolumnrenamed

val dfcsvd1renamed=dfcsvd1.toDF("custid","profession","IsAPilot","TypeofData","fullname","age")

println("Ineriem dataframe with proper names to understand further steps easily ")
dfcsvd1renamed.printSchema
                          
//val classobj=new org.inceptez.spark.allfunctions

    println("*************** 3. Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")                          
                          
    println("Applying UDF in DSL")
    
    //import org.apache.spark.sql._

    println("create an object reusableobj to Instantiate the cleanup class present in org.inceptez.spark.sql.reusable pkg")
    // how to make use of an existing scala method in a DSL -> once object is created -> convert the method into udf -> apply the udf to your df columns
 /*   val maskobj=new org.inceptez.spark.sql.rufw.masking;
     val udfmask=udf(maskobj.hashmask _)
 */   
    val reusableobj=new org.inceptez.spark.sql.reusablefw.cleanup;
    
    val udftrim = udf(reusableobj.trimupperdsl _)
    
   // val dfudfapplied1=dfcsvd1renamed.withColumn("fullnameudfmasked",udfmask('fullname))
   // .drop("fullname")
    
   /* dfcsvd1renamed.select(udfmask('fullname)).show();
    dfudfapplied1.printSchema;
    dfudfapplied1.show(5,false)
*/    
    
     val dfudfapplied1=dfcsvd1renamed.withColumn("trimmedname",udftrim('fullname))
    dfudfapplied1.printSchema;
    dfudfapplied1.show(5,false)
     println(" All the below declarations of the fullname are same using ' or $ or col function")
    
    dfcsvd1renamed.select(udftrim('fullname)).show(2,false)
    dfcsvd1renamed.select(udftrim($"fullname")).show(2,false)
    dfcsvd1renamed.select(udftrim(col("fullname"))).show(2,false)
    
        
 //https://spark.apache.org/docs/2.3.0/api/sql/
    
    println("*************** 4. Data Processing, Analysis & Summarization -> filter, transformation, Grouping, Aggregation *********************")
    
    val dfgrouping = dfudfapplied1.filter("age>5").groupBy("profession").
    agg(max("age").alias("maxage"),min("age").alias("minage"))
    println("dfgrouping")
    dfgrouping.show(5,false)

    println("Converting DSL Equivalent SQL queries")
    
    dfudfapplied1.createOrReplaceTempView("customer")

    val dfsqltransformedaggr=spark.sql("""select profession,max(age) as maxage,min(age) as minage 
                  from customer 
                  where age>5
                  group by profession""")
                  
    dfsqltransformedaggr.show(5,false)
    
    println("Example for spark sql udf function registration with method overloading concept")
    // method in rdds/scala types, method in dsl udf(methodname), method in sql spark.udf.register
    val reusableobj1=new org.inceptez.spark.sql.reusablefw.transform;
      spark.udf.register("udfgeneratemailid",reusableobj1.generatemailid _)
      spark.udf.register("udfgetcategory",reusableobj1.getcategory _)

    //val udf1=udf(reusableobj1.generatemailid _)

    val dfsqltransformed=
      spark.sql("""select custid,profession,isapilot,typeofdata,age,trimmedname,udfgeneratemailid(fullname,custid) as mailid,
        udfgetcategory(profession) as cat
             from customer""")
             
      dfsqltransformed.show(10,false)  
      
      dfsqltransformed.createOrReplaceTempView("customertransformed")
      
    spark.sql("""select cat,max(age) as maxage,min(age) as minage 
                  from customertransformed 
                  where age>5
                  group by cat""").show(10,false)

                  import org.apache.spark.sql.types._
  //  val structtype1=StructType(Array(StructField("txnid",IntegerType,false),StructField("txnid",IntegerType,false)))   ;           
                  
    val txns=spark.read.option("inferschema",true).
    option("header",false).option("delimiter",",").
    csv("file:///home/hduser/hive/data/txns").
    toDF("txnid","dt","cid","amt","category","product","city","state","transtype")
    
txns.show(10);    

println("*************** 5. Data Wrangling -> Lookup, Join, Enrichment *********************")
    

txns.createOrReplaceTempView("trans");

println("Lookup and Enrichment Scenario in temp views")
val dflookupsql = spark.sql("""select a.custid,b.amt,b.transtype,case when b.transtype is null then "No Transaction" else "Transaction" end as TransactionorNot
  from customertransformed a left join trans b
  on a.custid=b.cid
  """)
  
  println("Lookup dataset")
  dflookupsql.show(10,false)

println("Join Scenario in temp views")  
val dfjoinsql = spark.sql("""select a.custid,a.profession,a.isapilot,a.typeofdata,age,mailid,cat,b.amt,b.transtype,b.city,b.product,b.dt 
  from customertransformed a inner join trans b
  on a.custid=b.cid""")

        println("Enriched final dataset")
      dfjoinsql.show(10,false)

  
  dfjoinsql.createOrReplaceTempView("joinedview")
  
  println("Identify the 2nd recent transaction performed by the customers")
val dfjoinsqlkeygen = spark.sql("""select * from (select row_number() over(partition by custid order by dt desc) as transorder,
  cat,profession,city,product
    from joinedview) as joineddata
    where transorder=2""")  

      println("Key generated final dataset")
      dfjoinsqlkeygen.show(10,false)

  println("Aggregation on the joined data set")    
    val dfjoinsqlagg = spark.sql("""select city,cat,profession,transtype,sum(amt) amount,avg(amt) from joinedview 
      group by city,cat,profession,transtype
      having sum(amt)>50""")

      println("Aggregated final dataset")
      dfjoinsqlagg.show(10,false)

      
println("*************** 6. Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")

println("Writing to Hive Partition table")

spark.sql("create database if not exists sparkdb")

spark.sql("""use sparkdb""")

spark.sql("""create table IF NOT EXISTS customerhive_2(custid INT,profession STRING,isapilot STRING ) partitioned by (dt date)""")

spark.sql("""insert into table customerhive_2 partition(dt) select custid,profession,isapilot,dt from joinedview""")
  
dfjoinsql.write.mode("overwrite").partitionBy("dt").saveAsTable("sparkdb.customerhive_2")
      
val hivetblcnt=spark.sql("""select count(1) from sparkdb.customerhive_2""").show
      
println("Hive Table count is "+hivetblcnt);

println("Writing in JSON Format")
dfjoinsqlkeygen.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/custjson")
 
println("Writing to mysql")

val prop=new java.util.Properties();
prop.put("user", "root")
prop.put("password", "root")

dfjoinsqlagg.write.mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","customeraggreports",prop)


  }
 
  
  
}





