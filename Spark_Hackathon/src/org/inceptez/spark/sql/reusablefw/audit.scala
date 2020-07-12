package org.inceptez.spark.sql.reusablefw

class audit {
  def reconcile(df1:org.apache.spark.sql.DataFrame,df2:org.apache.spark.sql.DataFrame):Short=
  {
    val cnt1=df1.count()
    val cnt2=df2.count()
  
   
    if (cnt1==cnt2)
    return 1
    else
      return 0
  }
  
  def validateschema(df1:org.apache.spark.sql.DataFrame,df2:org.apache.spark.sql.DataFrame):Short=
  {
   
    if (df1.columns.sorted==df2.columns.sorted)
    return 1
    else
      return 0
  }  
  

}