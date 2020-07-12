package org.inceptez.spark.sql.rufw

class masking {
  def hashmask(indata:String):String=
    return indata.trim().hashCode().toString()
}