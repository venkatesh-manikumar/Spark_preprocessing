package org.inceptez.spark.sql.reusablefw

class cleanup extends java.io.Serializable {
  def trimupperdsl(i:String):String=
    {return i.trim().toUpperCase()}

  def trimupper(i:String):String=
    {return i.trim().toUpperCase()}

}