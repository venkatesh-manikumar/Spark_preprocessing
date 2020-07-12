package org.inceptez.spark.sql.reusablefw

class transform  extends java.io.Serializable
{
   def generatemailid(i:String,j:String):String=
    {
    return i.trim().capitalize.replace(" ",".").replace("-",".")+j+"@inceptez.com"
    }
    
    def getcategory(i:String):String=i.trim() match
    {
      case "Police officer"|"Politician"|"Judge"=> "Government"
      case "Automotive mechanic"|"Economist"|"Loan officer"=> "Worker"
      case "Psychologist"|"Veterinarian"|"Doctor"=> "Medical"
      case "Civil engineer"|"Computer hardware engineer"|"Engineering technician"=> "Engineering"  
      case _ => "Others"
    }
}