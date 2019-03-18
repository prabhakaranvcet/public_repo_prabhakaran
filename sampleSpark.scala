package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import shapeless.ops.nat.ToInt
import org.spark_project.dmg.pmml.DataType
import org.apache.spark.sql.functions.udf;

case class insuredata(id1:String,id2:String,year:String,stcd:String,srcnm:String,network:String,url:String);

object sampleSpark {

  def main(args:Array[String]){
    val sc = SparkSession.builder().appName("sample").master("local[*]").getOrCreate();
    val insuranceInfo1 = sc.sparkContext.textFile("E:\\spark_hack2\\spark_hack2\\insuranceinfo1.csv");
    val insuranceInfo2 = sc.sparkContext.textFile("E:\\spark_hack2\\spark_hack2\\insuranceinfo2.csv");
    val firstrow = insuranceInfo1.first();
    val firstrow2 = insuranceInfo2.first();
    
    val headerless1 = insuranceInfo1.filter(row => row!=(firstrow));
    val headerless2 = insuranceInfo2.filter(row => row!=(firstrow2));
    
    val mergedInsurance = headerless1.union(headerless2);

    //finding duplicates
//    mergedInsurance.map(i=
    
    mergedInsurance.cache();
    
    //mergedcount = info1count + infocount2
    val info1 = println(insuranceInfo1.count());
    val info2 = println(insuranceInfo2.count());
    val merged = println(mergedInsurance.count());
    
    mergedInsurance.repartition(8);
    
    //custstates df
    val custStates = sc.sparkContext.textFile("E:\\spark_hack2\\spark_hack2\\custs_states.csv");
    
    //filter rows with different columns
    val mappedRdd = custStates.map(row=>row.split(",")).cache();
    val twoRows = mappedRdd.filter(row=>row.length<=2);
    val fiveRows = mappedRdd.filter(row=>row.length<=5);
    
    //masking UDF
    val maskedUDF = udf[String,String](maskingUDF);
    //uppercase UDF
    val appendAndUCUDF = udf[String,String,String](appendAndUC);
    
    val mergedwithcaseclass = mergedInsurance.
                    map(row=>row.split(",")).
                    map(row=>insuredata(row(0),row(1),row(2),row(3),row(4),row(5),row(6)));
    val sqlcontext=sc.sqlContext;
    import sqlcontext.implicits._;
        
    val mergedDF=mergedwithcaseclass.toDF();
    mergedDF.createOrReplaceTempView("merged");
    
    val finalDF = mergedDF.select($"id1",$"id2",appendAndUCUDF($"stcd",$"network"),maskedUDF($"url"));
    
    finalDF.rdd.saveAsTextFile("E:\\spark_hack2\\spark_hack2\\output\\test.txt");
    
    finalDF.write.json("E:\\spark_hack2\\spark_hack2\\output\\test.json");
    finalDF.write.parquet("E:\\spark_hack2\\spark_hack2\\output\\test.parquet");
  }
  
  def maskingUDF(param: String) : String = {
		var maskedParam = "";
    val numerical = 49;
    param.map(chr=>{
    	val toMaskParam = chr.toChar;
    	if(toMaskParam >=48 && toMaskParam <=57){
    		maskedParam = maskedParam + (numerical.toChar.toString()); 
    	}else{
    		maskedParam= maskedParam+((toMaskParam+1).toChar.toString());
    	}
    })
    return maskedParam;
  }
  
  def appendAndUC(args1:String,args2:String) : String = {
    return args1.toUpperCase()+args2.toUpperCase();
  }
}