import org.apache.spark.sql.SparkSession

case class stats_class(match_id:String,series_id:String,match_details:String,result:String,
    scores:String,date:String,venue:String,round:String,home:String,away:String,winner:String,
    win_by_runs:String,win_by_wickets:String,balls_remaining:String,innings1:String,innings1_runs:String,
    innings1_wickets:String,innings1_overs_batted:String,innings1_overs:String,innings2:String,
    innings2_runs:String,innings2_wickets:String,innings2_overs_batted:String,innings2_overs:String,
    DLmethod:String,target:String);

object dashboard {
	val path = "E:\\bigdata\\t20_matches.csv"
  
  def main(args:Array[String]){
    
    val sample_commit;
    
    val spark = SparkSession.builder().appName("Samplespark")
                .master("local[*]").enableHiveSupport().getOrCreate();
    System.setProperty("hadoop.home.dir","E:\\HADOOP");
    
    val sparkContext = spark.sparkContext;
    val sqlContext = spark.sqlContext;
    
    val stats = spark.sparkContext.textFile(path);
    
    val first = stats.first()
       //without header
    val woutHeader = stats.filter(_.!=(first));
  /* to generate case class ;) */  
//    stats.first().split(",").map(_.concat(":String;")).foreach(println);
    
    val statsDFRDD = woutHeader.map(_.split(",")).map(row=> stats_class(row(0),row(1),row(2),row(3),row(4),row(5),
        row(6),row(7),row(8),row(9),row(10),
        row(11),row(12),row(13),row(14),row(15),
        row(16),row(17),row(18),row(19),row(20),
        row(21),row(22),row(23),row(24),row(25)));
    
    import spark.implicits._;
    
    val statsDF = statsDFRDD.toDF();
  }
}