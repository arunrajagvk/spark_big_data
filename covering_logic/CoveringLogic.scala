import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

val someData = Seq(
  (1234,"key01",1,300.00,"source"),
  (1235,"key01",2,200.00,"source"),
  (1236,"key01",3,50.00,"source"),
  (1237,"key01",4,400.00,"source"),
  (1238,"key01",1,400.00,"use"),
  (1239,"key01",2,300.00,"use"),
  (1240,"key01",3,50.00,"use"),
  (1241,"key02",1,300.00,"source"),
  (1242,"key02",2,300.00,"source"),
  (1243,"key02",1,100.00,"use"),
  (1244,"key02",2,300.00,"use"),
  (1245,"key02",3,800.00,"use")
).toDF("fact_id","match_key","rank","amt","src_type")

someDF.show(10)

someData.createOrReplaceTempView("tbl")

val src_df = spark.sql("""select * from tbl where src_type='source'""")
src_df.createOrReplaceTempView("s_tbl")
val use_df = spark.sql("""select * from tbl where src_type='use'""")
use_df.createOrReplaceTempView("u_tbl")
val join_df_1 = spark.sql("""select * from s_tbl a full outer join u_tbl b on a.match_key = b.match_key and a.rank = b.rank order by a.match_key,a.rank """)
join_df_1.show()
val join_df_2 = spark.sql("""select a.*,b.*,a.amt-lag(b.amt) over(order by b.rank) as calc_amt from s_tbl a full outer join u_tbl b on a.match_key = b.match_key and a.rank = b.rank order by a.match_key,a.rank """)
join_df_2.show()
val join_df_3 = spark.sql("""select a.*,b.*,a.amt-b.amt as calc_amt from s_tbl a full outer join u_tbl b on a.match_key = b.match_key order by a.match_key,a.rank,b.rank """)
join_df_3.show()

/*largeDataFrame
   .join(smallDataFrame, Seq("some_identifier"),"left_anti")
   .show*/

val Schema = StructType(
  StructField("fact_id", IntegerType, true),
  StructField("match_key", StringType, true),
  StructField("rank", IntegerType, true),
  StructField("amt", DoubleType, true),
  StructField("src_type", StringType, true)
)

/*val someDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)*/
 
def calcCov1(row: Row):Map[String,ListBuffer[Row]]= {
  var li = new scala.collection.mutable.ListBuffer[Row]
  var mp = Map[String,ListBuffer[Row]]()
  val srcType = row.getAs[String]("src_type")
  val matchKey = row.getAs[String]("match_key")
  val rank = row.getAs[Int]("rank")
  val nwRow = Row(matchKey,srcType,rank)
  //mp += (matchKey -> ListBuffer(Row(matchKey,srcType,rank)))
  mp.get(matchKey) match {
  case Some(xs: ListBuffer[Row]) => mp(matchKey) = xs :+ Row(matchKey,srcType,rank)
  case None => mp(matchKey) = ListBuffer(Row(matchKey,srcType,rank))
  }
  mp
}

def calcCov2(row: Row):Map[String,ListBuffer[Row]]= {
  var li = new scala.collection.mutable.ListBuffer[Row]
  val srcType = row.getAs[String]("src_type")
  val matchKey = row.getAs[String]("match_key")
  val rank = row.getAs[Int]("rank")
  val nwRow = Row(matchKey,srcType,rank)

  var mapCov: collection.mutable.Map[String, ListBuffer[Row]] = collection.mutable.Map()

  def addTag(matchKey: String, tag: Row): Unit = mapCov.get(matchKey) match {
    case Some(xs: ListBuffer[Row]) => mapCov(matchKey) = xs :+ tag
    case None => mapCov(matchKey) = ListBuffer(tag)
  }
  addTag(matchKey,Row(matchKey,srcType,rank))  
  mapCov
}

def calcCov(row: Row):Map[String,Row]= {
  var li = new scala.collection.mutable.ListBuffer[Row]
  val srcType = row.getAs[String]("src_type")
  val matchKey = row.getAs[String]("match_key")
  val rank = row.getAs[Int]("rank")
  val fact_id = row.getAs[Int]("fact_id")
  val amt = row.getAs[Double]("amt")
  val nwRow = Row(matchKey,srcType,rank,fact_id,amt)

  var mapCov: collection.mutable.Map[String, Row] = collection.mutable.Map()

  mapCov += (matchKey ->  Row(matchKey,srcType,rank,fact_id,amt))
  mapCov
}


val newDFCal = someData.rdd.flatMap{ row => calcCov(row) }
val newgroup = newDFCal.groupByKey

newgroup.take(10).foreach(println)


val newretDF = newgroup.map{ case(k,v) => {
      val newf = Map[String,List[Row]]()
      newf += (k -> v.toList)
    }}


val newDFCalSchema = Array(MapType(StringType,StructType(
  StructField("match_key", StringType, true) ::
  StructField("src_type", StringType, true) ::
  StructField("rank", IntegerType, true) :: Nil
)))


val someDF = spark.createDataFrame(newretDF,newDFCalSchema)



val newretDF = newgroup.map{ case(k,v) => {
      var srcbuffer = new ListBuffer[Row]
      var usebuffer = new ListBuffer[Row]
      var count = 1
      var sum = 0.0
      val src1= v.filter(x => x(1) == "source")
      val use1= v.filter(x => x(1) == "use")
      var srcCnt = 1
      var useCnt = 1
      var srcCurrentRow = src1.filter(x => x(2) == srcCnt).toList.head
      var useCurrentRow = use1.filter(x => x(2) == useCnt).toList.head
      println(srcCurrentRow)
      println(useCurrentRow)
      var amt_src = srcCurrentRow(4).toString.toDouble
      var amt_use = useCurrentRow(4).toString.toDouble

      var amt_residue_src = amt_src - amt_use
      var amt_residue_use = amt_use - amt_src
      var 

    }
  }


import org.apache.spark.HashPartitioner
rdd.map(r => r(6)).take(5).foreach(println)
val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
keyedRDD.partitionBy(new HashPartitioner(10)).take(10)

    val variance = input2.groupByKey().map{case(k, v) => {
      var buffer = new ListBuffer[Double]

      // First pass: calculate mean
      var count = 0
      var sum = 0.0
      v.foreach(w => {
        buffer.+=(w)
        count += 1
        sum += w
      })
      var mean = sum / count

      // Second pass: calculate variance
      var error = 0.0
      v.foreach(w => {
        error += Math.pow(Math.abs(w - mean), 2)
      })
      (k, error)
}}

val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("addresses", ArrayType(new StructType()
      .add("city",StringType)
      .add("state",StringType)))
    .add("properties", mapType)
    .add("secondProp", MapType(StringType,StringType))

