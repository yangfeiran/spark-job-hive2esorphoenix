import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
/**
  * Created by Administrator on 2017/4/18.
  */
object Test {


  def saveToPhoenix() {
    val sc = new SparkContext("local", "phoenix-test")
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.load(
      "org.apache.phoenix.spark",
      Map("table" -> "YANGFEIRAN_2", "zkUrl" -> "172.16.80.70:2181")
    )

    df
      .filter(df("cf:O_ORDERDATE") >= "1996-01-02 00:00:00.0" )
      .orderBy("cf:O_ORDERDATE")
      .show
    df.toDF()
   df.saveToPhoenix("YANGFEIRAN_2",zkUrl = Some( "172.16.80.70:2181"))
  }

  def main(args: Array[String]):Unit = {
    saveToPhoenix()
  }
}
