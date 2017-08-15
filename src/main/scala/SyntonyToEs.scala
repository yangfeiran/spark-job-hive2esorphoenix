/** * * Created by yangchunyong on 1/2/17.
  */


import java.io.{File, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.elasticsearch.spark._

/**
  * Created by yangchunyong on 11/21/16.
  */

object SyntonyToEs{
  val spark = SparkSession.builder().master("spark://master1:9099").appName("SyntonyToEs").enableHiveSupport().getOrCreate()
  import spark.implicits._


  private case class Params(
                             db: String = "chinacloud",
                             beginday:String="2016-11-12",
                             day:String = "",
                             top:Int=5,
                             tableprefix: String = "hive_partition_all_kkxx",
                             savedconf:String="/usr/",
                             filtercondition:String="",
                             entrysize:String="1",
                             resource:String="",
                             dataname:String="car",
                             update:Boolean=false
                           )

  case class cc(date:Long,plate:String,data:String)

  /**
    * 覆盖式插入,存在checkconfig在插入完了才写入,如果不存在插入过的记录,但是有可能插入了一部分,所以需要覆盖式插入
    */
  private def saveToEs(): Unit ={

  }

  private def getSyntonyDayData(day:String,topNum:Int): Unit ={

  }

  private def getSyntonyDayData(date:DateTime,tablepreifx:String,topNum:Int,filterCondition:String,dataname:String):Dataset[cc]={
    val cday=DateTimeFormat.forPattern("yyyy_MM_dd").print(date);
    val data=spark.sql(s"select id1,id2,relation,times from (select id1,id2,relation,times,dense_rank() over (partition by id1 order by relation desc) as rank from ${tablepreifx}_$cday $filterCondition) tmp where rank<=$topNum");
    val daytoes=DateTimeFormat.forPattern("yyyyMMdd").print(date).toLong;

    val formdata=data.map(row=>{
      cc(daytoes,row.getAs[String](0),s"""{"${dataname}":"${row.getAs[String](1)}","relation":"${row.getAs[Double](2)}","times":"${row.getAs[Double](3)}"}""")
    })
    formdata
  }

  private def run(params: Params): Unit ={
    spark.sql(s"use ${params.db}");
    var day="";
    if(params.day==""){
      day=DateTimeFormat.forPattern("yyyy-MM-dd").print(new DateTime().minusDays(1))
    }else{
      day=params.day
    }

    val begindate=DateTime.parse(params.beginday, DateTimeFormat.forPattern("yyyy-MM-dd"))
    val days=Days.daysBetween(begindate,DateTime.parse(day, DateTimeFormat.forPattern("yyyy-MM-dd"))).getDays+1

    val pw = new PrintWriter(new File("sparkTOesLog.txt" ))

    for(dayic<- 0 until days){
      val cdate=begindate.plusDays(dayic)
      val data=getSyntonyDayData(cdate,params.tableprefix,params.top,params.filtercondition,params.dataname)
      data.rdd.saveToEs(params.resource,Map("es.batch.size.entries" -> params.entrysize));
      println(s"saved ${cdate.toString("yyyy--MM--dd")}")
      pw.write(s"saved ${cdate.toString("yyyy--MM--dd")}")
    }
    pw.close()

  }

  private def checkInsertStatus(): Unit ={

  }

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("数据共振") {
      head("输入数据")

      opt[String]("db")
        .text(s"hive数据库名." +
          s"  default: ${defaultParams.db}")
        .action((x, c) => c.copy(db= x))
      opt[String]("beginday")
        .text(s"开始日期." +
          s"  default: ${defaultParams.beginday}")
        .action((x, c) => c.copy(beginday= x))

      opt[String]("day")
        .text(s"结束日期." +
          s"  default: ${defaultParams.day}")
        .action((x, c) => c.copy(day= x))


      opt[Int]("top")
        .text(s"hive数据库名." +
          s"  default: ${defaultParams.top}")
        .action((x, c) => c.copy(top= x))

      opt[String]("tableprefix")
        .text(s"不包含日期的表前缀" +
          s"  default: ${defaultParams.tableprefix}")
        .action((x, c) => c.copy(tableprefix= x))

      opt[String]("resource")
        .text(s"ES的index/type" +
          s"  default: ${defaultParams.resource}")
        .action((x, c) => c.copy(resource= x))

      opt[String]("filtercondition")
        .text(s"抽取的where过滤条件" +
          s"  default: ${defaultParams.filtercondition}")
        .action((x, c) => c.copy(filtercondition= x))

      opt[String]("dataname")
        .text(s"json中的实体名" +
          s"  default: ${defaultParams.dataname}")
        .action((x, c) => c.copy(dataname= x))

      opt[String]("entrysize")
        .text(s"entrysize" +
          s"  default: ${defaultParams.entrysize}")
        .action((x, c) => c.copy(entrysize= x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }
}

