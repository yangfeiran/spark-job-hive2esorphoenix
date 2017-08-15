/** * * Created by yangchunyong on 1/2/17.
  */


import java.io.{File, PrintWriter}

import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.types.DataType
import org.apache.htrace.SpanReceiver
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.apache.phoenix.util._
import org.apache.tephra.TransactionSystemClient
import org.apache.twill.zookeeper.ZKClient


/**
  * Created by yangchunyong on 11/21/16.
  */

object SyntonyToPhoenix{
  val spark = SparkSession.builder().master("spark://master1:9099").appName("SyntonyToEs").enableHiveSupport().getOrCreate()
  import spark.implicits._


  private case class Params(
                             db: String = "chinacloud",
                             beginday:String="2016-11-12",
                             day:String = "",
                             top:Int=5,
                             tableprefix: String = "hive_table",
                             table:String = "TABLE2",
                             zkUrl: String = "172.16.80.70:2181",
                             savedconf:String="/usr/",
                             filtercondition:String="",
                             entrysize:String="1",
                             resource:String="",
                             dataname:String="car",
                             update:Boolean=false
                           )

  case class cc(date:Long,plate:String,data:String)
  case class data_frame_to_phoenix(plate_date:String,data:String)

  /**
    * 覆盖式插入,存在checkconfig在插入完了才写入,如果不存在插入过的记录,但是有可能插入了一部分,所以需要覆盖式插入
    */
  private def saveToPhoenix(params: Params,df:Dataset[cc]): Unit ={
    val table_name = params.table
    val zk_url = params.zkUrl
    val new_df = df.map(c=> {data_frame_to_phoenix(c.plate + c.date.toString(), c.data)})
    new_df.toDF().saveToPhoenix(table_name,zkUrl = Some(zk_url))
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
      saveToPhoenix(params,data);
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

      opt[String]("table")
        .text(s"table" +
          s"  default: ${defaultParams.table}")
        .action((x, c) => c.copy(table= x))
      opt[String]("zkUrl")
        .text(s"zkUrl" +
          s"  default: ${defaultParams.zkUrl}")
        .action((x, c) => c.copy(zkUrl= x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }
}

