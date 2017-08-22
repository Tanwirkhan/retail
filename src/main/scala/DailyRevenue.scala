/**
  * Created by Tanwir on 14-06-2017.
  */
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DailyRevenue {

  def main(args:Array[String]): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("Total Revenue per day")
    val sc = new SparkContext(conf)
    val inputBaseDir = args(0)
    val outputpath = args(1)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //again sc has a API called hadoopConfiguration. If you run this
    // then fs is of type file system.
    //and we need to do FileSystem.get and pass the configuration to it(sc.hadoopConfiguration).


    //Now if I want to use exists, I need to pass object of type path but our inputBaseDir and outputpath
    //is of type string. So we to convert the string into path object and then we have to use fs.exists to
    //compare if the path exists or not and then we have to validate. It returns boolean so we can use !exists
    //and then raise some exception.

    if(!fs.exists(new Path(inputBaseDir))) {
      println("base directory doesnt exist")
      return
    }

    //Similarly if output directory already exists , for this purpose i just want to delete.

    val op = new Path(outputpath )
    if(fs.exists(op)){
      fs.delete(op,true)
    }
//so if the output path exists then it will just delete and it will try to create new directory to copy the data.

    val orders = sc.textFile(inputBaseDir + "/orders")
    val orderFiltered = orders.filter(rec=> rec.split(",")(3)=="COMPLETE"||rec.split("")(3)=="CLOSED")
    val ordersMap = orderFiltered.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
    val order_item = sc.textFile(inputBaseDir + "/order_items")
    val order_itemMap = order_item.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toDouble))
    val ordersjoin = ordersMap.join(order_itemMap)
    val ordersJoinMap = ordersjoin.map(rec => rec._2)
    val ordersJoinMapRBK = ordersJoinMap.reduceByKey((agg, value) => agg+value)
   // val orders1 =  ordersJoinMapRBK.map(rec=> rec._1 +"\t" + rec._2)
     //   orders1.saveAsTextFile(outputpath)
   ordersJoinMapRBK.map(rec => rec._1 +"\t" + rec._2).saveAsTextFile(outputpath)
    //ordersJoinMapRBK.map(rec=>rec.1.saveAsTextFile(outputpath)
  }

}
