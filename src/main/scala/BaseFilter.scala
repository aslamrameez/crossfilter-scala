import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 9/2/17.
  */
class BaseFilter {

  def createRDD(name :String): SparkContext = {
    val conf = new SparkConf().setAppName(name)
    new SparkContext(conf)
  }

  def filter(array : Array[Int], range: Range): String ={
    "Not implemented"
  }

}
