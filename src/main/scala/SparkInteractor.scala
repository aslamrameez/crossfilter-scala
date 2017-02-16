import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 9/2/17.
  */
object SparkInteractor {

  val conf = new SparkConf().setAppName("crossfilter")
  val context=new SparkContext(conf)

  def makeRDD(array :Seq[Any]): RDD[Any] = {
    context.makeRDD(array)
  }

  def makeIntRDD(array :Seq[Any]): RDD[Int] ={
    makeRDD(array).map( (f: Any) => f match {
      case Int => true
      case _ => false
    } ).map(_.asInstanceOf[Int])
  }
}
