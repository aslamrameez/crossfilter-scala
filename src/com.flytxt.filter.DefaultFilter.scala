class DefaultFilter{


  def createRdd() = {
    val conf = new SparkConf().setAppName("Simple Application")
    new SparkContext(conf)
  }

  def filter( seq: Seq): Unit ={

  }

  def filter( seq: Seq, function1: Function1[Seq]): Unit ={

  }

}






