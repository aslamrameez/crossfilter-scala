import java.time.temporal.IsoFields
import java.time.temporal.IsoFields.Unit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
/**
  * Created by root on 9/2/17.
  */
class BaseFilter {


  def filter(array : Seq[Any], min: Int, max: Int): Array[Int] ={
    val rdd=SparkInteractor.makeIntRDD(array)
    val filterOut= rdd.filter ((number: Int) =>
      number >= min && number < max
    )
    filterOut.collect()
  }

  def sparkRDD(spark: SparkSession, uri: String): RDD[Row] ={
    val stringRDD=spark.sparkContext.textFile(uri)
    createRowRDD(stringRDD)
  }



  def sparkRDD(spark: SparkSession, seq: Seq[String]): RDD[Row]={
    val stringRDD=spark.sparkContext.makeRDD(seq)
    createRowRDD(stringRDD)
  }

  private def createRowRDD(stringRDD: RDD[String]) = {
    stringRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
  }

  def createSchema(schemaString: String): StructType = {
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
     StructType(fields)
  }

  def createDataFrame(spark: SparkSession,rowRDD: RDD[Row],schema :StructType, name: String): DataFrame ={
    val dataframe=spark.createDataFrame(rowRDD, schema)
    dataframe.createOrReplaceTempView(name)
    dataframe
  }

  def createRDDView(spark: SparkSession, rowRDD: RDD[Row], schema: StructType, table: String): Unit={
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")
  }

  def runSparkQuery(spark: SparkSession,query: String):Unit={
    val results = spark.sql("SELECT name FROM people")
    results.toJSON.collect().foreach(println)
  }

  private def runProgrammaticSchema(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = sparkRDD(spark,"examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema

    val schema = createSchema(schemaString)

    // Convert records of the RDD (people) to Rows

    // Apply the schema to the RDD
    val peopleDF = createDataFrame(spark,peopleRDD, schema, "tablename")

    // Creates a temporary view using the DataFrame



    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    results.toJSON.collect().foreach(println)
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }


  }
}
