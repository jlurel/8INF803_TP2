package exercice1

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.WrappedArray

case class Monster(_id: ObjectId, name: String, spells: Array[String])

object Monster extends App {

  /* Create the SparkSession.
   * If config arguments are passed from the command line using --conf,
   * parse args for the values to set.
   */
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/uqac.monsters")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/uqac.monstersBySpell")
    .getOrCreate()

  val sc = spark.sparkContext

  val dataframe = MongoSpark.load(sc).toDF[Monster]

  val monstersBySpell = dataframe.select("name", "spells").rdd.flatMap(row =>
    row.get(1).asInstanceOf[WrappedArray[String]].map(
      spell => (spell, row.get(0).asInstanceOf[String])
    )
  ).reduceByKey((a, b) => a + " - " + b)

//    MongoSpark.save(spark.createDataFrame(monstersBySpell).toDF())
  monstersBySpell.coalesce(1).saveAsTextFile(System.getProperty("user.dir") + "/exercice1/results")


}
