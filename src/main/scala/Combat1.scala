import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Combat1 extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/UQAC.monsters")
    .getOrCreate()

  val sc = spark.sparkContext

  // Create an RDD for the vertices
  val users: RDD[(VertexId, String)] =
    sc.parallelize(Array((1L, "Pito"), (2L, "Solar"), (3L, "Worgs Rider"),(4L, "Worgs Rider"),
      (5L, "Worgs Rider"), (6L, "Worgs Rider"), (7L, "Worgs Rider"), (8L, "Worgs Rider"), (9L, "Worgs Rider"),
      (10L, "Worgs Rider"), (11L, "Worgs Rider"), (12L, "Le Warlord"), (13L, "Barbares Orc"), (14L, "Barbares Orc"),
      (15L, "Barbares Orc"), (16L, "Barbares Orc")))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(1L, 2L, "ally"),Edge(3L, 2L, "enemy"), Edge(4L, 2L, "enemy"), Edge(5L, 2L, "enemy"), Edge(6L, 2L, "enemy"),
      Edge(7L, 2L, "enemy"), Edge(8L, 2L, "enemy"), Edge(9L, 2L, "enemy"), Edge(10L, 2L, "enemy"), Edge(11L, 2L, "enemy"),
      Edge(12L, 2L, "enemy"), Edge(13L, 2L, "enemy"), Edge(14L, 2L, "enemy"), Edge(15L, 2L, "enemy"), Edge(16L, 2L, "enemy"),
      Edge(3L, 1L, "enemy"), Edge(4L, 1L, "enemy"), Edge(5L, 1L, "enemy"), Edge(6L, 1L, "enemy"),
      Edge(7L, 1L, "enemy"), Edge(8L, 1L, "enemy"), Edge(9L, 1L, "enemy"), Edge(10L, 1L, "enemy"), Edge(11L, 1L, "enemy"),
      Edge(12L, 1L, "enemy"), Edge(13L, 1L, "enemy"), Edge(14L, 1L, "enemy"), Edge(15L, 1L, "enemy"), Edge(16L, 1L, "enemy"),

      Edge(2L, 3L, "enemy"), Edge(2L, 4L, "enemy"), Edge(2L, 5L, "enemy"), Edge(2L, 6L, "enemy"),
      Edge(2L, 7L, "enemy"), Edge(2L, 8L, "enemy"), Edge(2L, 9L, "enemy"), Edge(2L, 10L, "enemy"), Edge(2L, 11L, "enemy"),
      Edge(2L, 12L, "enemy"), Edge(2L, 13L, "enemy"), Edge(2L, 14L, "enemy"), Edge(2L, 15L, "enemy"), Edge(2L, 16L, "enemy"),
      Edge(1L, 3L, "enemy"), Edge(1L, 4L, "enemy"), Edge(1L, 5L, "enemy"), Edge(1L, 6L, "enemy"),
      Edge(1L, 7L, "enemy"), Edge(1L, 8L, "enemy"), Edge(1L, 9L, "enemy"), Edge(1L, 10L, "enemy"), Edge(1L, 11L, "enemy"),
      Edge(1L, 12L, "enemy"), Edge(1L, 13L, "enemy"), Edge(1L, 14L, "enemy"), Edge(1L, 15L, "enemy"), Edge(1L, 16L, "enemy")
    ))
  // Define a default user in case there are relationship with missing user
  val defaultUser = ("Missing")
  // Build the initial Graph
  val graph = Graph(users, relationships, defaultUser)

  println(graph.vertices.filter { case (id, name) => name == "Worgs Rider" }.count)
}
