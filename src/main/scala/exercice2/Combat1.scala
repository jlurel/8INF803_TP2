package exercice2

import breeze.numerics.sqrt
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random


case class Point() {
  val random: Random.type = scala.util.Random
  var x: Int = random.nextInt(50)
  var y: Int = random.nextInt(50)

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
  }

  def dist(p: Point): Unit = {
    sqrt( (p.x-x)^2+(p.y-y)^2 )
  }
}

case class Monster(id: Int, name: String, color: Int, alive: Boolean, position: Point,
                   armor: Int, hp: Int, regeneration: Int) extends Serializable

object Combat1 extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Combat1")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/UQAC.monsters")
    .getOrCreate()

  val sc = spark.sparkContext

  // Create an RDD for the vertices (creatures)
  val monsters: RDD[(VertexId, Monster)] =
    sc.parallelize(Array(
      (1L, Monster(1, "Pito", 1, alive = true, Point(), 5, 5, 0)),
      (2L, Monster(2, "Solar", 1, alive = true, Point(), 44, 363, 15)),
      (3L, Monster(3, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (4L, Monster(4, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (5L, Monster(5, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (6L, Monster(6, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (7L, Monster(7, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (8L, Monster(8, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (9L, Monster(9, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (10L, Monster(10, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (11L, Monster(11, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0)),
      (12L, Monster(12, "Warlord", 3, alive = true, Point(), 27, 141, 0)),
      (13L, Monster(13, "Barbare Orc", 4, alive = true, Point(), 17, 141, 0)),
      (14L, Monster(14, "Barbare Orc", 4, alive = true, Point(), 17, 142, 0)),
      (15L, Monster(15, "Barbare Orc", 4, alive = true, Point(), 17, 142, 0)),
      (16L, Monster(16, "Barbare Orc", 4, alive = true, Point(), 17, 142, 0))
    ))

  // Create an RDD for edges (link between two creatures)
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

  // Build the Graph
  val graph = Graph(monsters, relationships)
  println(graph.vertices.count)

  val point = Point()
  point.move(3, 3)
  printPoint()

  def printPoint(){
    println ("Point x location : " + point.x)
    println ("Point y location : " + point.y)
  }
}
