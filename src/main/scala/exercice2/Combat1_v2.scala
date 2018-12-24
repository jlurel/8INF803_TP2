package exercice2

import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import breeze.numerics.{pow, sqrt}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}

import scala.util.Random


case class Point(var x: Int, var y: Int) {
  val random: Random.type = scala.util.Random
  if (x == 0 && y == 0) {
    x = random.nextInt(100)
    y = random.nextInt(100)
  }

  override def toString: String = s"($x,$y)"

  //Se rapproche du target point
  def move(point: Point, speed: Int): Unit = {
    val deltaX = point.x - x
    val deltaY = point.y - y
    val angle = Math.atan2(deltaY, deltaX)


    x += (speed * Math.cos( angle )).toInt
    y += (speed * Math.sin( angle )).toInt
  }

  //Calcule la distance avec le target point
  def distance(point: Point): Double = {
    sqrt(pow(x - point.x, 2) + pow(y - point.y, 2))
  }
}

//meleeDamage : 1d8 + 2 => List(1, 8, 2)
case class Monster(id: Int, name: String, var color: Long, var position: Point, var alive: Boolean = true,
              armor: Int, var hp: Int, regeneration: Int, melee: List[Int], meleeDamage: List[Int],
              ranged: List[Int], rangedDamage: List[Int], speed: Int, var target: Boolean) extends Serializable {
  val random: Random.type = scala.util.Random
  val hpMax: Int = hp

  override def toString: String = s"id : $id, name : $name, color : $color, hp : $hp, alive : $alive, position : $position"

  //Attaque de mêlée
  def melee(m: Monster): Int = {
    var attackCount = 0
    var damage = 0
    var totalHp = m.hp
    println()
    while (alive && m.alive && attackCount < melee.length) {
      println("%s (%d) vs %s (%d)".format(name, id, m.name, m.id))
      println("-------------------------------------------------")
      println(name + " - Melee attack nº " + (attackCount + 1))
      if (random.nextInt(19) + 1 + melee(attackCount) >= m.armor) {
        damage = meleeDamage.head * (random.nextInt(meleeDamage(1) - 1) + 1) + meleeDamage(2)
        if (damage > totalHp) {
          println(name +  " damaged " + m.name + " : -" + damage)
          totalHp = 0
          m.color = 0
          m.alive = false
          println(name + " killed " + m.name)
        } else {
          println(name +  " damaged " + m.name + " : -" + damage)
          totalHp = totalHp - damage
          println(m.name + "'s HP : " + totalHp )
        }
      } else {
        println(name +  " missed " + m.name)
      }
      attackCount += 1
    }
    damage
  }

  //Attaque à distance
  def ranged(m:Monster): Int = {
    var attackCount = 0
    var damage = 0
    var totalHp = m.hp
    println()
    while (alive && m.alive && attackCount < ranged.length) {
      println("%s (%d) vs %s (%d)".format(name, id, m.name, m.id))
      println("--------------------------------------------------")
      println("Ranged attack nº " + (attackCount + 1) + " - " + name + " vs " + m.name)
      if (random.nextInt(19) + 1 + ranged(attackCount) >= m.armor) {
        damage = rangedDamage.head * (random.nextInt(rangedDamage(1) - 1) + 1) + rangedDamage(2)
        if (damage > totalHp) {
          println(name +  " damaged " + m.name + " : -" + damage)
          totalHp = 0
          m.color = 0
          m.alive = false
          println(name + " killed " + m.name)
        } else {
          println(name +  " damaged " + m.name + " : -" + damage)
          totalHp = totalHp - damage
          println(m.name + "'s HP : " + totalHp )
        }
      } else {
        println(name +  " missed " + m.name)
      }
      attackCount += 1
    }
    damage
  }

  //Regeneration
  def regen(): Unit = {
    if (regeneration != 0) {
      hp += regeneration
      if (hp > hpMax) {
        hp = hpMax
      }
      println("%s regenerated %d HP, new HP : %d HP".format(name, regeneration, hp))
    }
  }

}

class Fight extends Serializable {
  //Envoie la distance avec l'autre monstre
  def sendDistance(context: EdgeContext[Monster, String, Array[Int]]): Unit = {
    val distance = context.srcAttr.position.distance(context.dstAttr.position).toInt
    if (context.srcAttr.alive) {
      val a = Array(context.srcAttr.id,distance)
      context.sendToDst(a)
    }
  }

  //Choisit l'action à effectuer pendant le tour
  def action(context: EdgeContext[Monster, String, (Point, Long)]): Unit ={
    val distance = context.srcAttr.position.distance(context.dstAttr.position).toInt
    var damage = 0
      if (context.srcAttr.alive) {
        if (context.srcAttr.name != "Solar") {
          if (distance <= 10 ) {
            damage = context.srcAttr.melee(context.dstAttr)
          } else {
            val speed = context.srcAttr.speed
            val target = context.dstAttr.position
            context.srcAttr.position.move(target, speed)
            println("%s (%d) moved".format(context.srcAttr.name, context.srcAttr.id))
          }
        } else {
          if (distance <= 10) {
            damage = context.srcAttr.melee(context.dstAttr)
          } else if (distance <= 110) {
            damage = context.srcAttr.ranged(context.dstAttr)
          }
        }
      }
    var tuple: (Point, Long)  = (context.srcAttr.position, 0)
    context.sendToSrc(tuple)

    tuple = (context.dstAttr.position, damage)
    context.sendToDst(tuple)
  }

  //Choisit la distance la plus petite
  def selectBest(id1: Array[Int], id2: Array[Int]): Array[Int] = {
    if (id1(1) < id2(1)) id1
    else id2
  }

  //Cumule les dégats en cas d'attaques multiples sur une target
  def sumDamage(id1: (Point, Long), id2: (Point, Long)): (Point, Long) = {
    (id1._1, id1._2 + id2._2)
  }

  //Afflige les dégats
  def hitAndKill(vid: VertexId, monster: Monster, data: (Point, Long)): Monster = {
    if (data._2 >= monster.hp) {
      return new Monster(monster.id, monster.name, 0, monster.position, false, monster.armor,
        0, monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, monster.target)
    }
    else {
      return new Monster(monster.id, monster.name, monster.color, data._1, true, monster.armor,
        (monster.hp - data._2).toInt, monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, monster.target)
    }
  }

  def selectTarget(vid: VertexId, monster: Monster, target: Array[Int]): Monster ={
    if (monster.id == target(0)){
      return new Monster(monster.id, monster.name, monster.color, monster.position, monster.alive, monster.armor,monster.hp,
        monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, true)
    }
    else {
      return new Monster(monster.id, monster.name, monster.color, monster.position, monster.alive, monster.armor,monster.hp,
        monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, false)
    }
  }


  def execute(g: Graph[Monster, String], maxIterations: Int, sc: SparkContext): Graph[Monster, String] = {
    var myGraph = g
    var counter = 0
    val fields = new TripletFields(true, true, false) //join strategy
    val directory = new File(System.getProperty("user.dir") + "/exercice2/")

    FileUtils.cleanDirectory(directory)

    def loop1: Unit = {
      while (true) {
        println("------------------")
        println("ROUND nº : " + (counter + 1))
        println("------------------")
        println()
        counter += 1

        //Enregistre le graphe dans un fichier GEXF pour la visualisation avec Gephi
        val pw = new PrintWriter(directory + "/round"+ counter + ".gexf")
        pw.write(toGexf(myGraph))
        pw.close()


        if (counter == maxIterations) return

        val messages1 = myGraph.aggregateMessages[Array[Int]](
          sendDistance,
          selectBest,
          fields
        )

        myGraph = myGraph.joinVertices(messages1)(
          (vid, monster, target) => selectTarget(vid, monster, target))


        val messages2 = myGraph.aggregateMessages[(Point, Long)](
          action,
          sumDamage,
          fields
        )

        println("**********************")
        println("Alive monsters count : " + myGraph.vertices.filter {case (id, monster) => monster.alive}.count())
        println("**********************")

        //Conditions d'arrêt
        if (myGraph.vertices.filter {case (id, monster) => monster.alive }.count() == 1) {
          println("Fight ended, Solar wins")
          return
        }

        myGraph = myGraph.joinVertices(messages2)(
          (vid, monster, damage) => hitAndKill(vid, monster, damage))

        myGraph.vertices.collect().foreach(
          monster => monster._2.regen()
        )
      }
    }

    loop1
    myGraph
  }

  def toGexf[VD, ED](g: Graph[VD, ED]): String = {
    val header =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2"
        | xmlns:viz="http://www.gexf.net/1.2draft/viz" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        |  xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd">
        |  <meta>
        |    <description>A gephi graph in GEXF format</description>
        |  </meta>
        |    <graph mode="static" defaultedgetype="directed">
        |    <attributes class="node" mode="static">
        |      <attribute id="lat" title="Latitude" type="double"></attribute>
        |      <attribute id="long" title="Longitude" type="double"></attribute>
        |    </attributes>
      """.stripMargin

    val vertices = "<nodes>\n" + g.vertices.map(
      v =>
        if (v._2.asInstanceOf[Monster].color == 1) {
          s"""<node id=\"${v._1}\" label=\"${v._2.asInstanceOf[Monster].name}\">
             |<attvalues>
             | <attvalue for=\"lat\" value=\"${v._2.asInstanceOf[Monster].position.x}\"></attvalue>
             | <attvalue for=\"long\" value=\"${v._2.asInstanceOf[Monster].position.y}\"></attvalue>
             |</attvalues>
             |<viz:position x=\"${v._2.asInstanceOf[Monster].position.x}\" y=\"${v._2.asInstanceOf[Monster].position.y}\"></viz:position>
             |<viz:color r=\"255\" g=\"204\" b=\"0\"></viz:color>
             |</node>
         """.stripMargin
        } else if (v._2.asInstanceOf[Monster].color == 2) {
          s"""<node id=\"${v._1}\" label=\"${v._2.asInstanceOf[Monster].name}\" lat=\"${v._2.asInstanceOf[Monster].position.x}\" long=\"${v._2.asInstanceOf[Monster].position.y}\">
             |<attvalues>
             | <attvalue for=\"lat\" value=\"${v._2.asInstanceOf[Monster].position.x}\"></attvalue>
             | <attvalue for=\"long\" value=\"${v._2.asInstanceOf[Monster].position.y}\"></attvalue>
             |</attvalues>
             |<viz:position x=\"${v._2.asInstanceOf[Monster].position.x}\" y=\"${v._2.asInstanceOf[Monster].position.y}\"></viz:position>
             |<viz:color r=\"0\" g=\"0\" b=\"0\"></viz:color>
             |</node>
         """.stripMargin
        } else if (v._2.asInstanceOf[Monster].color == 3) {
          s"""<node id=\"${v._1}\" label=\"${v._2.asInstanceOf[Monster].name}\" lat=\"${v._2.asInstanceOf[Monster].position.x}\" long=\"${v._2.asInstanceOf[Monster].position.y}\">
             |<attvalues>
             | <attvalue for=\"lat\" value=\"${v._2.asInstanceOf[Monster].position.x}\"></attvalue>
             | <attvalue for=\"long\" value=\"${v._2.asInstanceOf[Monster].position.y}\"></attvalue>
             |</attvalues>
             |<viz:position x=\"${v._2.asInstanceOf[Monster].position.x}\" y=\"${v._2.asInstanceOf[Monster].position.y}\"></viz:position>
             |<viz:color r=\"0\" g=\"0\" b=\"255\"></viz:color>
             |</node>
         """.stripMargin
        } else if (v._2.asInstanceOf[Monster].color == 4) {
          s"""<node id=\"${v._1}\" label=\"${v._2.asInstanceOf[Monster].name}\" lat=\"${v._2.asInstanceOf[Monster].position.x}\" long=\"${v._2.asInstanceOf[Monster].position.y}\">
             |<attvalues>
             | <attvalue for=\"lat\" value=\"${v._2.asInstanceOf[Monster].position.x}\"></attvalue>
             | <attvalue for=\"long\" value=\"${v._2.asInstanceOf[Monster].position.y}\"></attvalue>
             |</attvalues>
             |<viz:position x=\"${v._2.asInstanceOf[Monster].position.x}\" y=\"${v._2.asInstanceOf[Monster].position.y}\"></viz:position>
             |<viz:color r=\"255\" g=\"0\" b=\"0\"></viz:color>
             |</node>
         """.stripMargin
        } else {
          s"""<node id=\"${v._1}\" label=\"${v._2.asInstanceOf[Monster].name}\" lat=\"${v._2.asInstanceOf[Monster].position.x}\" long=\"${v._2.asInstanceOf[Monster].position.y}\">
             |<attvalues>
             | <attvalue for=\"lat\" value=\"${v._2.asInstanceOf[Monster].position.x}\"></attvalue>
             | <attvalue for=\"long\" value=\"${v._2.asInstanceOf[Monster].position.y}\"></attvalue>
             |</attvalues>
             |<viz:position x=\"${v._2.asInstanceOf[Monster].position.x}\" y=\"${v._2.asInstanceOf[Monster].position.y}\"></viz:position>
             |<viz:color r=\"255\" g=\"255\" b=\"255\"></viz:color>
             |</node>
         """.stripMargin
        }

    ).collect.mkString + "</nodes>\n"

    val edges = "<edges>\n" + g.edges.map(
      e => s"""<edge source=\"${e.srcId}\" target=\"${e.dstId}\" label=\"${e.attr}\"/>\n"""
    ).collect.mkString + "</edges>\n"

    val footer = "</graph>\n</gexf>"

    header + vertices + edges + footer
  }

}

object Combat1 extends App {

  val conf = new SparkConf()
    .setAppName("Combat 1")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  // Create an RDD for the vertices (creatures)
  var monsters =
    sc.makeRDD(Array(
//      (1L, Monster(1, "Pito", 1, alive = true, Point(), 5, 5, 0, List(0), List(0, 0, 0)),
      (2L, new Monster(2, "Solar", 1, Point(0,0), true,44, 363, 15,
        List(35, 30 , 25, 20), List(3, 6, 18), List(31, 26, 21, 16), List(2, 6, 14), 50, true)),
      (3L, new Monster(3, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (4L, new Monster(4, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (5L, new Monster(5, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (6L, new Monster(6, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (7L, new Monster(7, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (8L, new Monster(8, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (9L, new Monster(9, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (10L, new Monster(10, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (11L, new Monster(11, "Worgs Rider", 2, Point(0,0), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (12L, new Monster(12, "Warlord", 3, Point(0,0), true, 27, 141, 0,
        List(20, 15, 10), List(1, 8, 10), List(19), List(1, 6, 5), 30, false)),
      (13L, new Monster(13, "Barbare Orc", 4, Point(0,0), true, 17, 141, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (14L, new Monster(14, "Barbare Orc", 4, Point(0,0), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (15L, new Monster(15, "Barbare Orc", 4, Point(0,0), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (16L, new Monster(16, "Barbare Orc", 4, Point(0,0), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false))
    ))

  // Create an RDD for edges (link between two creatures)
  var relationships =
    sc.makeRDD(Array(
//      Edge(1L, 2L, "ally"),
      Edge(3L, 2L, "enemy"), Edge(4L, 2L, "enemy"), Edge(5L, 2L, "enemy"), Edge(6L, 2L, "enemy"),
      Edge(7L, 2L, "enemy"), Edge(8L, 2L, "enemy"), Edge(9L, 2L, "enemy"), Edge(10L, 2L, "enemy"), Edge(11L, 2L, "enemy"),
      Edge(12L, 2L, "enemy"), Edge(13L, 2L, "enemy"), Edge(14L, 2L, "enemy"), Edge(15L, 2L, "enemy"), Edge(16L, 2L, "enemy"),

      Edge(2L, 3L, "enemy"), Edge(2L, 4L, "enemy"), Edge(2L, 5L, "enemy"), Edge(2L, 6L, "enemy"),
      Edge(2L, 7L, "enemy"), Edge(2L, 8L, "enemy"), Edge(2L, 9L, "enemy"), Edge(2L, 10L, "enemy"), Edge(2L, 11L, "enemy"),
      Edge(2L, 12L, "enemy"), Edge(2L, 13L, "enemy"), Edge(2L, 14L, "enemy"), Edge(2L, 15L, "enemy"), Edge(2L, 16L, "enemy")
    ))

  // Build the Graph
  val graph = Graph(monsters, relationships)
  val algoFight = new Fight()
  val res = algoFight.execute(graph, 200, sc)
}
