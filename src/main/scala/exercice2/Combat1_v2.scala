package exercice2

import breeze.numerics.{sqrt, pow}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}
import scala.util.Random


case class Point() {
  val random: Random.type = scala.util.Random
  var x: Int = random.nextInt(500)
  var y: Int = random.nextInt(500)

  def move(point: Point, speed: Int) {
    val deltaX = point.x - x
    val deltaY = point.y - y
    val angle = Math.atan2(deltaY, deltaX)


    x += (speed * Math.cos( angle )).toInt
    y += (speed * Math.sin( angle )).toInt
  }

  def dist(point: Point): Double = {
    sqrt(pow(x - point.x, 2) + pow(y - point.y, 2))
  }
}

//meleeDamage : 1d8 + 2 => List(1, 8, 2)
case class Monster(val id: Int, val name: String, var color: Long, var position: Point, var alive: Boolean = true,
              val armor: Int, var hp: Int, val regeneration: Int, val melee: List[Int], val meleeDamage: List[Int],
              val ranged: List[Int], val rangedDamage: List[Int], val speed: Int, val target: Boolean) extends Serializable {
  val random: Random.type = scala.util.Random
  val hpMax: Int = hp

  override def toString: String = s"id : $id, name : $name, color : $color, hp : $hp, alive : $alive"

  def melee(m: Monster): Int = {
    var attackCount = 0
    var damage = 0
    println()
    println("%s vs %s".format(name, m.name))
    while (alive && m.alive && attackCount < melee.length) {
      println("--------------------------------------------------")
      println(name + " - Melee attack nº " + (attackCount + 1))
      if (random.nextInt(19) + 1 + melee(attackCount) >= m.armor) {
        damage = meleeDamage.head * (random.nextInt(meleeDamage(1) - 1) + 1) + meleeDamage(2)
        if (damage > m.hp) {
          println(name +  " damaged " + m.name + " : -" + damage)
          m.hp = 0
          m.color = 0
          println(name + " killed " + m.name)
        } else {
          println(name +  " damaged " + m.name + " : -" + damage)
          m.hp = m.hp - damage
          println(m.name + "'s HP : " + m.hp )
        }
      } else {
        println(name +  " missed " + m.name)
      }
      attackCount += 1
    }
    damage
  }

  def ranged(m:Monster): Int = {
    var attackCount = 0
    var damage = 0
    println()
    println("%s vs %s".format(name, m.name))
    while (alive && m.alive && attackCount < ranged.length) {
      println("--------------------------------------------------")
      println("Ranged attack nº " + (attackCount + 1) + " - " + name + " vs " + m.name)
      if (random.nextInt(19) + 1 + ranged(attackCount) >= m.armor) {
        damage = rangedDamage.head * (random.nextInt(rangedDamage(1) - 1) + 1) + rangedDamage(2)
        if (damage > m.hp) {
          println(name +  " damaged " + m.name + " : -" + damage)
          m.hp = 0
          m.color = 0
          println(name + " killed " + m.name)
        } else {
          println(name +  " damaged " + m.name + " : -" + damage)
          m.hp = m.hp - damage
          println(m.name + "'s HP : " + m.hp )
        }
      } else {
        println(name +  " missed " + m.name)
      }
      attackCount += 1
    }
    damage
  }

  def regen(): Unit = {
    if (regeneration != 0) {
      hp += regeneration
      if (hp > hpMax) {
        hp = hpMax
      }
      println(name + " regenerated, new HP : " + hp)
    }
  }

}

class Fight extends Serializable {
  def sendDistance(context: EdgeContext[Monster, String, Array[Int]]): Unit = {
    val distance = context.srcAttr.position.dist(context.dstAttr.position).toInt
    if (context.srcAttr.alive) {
      val a = Array(context.srcAttr.id,distance)
      context.sendToDst(a)
    }
  }

  def action(context: EdgeContext[Monster, String, Long]): Unit ={
    val distance = context.srcAttr.position.dist(context.dstAttr.position).toInt
    var damage = 0

    if (distance <= 10) {
      damage = context.srcAttr.melee(context.dstAttr)
      //context.dstAttr.melee(context.srcAttr)
    } else if (distance <= 110) {
      damage = context.srcAttr.ranged(context.dstAttr)
      // context.dstAttr.ranged(context.srcAttr)
    }

    context.sendToDst(damage)

    if (distance > 5) {
      val speed = context.srcAttr.speed
      val target = context.dstAttr.position
      context.srcAttr.position.move(target, speed)
    }
  }

  def selectBest(id1: Array[Int], id2: Array[Int]): Array[Int] = {
    if (id1(1) < id2(1)) id1
    else id2
  }

  def sumDamage(id1: Long, id2: Long): Long = {
    id1 + id2
  }

  def deadColor(vid: VertexId, monster: Monster, totalDamage: Long): Monster = {
    if (totalDamage >= monster.hp) {
      return new Monster(monster.id, monster.name, monster.color, monster.position, false, monster.armor,
        0, monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, monster.target)
    }
    else {
      return new Monster(monster.id, monster.name, monster.color, monster.position, true, monster.armor,
        (monster.hp - totalDamage).toInt, monster.regeneration, monster.melee, monster.meleeDamage, monster.ranged, monster.rangedDamage, monster.speed, monster.target)
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

    def loop1: Unit = {
      while (true) {
        println("------------------")
        println("ROUND nº : " + (counter + 1))
        println("------------------")
        println()
        counter += 1
        if (counter == maxIterations) return

        val messages1 = myGraph.aggregateMessages[Array[Int]](
          sendDistance,
          selectBest,
          fields
        )

//        if (messages.isEmpty()) {
//          println("Empty messages")
//        }

        myGraph = myGraph.joinVertices(messages1)(
          (vid, monster, target) => selectTarget(vid, monster, target))


        val messages2 = myGraph.aggregateMessages[Long](
          action,
          sumDamage,
          fields
        )

        println("**********************")
        println("Alive monsters count : " + myGraph.vertices.filter {case (id, monster) => monster.alive}.count())
        println("**********************")

        if (myGraph.vertices.filter {case (id, monster) => monster.alive }.count() == 1) {
          println("Fight ended, Solar wins")
          return
        }

        myGraph = myGraph.joinVertices(messages2)(
          (vid, monster, damage) => deadColor(vid, monster, damage))

      }
    }

    loop1 //execute loop
    myGraph //return the result graph
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
      (2L, new Monster(2, "Solar", 1, Point(), true,44, 363, 15,
        List(35, 30 , 25, 20), List(3, 6, 18), List(31, 26, 21, 16), List(2, 6, 14), 50, false)),
      (3L, new Monster(3, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (4L, new Monster(4, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (5L, new Monster(5, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (6L, new Monster(6, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (7L, new Monster(7, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (8L, new Monster(8, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (9L, new Monster(9, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (10L, new Monster(10, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (11L, new Monster(11, "Worgs Rider", 2, Point(), true, 18, 13, 0,
        List(6), List(1, 8, 2), List(4), List(1, 6, 0), 20, false)),
      (12L, new Monster(12, "Warlord", 3, Point(), true, 27, 141, 0,
        List(20, 15, 10), List(1, 8, 10), List(19), List(1, 6, 5), 30, false)),
      (13L, new Monster(13, "Barbare Orc", 4, Point(), true, 17, 141, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (14L, new Monster(14, "Barbare Orc", 4, Point(), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (15L, new Monster(15, "Barbare Orc", 4, Point(), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false)),
      (16L, new Monster(16, "Barbare Orc", 4, Point(), true, 17, 142, 0,
        List(19, 14, 9), List(1, 8, 10), List(16, 11, 6), List(1, 8, 6), 40, false))
    ))

  // Create an RDD for edges (link between two creatures)
  var relationships =
    sc.makeRDD(Array(
//      Edge(1L, 2L, "ally"),
      Edge(3L, 2L, "enemy"), Edge(4L, 2L, "enemy"), Edge(5L, 2L, "enemy"), Edge(6L, 2L, "enemy"),
      Edge(7L, 2L, "enemy"), Edge(8L, 2L, "enemy"), Edge(9L, 2L, "enemy"), Edge(10L, 2L, "enemy"), Edge(11L, 2L, "enemy"),
      Edge(12L, 2L, "enemy"), Edge(13L, 2L, "enemy"), Edge(14L, 2L, "enemy"), Edge(15L, 2L, "enemy"), Edge(16L, 2L, "enemy"),
//      Edge(3L, 1L, "enemy"), Edge(4L, 1L, "enemy"), Edge(5L, 1L, "enemy"), Edge(6L, 1L, "enemy"),
//      Edge(7L, 1L, "enemy"), Edge(8L, 1L, "enemy"), Edge(9L, 1L, "enemy"), Edge(10L, 1L, "enemy"), Edge(11L, 1L, "enemy"),
//      Edge(12L, 1L, "enemy"), Edge(13L, 1L, "enemy"), Edge(14L, 1L, "enemy"), Edge(15L, 1L, "enemy"), Edge(16L, 1L, "enemy"),

      Edge(2L, 3L, "enemy"), Edge(2L, 4L, "enemy"), Edge(2L, 5L, "enemy"), Edge(2L, 6L, "enemy"),
      Edge(2L, 7L, "enemy"), Edge(2L, 8L, "enemy"), Edge(2L, 9L, "enemy"), Edge(2L, 10L, "enemy"), Edge(2L, 11L, "enemy"),
      Edge(2L, 12L, "enemy"), Edge(2L, 13L, "enemy"), Edge(2L, 14L, "enemy"), Edge(2L, 15L, "enemy"), Edge(2L, 16L, "enemy")
//      Edge(1L, 3L, "enemy"), Edge(1L, 4L, "enemy"), Edge(1L, 5L, "enemy"), Edge(1L, 6L, "enemy"),
//      Edge(1L, 7L, "enemy"), Edge(1L, 8L, "enemy"), Edge(1L, 9L, "enemy"), Edge(1L, 10L, "enemy"), Edge(1L, 11L, "enemy"),
//      Edge(1L, 12L, "enemy"), Edge(1L, 13L, "enemy"), Edge(1L, 14L, "enemy"), Edge(1L, 15L, "enemy"), Edge(1L, 16L, "enemy")
    ))

  // Build the Graph
  val graph = Graph(monsters, relationships)
  val algoFight = new Fight()
  val res = algoFight.execute(graph, 20, sc)

//  val monster1 = Monster(2, "Solar", 1, alive = true, Point(), 44, 363, 15,
//    List(35, 30 , 25, 20), List(3, 6, 18), List(31, 26, 21, 16), List(2, 6, 14))
//  val monster2 = Monster(3, "Worgs Rider", 2, alive = true, Point(), 18, 13, 0,
//    List(6), List(1, 8, 2), List(4), List(1, 6, 0))
//
//  monster2.ranged(monster1)

//  val point = Point()
//  point.move(3, 3)
//  printPoint()
//
//  def printPoint(){
//    println ("Point x location : " + point.x)
//    println ("Point y location : " + point.y)
//  }
}
