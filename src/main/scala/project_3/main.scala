package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main {
  val seed = new java.util.Date().hashCode;
  val rand = new scala.util.Random(seed);
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {

    val test = g_in.aggregateMessages[Int](
      msg => {
        if(msg.dstAttr == 1 && msg.srcAttr == 1){
          msg.sendToDst(2)
          msg.sendToSrc(2)
        }
        else if(msg.dstAttr == -1 && msg.srcAttr == -1){
          msg.sendToSrc(-1)
          msg.sendToDst(-1)
        }
        else{
          msg.sendToSrc(0)
          msg.sendToDst(0)
        }
      },
      (msg1, msg2) => {
        if (msg1 == 2 || msg2 == 2){
          2
        }
        else if (msg1 == -1 && msg2 == -1) {
          -1
        }
        else{
          0
        }
      }
    )
    val count2 = test.filter(x => x._2 == 2).count()
    val count_1 = test.filter(x => x._2 == -1).count()
    var ans = count2 == 0 && count_1 == 0
    return ans
  }

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id, attr) => (0, 0.asInstanceOf[Float]))
    //Setting attribute that first one shows whether one vertex is active or not, the second one is going to be assigned value of random float.
    var remaining_vertices = 2
    //initialize remaining_vertices
    var count = 0
    while (remaining_vertices >= 1) {
      count += 1
      g = g.mapVertices((id, attr) => (attr._1, rand.nextFloat()))
      //g.vertices.collect().foreach(println(_))

      val v1 = g.aggregateMessages[(Int, Float)](
        msg => {
          if ((msg.srcAttr._2 + msg.srcAttr._1) > (msg.dstAttr._2 + msg.dstAttr._1)) {
            msg.sendToSrc(1, 0) //tells src that it is bigger
            msg.sendToDst(0, 0) //tells dst that it is smaller
          }
          else {
            msg.sendToSrc(0, 0)
            msg.sendToDst(1, 0)
          }
        },
        (msg1, msg2) => if (msg1._1 == 1 && msg2._1 == 1) (1, 0) else (0, 0)
        //merge messages so that if one vertex receives multiple messages showing it's biggest, make it 1.
      )
      //v1 compare the bx to bv and returns corresponding messages to nearby vertices.

      val g2 = Graph(v1, g.edges)
      //g2.vertices.collect.foreach(println(_))

      val v2 = g2.aggregateMessages[(Int, Float)](
        msg => {
          if (msg.dstAttr._1 == 1) {
            msg.sendToDst(1, 0)
            msg.sendToSrc(-1, 0)
          }
          else if (msg.srcAttr._1 == 1) {
            msg.sendToDst(-1, 0)
            msg.sendToSrc(1, 0)
          }
          else {
            msg.sendToSrc(0, 0)
            msg.sendToDst(0, 0)
          }
        },
        (msg1, msg2) => {
          if (msg1._1 == 1 || msg2._1 == 1) {
            (1, 0)
          }
          else if (msg1._1 == -1 || msg2._1 == -1) {
            (-1, 0)
          }
          else {
            (0, 0)
          }
        }
      )
      //g.vertices.collect().foreach(println(_))
      g = Graph(v2, g.edges)
      g.cache()
      remaining_vertices = g.vertices.filter({ case (id, attr) => (attr._1 == 0) }).count().toInt
      println(remaining_vertices)
      //Filter vertices that has attribute showing it's active and not in MIS
    }
    println("count = " + count)
    val ans = g.mapVertices((id, attr) => (attr._1))
    //ans.vertices.collect().foreach(println(_))
    return ans
  }


//gs://group2-csci3390-cluster/project_3_2.12-1.0.jar
  //gs://group2-csci3390-cluster/twitter_original_edges.csv
 // gs://group2-csci3390-cluster/



  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    //conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir("/Users/stevezhang/Downloads/test/")
    val spark = SparkSession.builder.config("spark.master", "local").getOrCreate()
    /* You can either use sc or spark */

    if (args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if (args(0) == "compute") {
      if (args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }

      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {
        val x = line.split(","); Edge(x(0).toLong, x(1).toLong, 1)
      }).filter({ case Edge(a, b, c) => a != b })
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))

    }
    else if (args(0) == "verify") {
      if (args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {
        val x = line.split(","); Edge(x(0).toLong, x(1).toLong, 1)
      })
      val g_in = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      //g_in.vertices.collect().foreach(println)
      val vertices = sc.textFile(args(2)).map(line => {
        val x = line.split(","); (x(0).toLong, x(1).toInt)
      })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      //edges.collect().foreach(println)
      //vertices.collect().foreach(println)
      val ans = verifyMIS(g)
      if (ans)
        println("Yes")
      else
        println("No")
    }
    else {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
  }
}
