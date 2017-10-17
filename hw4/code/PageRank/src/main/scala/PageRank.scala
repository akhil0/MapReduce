/**
* Created by akhil0 on 10/26/16.
*/

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {

  def evaluate(contributions: RDD[(String, Double)]) = {
    contributions.sparkContext.runJob(contributions,(iter: Iterator[(String, Double)]) => {
      while(iter.hasNext) iter.next()
    })
  }

  def main(args: Array[String]) {

    // Spark configuration
    val conf = new SparkConf()
    //conf.setMaster("local")
    conf.setAppName("PageRank")
    val sc = new SparkContext(conf)

    // Load the bz2files into RDD
    val pairRDD = sc.textFile(args(0))

    // Go through each line and map with lambda as Parser provided
    val adjRDD = pairRDD.map(k => new Bz2WikiParser().getAdjList(k))
      //filter if return is null
      .filter(k => (k != null))
      .map(line => line.split("``"))
      // convert into RDD with pagenode as key, adjacency list as value
      .map(fields => (fields(0), buildArray(fields(1)))).persist()

    // Count of all the nodes intially
    val docuCount = adjRDD.count

    // Initialize all nodes to 1/N value
    var rankRDD = adjRDD.map(k => (k._1, 1 / docuCount.toDouble))



    // 10 iterations
    for (iteration <- 0 to 9) {

      //val danglingval = sc.doubleAccumulator("DELTA_LOSS")
      var danglingval = sc.accumulator[Double](0.0)

      // contributions from each node
      val contributions = adjRDD.join(rankRDD).flatMap {
        //signature for joined k,v; for each link in links, emit (link, rank/links.size)
        case (url, (links, rank)) => {
          // if dangling node, add the value to dangling
          if (links.size == 0) {
            danglingval+=rank
            None
            //links.map(adjnode => (adjnode, rank / links.size))
          }
          else
          //emit each adjnode contribution
            links.map(adjnode => (adjnode, rank / links.size))
        }
      }



      //val contributionscount = contributions.count

      evaluate(contributions)

      //val danglingloss = contributions.subtractByKey(adjRDD).mapValues(x => x + danglingval)

      //Updating ???
      val danglingvalue = danglingval.value


      // new Rank RDD is achieved by doing reducebyKey and then applying page rank formulae
      val newrankRDD = contributions.reduceByKey((a, b) => a + b)
        .mapValues(v => 0.15 / docuCount + 0.85 * (v + (danglingvalue / docuCount)))

      // add the dangling value nodes which are missing back to original rankRDD
      rankRDD = rankRDD.subtractByKey(newrankRDD).mapValues(x => 0.15 / docuCount + 0.85 * (danglingvalue / docuCount))
        .union(newrankRDD)

    }

    // Get Top-100 pageranks
    var finallist = rankRDD.sortBy(v => v._2, false).take(100)
    sc.parallelize(finallist, 1).saveAsTextFile(args(1))

    sc.stop()
  }

  // Function builds a List from String containing Adjacency Nodes:
  // [a, b, c] => List[a, b, c]
  def buildArray(line: String): List[String] = {
    val lineop = line.substring(1, line.length() - 1)
    if (lineop.length() == 0)
      return List[String]()
    else
      return lineop.split(", ").map(_.trim).toList;
  }


}