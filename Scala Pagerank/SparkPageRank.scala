package sparkpagerank

import scala.io.Source

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

  /**
   * Object to run page rank on spark 
   *
   */
object SparkPageRank {
  var danglingNodeScore = 0.0
  var pageCnt = 3175035

  /**
   * Main method for running pagerank
   *
   */
  def main(args: Array[String]): Unit = {
	// create spark context
    val sc = new SparkContext(new SparkConf())
	// parse the input using java parser
    utils.Parser.process(args)
	// store page count
    pageCnt = Parser.getPageCnt.toInt
	// calculate page ranks
    var finalRanks = pageRankJob(args, sc)
	// output top-100 pages
    topKResults(args, finalRanks, sc)
  }

  /**
   * Method for getting dangling node score
   **/
  def getDanglingNodeScore(filename: String): Double = {
    var danglingNodeScore = 0.0
	// find out all dangling nodes and accumulate their pageranks
    for (line <- Source.fromFile(filename).getLines) {
      val tokens = line.split(",")
      var str = tokens(1)
      str = str.substring(str.lastIndexOf("(") + 1)
      if (str.length() == 1) {
        var str1 = tokens(tokens.length - 1)
        str1 = str1.substring(0, str1.indexOf(")"))
        danglingNodeScore = danglingNodeScore + str1.toDouble
      }
    }

    return danglingNodeScore
  }

  /**
   * Method for getting top 100 results in descending order
   *
   * @param conf
   * @param input
   * @param output
   * @throws Exception
   */
  private def topKResults(args: Array[String], ranks: org.apache.spark.rdd.RDD[(String, Double)], sc: SparkContext): Unit = {
	// sort the final ranks
    var sortedRanks = ranks.sortBy(_._2, false, 1)
	// output top 100 page ranks
    sc.parallelize(sortedRanks.take(100)).partitionBy(new HashPartitioner(1)).saveAsTextFile(args(1))
  }

  /**
   * Helper Method to calculate the page rank
   *
   * @param conf
   * @param iteration
   * @param input
   * @throws Exception
   */
  def getPageRank(args: Array[String], iteration: Int,
                  sc: SparkContext, links: org.apache.spark.rdd.RDD[(String, List[String])],
                  ranks: org.apache.spark.rdd.RDD[(String, Double)]): org.apache.spark.rdd.RDD[(String, Double)] = {
	
	// create faltMap for contributions to each outlink
    val contributions = links.join(ranks).flatMap {
      case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
    }
	
	// join ranks to the list of lins
    val r = links.join(ranks)
	// output the page rank for iteration
    r.partitionBy(new HashPartitioner(1)).saveAsTextFile(args(0) + "/page_rank_output/PR/" + iteration)
    val filename = args(0) + "/page_rank_output/PR/" + iteration + "/part-00000"
    // get dangling node score
	var danglingNodeScore = getDanglingNodeScore(filename)
	// get dangling node score distribution
    var danglingScoreDistribution = danglingNodeScore / pageCnt
    //println(danglingScoreDistribution)
	// calculate final page ranks for iteration
    var finalranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => (0.15 / pageCnt) + 0.85 * (danglingScoreDistribution + v))

	// save page ranks and return for next iteration
    finalranks.saveAsTextFile(args(0) + "/page_rank_output/PR_" + iteration);
    finalranks
  }

  /**
   * Method to calculate the page rank
   *
   * @param conf
   * @param iteration
   * @param input
   * @throws Exception
   */
  def pageRankJob(args: Array[String], sc: SparkContext): org.apache.spark.rdd.RDD[(String, Double)] = {
    val map = scala.collection.mutable.Map("" -> List(""))
	// read the parser output
    val filename = args(0) + "/parser_output/part-r-00000"
    for (line <- Source.fromFile(filename).getLines) {
      val tokens = line.split("#")
	  // generate a List(String: Node, List(String: Outlinks))
      if (!tokens(0).isEmpty() && !tokens(0).equals("")) {
        if (tokens.length <= 1) {
          map(tokens(0)) = List("")
        } else {
          map(tokens(0)) = tokens(1).split("~").toList
        }
      }
    }

    val adjList: List[(String, List[String])] =
      map.toList
	
	// convert list to RDD
    val links = sc.parallelize(adjList).partitionBy(new HashPartitioner(1)).persist()

    var i = 0;
	// set initial page rank to 1/total pages
    var ranks = links.mapValues(v => 1.0 / pageCnt)
	// iterate 10 times
    for (i <- 0 until 10) {
      ranks = getPageRank(args, i, sc, links, ranks)
    }

    ranks
  }
}