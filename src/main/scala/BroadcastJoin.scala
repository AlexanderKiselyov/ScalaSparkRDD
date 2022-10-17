import org.apache.spark.SparkContext

object BroadcastJoin {
  def main(args: Array[String]): Unit = {
    val input1 = args(0)
    val input2 = args(1)
    val output = args(2)
    val sc = new SparkContext()
    val pairRdd = sc.textFile(input1)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => (line.split(",").apply(0), line.split(",").apply(1)))
    val pairRdd2 = sc.textFile(input2)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => (line.split(",").apply(2), line.split(",").apply(0) + "," + line.split(",").apply(1)))
    pairRdd.saveAsTextFile(output + "1")
    pairRdd2.saveAsTextFile(output + "2")
    val broadcasted = sc.broadcast(pairRdd.collect().toMap)
    pairRdd2.flatMap(x => {
      broadcasted.value.get(x._1) match {
        case Some(r) => Some(x._1, (x._2, r))
        case None => None
      }
    }).saveAsTextFile(output)
    sc.stop()
  }
}
