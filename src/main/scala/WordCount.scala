import org.apache.spark.SparkContext
import scala.collection.mutable

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val stopWordsFile = args(2)
    val sc = new SparkContext()
    val stopWords = sc.textFile(stopWordsFile).collect()
    sc.textFile(input)
      .flatMap(line => line.trim().replaceAll("\\pP", "").toLowerCase().split("\\s+"))
      .filter(word => filterPair(word, stopWords))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy { case (word, count) => count }
      .map(pair => (pair._2, pair._1))
      .map(pair => mutable.StringBuilder.newBuilder.append(pair._2).append("\u0009").append(pair._1))
      .saveAsTextFile(output)
    sc.stop()
  }

  def filterPair(word : String, stopWordsLines : Array[String]): Boolean = {
    for (stopWordsLine <- stopWordsLines) {
      val splitedLines = stopWordsLine.trim().replaceAll("\\pP", "").toLowerCase().split("\\s")
      for (stopWord <- splitedLines) {
        if (stopWord.equals(word)) return false
      }
    }
    true
  }
}
