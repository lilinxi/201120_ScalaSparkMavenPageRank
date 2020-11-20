import org.apache.spark.{SparkConf, SparkContext} // 导入需要的依赖

object PageRank { // 定义主类
  def main(args: Array[String]): Unit = { // 定义主函数
    val conf = new SparkConf().setAppName("PageRank").setMaster("local") // 初始化 Spark 配置
    val sc = new SparkContext(conf) // 获取 Spark Context

    // 定义 links 数据
    var links = sc.parallelize(
      List(
        ("A", List("B", "C", "D")),
        ("B", List("A")),
        ("C", List("A", "B")),
        ("D", List("B", "C"))
      )
    )

    // 初始化 ranks 中每个页面的 PR 值为 1.0
    var ranks = links.mapValues(v => 1.0)

    // 迭代 10 次
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageID, (links, rank)) => links.map(link => (link, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v =>
        0.15 * 1.0 + 0.85 * v)
    }

    ranks.collect().foreach(println)
    ranks.saveAsTextFile("result")
  }
}