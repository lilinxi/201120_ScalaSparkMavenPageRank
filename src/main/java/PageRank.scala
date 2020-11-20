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

    // Debug：将 ranks 的结果循环输出
    ranks.collect().foreach(println)
    /**
     * (A,1.0)
     * (B,1.0)
     * (C,1.0)
     * (D,1.0)
     */

    // Debug：将 links 表和 ranks 表连接后的结果循环输出
    // join 函数将 key 相同的 value 连接到一个列表中
    links.join(ranks).collect().foreach(println)
    /**
     * (B,(List(A),1.0))
     * (A,(List(B, C, D),1.0))
     * (C,(List(A, B),1.0))
     * (D,(List(B, C),1.0))
     */

    // 迭代 10 次
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap { // 将 links 表和 ranks 表连接
        case (pageID, (links, rank)) => links.map(link => (link, rank / links.size)) // 通过 map 函数计算每个 link 的权重
      }

      // Debug：将 link 权重的中间结果循环输出
      contributions.collect().foreach(println)
      /**
       * (A,1.0)
       * (B,0.3333333333333333)
       * (C,0.3333333333333333)
       * (D,0.3333333333333333)
       * (A,0.5)
       * (B,0.5)
       * (B,0.5)
       * (C,0.5)
       */

      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 * 1.0 + 0.85 * v) // 通过 reduce 函数计算每个 link 本轮更新后的权值
    }

    ranks.collect().foreach(println) // 循环输出结果到命令行
    /**
     * (B,1.151795159344013)
     * (A,1.4729483816191942)
     * (C,0.8081470492728162)
     * (D,0.5671094097639748)
     */

    ranks.saveAsTextFile("result") // 保存结果文件
  }
}