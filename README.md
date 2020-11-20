# 201120_ScalaSparkMavenPageRank

1. 在命令行中运行
2. 在 Idea 中运行

- [项目 GitHub 地址](https://github.com/lilinxi/201120_ScalaSparkMavenPageRank)

## 一、在命令行中运行

1. 首先安装 java，scala 和 spark 环境（MacOS）
2. 输入`spark-shell`开始运行

```shell script
brew cask install java
brew install scala
brew install apache-spark
spark-shell
```

3. 输入命令，输出结果，如下所示

```shell script
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 11.0.8)
Type in expressions to have them evaluated.
Type :help for more information.

scala> var links = sc.parallelize(
     |       List(
     |         ("A", List("B", "C", "D")),
     |         ("B", List("A")),
     |         ("C", List("A", "B")),
     |         ("D", List("B", "C"))
     |       )
     |     )
links: org.apache.spark.rdd.RDD[(String, List[String])] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> var ranks = links.mapValues(v => 1.0)
ranks: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[1] at mapValues at <console>:25

scala>     // 迭代 10 次

scala>     for (i <- 0 until 10) {
     |       val contributions = links.join(ranks).flatMap {
     |         case (pageID, (links, rank)) => links.map(link => (link, rank / links.size))
     |       }
     |       ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 * 1.0 + 0.85 * v)
     |     }

scala> ranks.collect().foreach(println) // 循环输出结果到命令行
(A,1.4729483816191942)
(B,1.151795159344013)
(C,0.8081470492728162)
(D,0.5671094097639748)

scala> 
```

## 二、在 Idea 中运行

1. 在 Maven 中配置 Scala 和 Spark，下面文件为 pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>201120_ScalaSparkMavenPageRank</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.0.1</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
    </dependencies>


</project>
```

2. 编写代码，下面文件为 PageRank.scala

```scala
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
         // 通过 map 函数计算每个 link 的权重
        case (pageID, (links, rank)) => links.map(link => (link, rank / links.size))
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

      // 通过 reduce 函数计算每个 link 本轮更新后的权值
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 * 1.0 + 0.85 * v)
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
```

## 三、运行结果

综上，迭代 10 次后的运行结果为：

```shell script
(B,1.151795159344013)
(A,1.4729483816191942)
(C,0.8081470492728162)
(D,0.5671094097639748)
```