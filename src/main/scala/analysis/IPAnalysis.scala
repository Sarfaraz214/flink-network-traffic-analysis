/**
 * ## user_id, network_name, user_IP, user_country, website, time spent before next click
 *
 * For every 10 second find out for US country -
 *
 * a.) total number of clicks on every website in separate file
 *
 * b.) the website with maximum number of clicks in separate file.
 *
 * c.) the website with minimum number of clicks in separate file.
 *
 * d.) Calculate number of distinct users on every website in separate file.
 *
 * e.) Calculate the average time spent on website by users.
 */

package analysis

import analysis.helper.DistinctUsers
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object IPAnalysis {

  val inputFilePath: String = getClass.getClassLoader.getResource("IP_DATA.txt").getPath

  def main(args: Array[String]): Unit = {

    if(args.length == 0) {
      println("Path for output file is missing!!")
      return
    }

    val outputFilePath = args(0)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    println("**** IP DATA ANALYSIS STARTED ****")

    val data: DataStream[String] = env.readTextFile(inputFilePath)

    val mappedData: DataStream[(String, String)] = data.map { x =>
      val words = x.split(",")
      (words(4), x)
    }

    // US click stream only
    val usStream: DataStream[(String, String)] = mappedData.filter { x =>
      val country = x._2.split(",")(3)
      !country.equals("US")
    }

    // total number of clicks on every website in US
    val clicksPerWebsite: DataStream[(String, Int)] = usStream.map(x => (x._1, x._2, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
      .sum(2)
      .map(x => (x._1, x._3))

    clicksPerWebsite.writeAsText(outputFilePath + "/clicksPerWebsite.txt").setParallelism(1)

    // website with max clicks
    val maxClicks: DataStream[(String, Int)] = clicksPerWebsite
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
      .maxBy(1)

    maxClicks.writeAsText(outputFilePath + "/maxClicks.txt").setParallelism(1)

    // website with min clicks
    val minClicks: DataStream[(String, Int)] =
      clicksPerWebsite
        .keyBy(0)
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
        .minBy(1)

    minClicks.writeAsText(outputFilePath + "/minClicks.txt").setParallelism(1)

    // average time spent on website by users
    val avgTimeWebsite: DataStream[(String, Int)] = usStream.map { x =>
      val timeSpent = x._2.split(",")(5).toInt
      (x._1, 1, timeSpent)
    }
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
      .reduce((curr, prev) => {
        val sum1 = prev._2 + curr._2
        val sum2 = prev._3 + curr._3
        (curr._1, sum1, sum2)
      })
      .map(x => (x._1, x._3 / x._2))

    avgTimeWebsite.writeAsText(outputFilePath + "/avgTimeWebsite.txt").setParallelism(1)

    // distinct users on each website
    val distinctUsersPerWebsite: DataStream[(String, Int)] = usStream
      .keyBy(0)
      .flatMap(new DistinctUsers())

    distinctUsersPerWebsite.writeAsText(outputFilePath + "/distinctUsersPerWebsite.txt").setParallelism(1)

    env.execute("IP Data Analysis")

    println("**** IP DATA ANALYSIS COMPLETED ****")
  }
}
