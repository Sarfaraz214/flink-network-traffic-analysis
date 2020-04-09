package analysis.helper

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

class DistinctUsers extends RichFlatMapFunction[(String, String), (String, Int)] { //[(input), (output)]

  private var userState: ListState[String] = _

  override def flatMap(input: (String, String), out: Collector[(String, Int)]): Unit = {

    userState.add(input._2)

    var distinctUsers: Set[String] = Set()
    for (user <- userState.get) {
      distinctUsers += user

      out.collect((input._1, distinctUsers.size))
    }

  }

  override def open(parameters: Configuration): Unit = {
    val listDesc = new ListStateDescriptor[String]("userState", BasicTypeInfo.STRING_TYPE_INFO)
    userState = getRuntimeContext.getListState(listDesc)
  }
}