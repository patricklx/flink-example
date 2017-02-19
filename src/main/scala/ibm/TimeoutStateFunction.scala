package ibm

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction.Context
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.RichProcessFunction
import rapture.json._, jsonBackends.jawn._

class Counter(var count: Long) {
  def this() {
    this(0)
  }
}

class TimeoutStateFunction extends RichProcessFunction[(String, Json), String] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[Counter] = getRuntimeContext().getState(new ValueStateDescriptor("myState", classOf[Counter]))


  override def processElement(value: (String, Json), ctx: Context, out: Collector[String]): Unit = {
    // initialize or retrieve/update the state

    var current = state.value
    if (current == null) {
      current = new Counter()
    }

    current.count += 1

    // write the state back
    state.update(current)

    out.collect((current.count, value).toString())
    // schedule the next timer 60 seconds from the current event time
    // ctx.timerService.registerEventTimeTimer(current.timestamp + 60000)
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[String]): Unit = {

  }
}