package ibm

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.mqtt._
import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}

import rapture.json._, jsonBackends.jawn._
/**
 * Skeleton for a Flink Job.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */



object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val brokerUrl = "ssl://f2msp0.messaging.internetofthings.ibmcloud.com:8883"

    val parameterTool = ParameterTool.fromArgs(args)

    val brokerUrl = parameterTool.get("brokerUrl", "ssl://ts46va.messaging.internetofthings.ibmcloud.com:8883")
    val topic = parameterTool.get("topic", "iot-2/type/+/id/+/evt/+/fmt/json")
    val org = parameterTool.get("org", "ts46va")
    val apiKey = parameterTool.get("api-key", "a-ts46va-p806hlysdv")
    val authToken = parameterTool.get("auth-token", "FN75DH2KFqRxUM(gyV")
    var appId = parameterTool.get("app-id", "flink-mqtt-patrick")

    env.getConfig.disableSysoutLogging()
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000) // create a checkpoint every 5 secodns
    env.getConfig.setGlobalJobParameters(parameterTool); // make parameters available in the web interface

    val deserializationSchema = new SimpleStringSchema()
    val serializationSchema = new SimpleStringSchema()

    // this allows cancelling the job
    val rc = new RunningChecker()
    val topics = Array(topic)

    //brokerURL: String, userName: String, password: String, clientId: String, deserializationSchema: DeserializationSchema[OUT], runningChecker: RunningChecker, topicNames: Array[String]
    //"a-ts46va-p806hlysdv", "FN75DH2KFqRxUM(gyV",
    //"a:ts46va:flink-mqtt-patrick",
    val mqttSCDemoSrc = new MQTTSourceConfig(
      brokerUrl,
      apiKey, authToken,
      s"a:$org:$appId",
//      "a-f2msp0-fgpooukoh3", "Q)x2jui2zqGt9Y_wZ5",
//      "a:f2msp0:flink-mqtt-patrick",
      deserializationSchema,rc,
      topics);   // get all events

    System.out.println("Create MQTT Source")
    val mqttSource = new MQTTSource(mqttSCDemoSrc)
    val messageStream = env.addSource(mqttSource)

    def mapper(x: String): (String,Json) = {
      try {
        val o = Json.parse(x)
        return (o.username.as[String], o)
      } catch {
        case unknown => return (null, null)
      }
    }
    var dataStream = messageStream.map { x => mapper(x)}
    dataStream = dataStream.filter { x => x._1 != null}
    dataStream = dataStream.filter { x => x._1 == "patrick-2"}
    val result = dataStream.keyBy(0).process(new TimeoutStateFunction())
    result.print()
    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
