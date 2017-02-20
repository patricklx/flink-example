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
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}
import rapture.json.Json,org.apache.flink.api.java.tuple.Tuple
import rapture.json._, jsonBackends.jawn._
import rapture.core._
import modes.keepCalmAndCarryOn._
//import modes.returnOption._

object Job {

  def counter(stream: KeyedStream[(String, Json), Tuple]): Unit = {
    val result = stream.process(new CounterFunction())
    result.print()
  }

  def waterLeak(stream: KeyedStream[(String, Json), Tuple]): Unit = {
    var filtered = stream.filter { _ match {case (user: String, data: Json) => data.d.states.waterLeakage.value == true}}
    var hazard = stream.map { _ match {case (user: String, data: Json) => s"""{"user": $user, "shield": "waterleak"}"""}}
    hazard.print()
  }

  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val brokerUrl = "ssl://f2msp0.messaging.internetofthings.ibmcloud.com:8883"

    val parameterTool = ParameterTool.fromArgs(args)

    val brokerUrl = parameterTool.get("brokerUrl", "ssl://ts46va.messaging.internetofthings.ibmcloud.com:8883")
    val topic = parameterTool.get("topic", "iot-2/type/+/id/+/evt/+/fmt/json")
    val org = parameterTool.get("org", "ts46va")
    val apiKey = parameterTool.get("api-key", "a-ts46va-hjn3o39irt")
    val authToken = parameterTool.get("auth-token", "W0i*R09U*_I@1eiT+_")
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
    val mqttSCDemoSrc = new MQTTSourceConfig(
      brokerUrl,
      apiKey, authToken,
      s"A:$org:$appId",
      deserializationSchema,rc,
      topics
    );

    System.out.println("Create MQTT Source")
    val mqttSource = new MQTTSource(mqttSCDemoSrc)
    val messageStream = env.addSource(mqttSource)

    var dataStream = messageStream.map { x => Json.parse(x)}
    dataStream = dataStream.filter {x => x.username.as[String] != null}
    val mappedStream = dataStream.map { x: Json => (x.username.as[String], x) }
    val keyedStream = mappedStream.keyBy(0)

    this.counter(keyedStream)
    this.waterLeak(keyedStream)
    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
