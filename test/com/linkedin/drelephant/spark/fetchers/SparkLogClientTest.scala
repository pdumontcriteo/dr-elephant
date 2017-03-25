/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.fetchers

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI

import scala.concurrent.ExecutionContext

import com.google.common.io.Resources
import com.ning.compress.lzf.LZFEncoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, PositionedReadable}
import org.apache.hadoop.io.compress.CompressionInputStream
import org.apache.spark.SparkConf
import org.mockito.BDDMockito
import org.scalatest.{AsyncFunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

class SparkLogClientTest extends AsyncFunSpec with Matchers with MockitoSugar {
  import SparkLogClientTest._

  describe("SparkLogClient") {
    it("throws an exception if spark.eventLog.dir is missing") {
      an[IllegalArgumentException] should be thrownBy { new SparkLogClient(new Configuration(), new SparkConf()) }
    }

    it("uses spark.eventLog.dir if it is already an webhdfs URI") {
      val hadoopConfiguration = new Configuration()
      val sparkConf = new SparkConf().set("spark.eventLog.dir", "webhdfs://nn1.grid.example.com:50070/logs/spark")
      val sparkLogClient = new SparkLogClient(hadoopConfiguration, sparkConf)
      sparkLogClient.webhdfsEventLogUri should be(new URI("webhdfs://nn1.grid.example.com:50070/logs/spark"))
    }

    it("uses a webhdfs URI constructed from spark.eventLog.dir and dfs.namenode.http-address if spark.eventLog.dir is an hdfs URI") {
      val hadoopConfiguration = new Configuration()
      hadoopConfiguration.set("dfs.namenode.http-address", "0.0.0.0:50070")
      val sparkConf = new SparkConf().set("spark.eventLog.dir", "hdfs://nn1.grid.example.com:9000/logs/spark")
      val sparkLogClient = new SparkLogClient(hadoopConfiguration, sparkConf)
      sparkLogClient.webhdfsEventLogUri should be(new URI("webhdfs://nn1.grid.example.com:50070/logs/spark"))
    }

    it("returns the desired data from the Spark event logs") {
      import ExecutionContext.Implicits.global

      val hadoopConfiguration = new Configuration()
      hadoopConfiguration.set("dfs.namenode.http-address", "0.0.0.0:50070")

      val sparkConf =
        new SparkConf()
          .set("spark.eventLog.dir", "hdfs://nn1.grid.example.com:9000/logs/spark")
          .set("spark.eventLog.compress", "true")
          .set("spark.io.compression.codec", "lzf")

      val appId = "application_1"
      val attemptId = Some("1")

      val sparkLogClient = new SparkLogClient(hadoopConfiguration, sparkConf) {
        override lazy val fs: FileSystem = {
          val fs = mock[FileSystem]
          val expectedPath = new Path("webhdfs://nn1.grid.example.com:50070/logs/spark/application_1_1.lzf")
          BDDMockito.given(fs.exists(expectedPath)).willReturn(true)
          BDDMockito.given(fs.open(expectedPath)).willReturn(
            new FSDataInputStream(new FakeCompressionInputStream(
              new ByteArrayInputStream(LZFEncoder.encode(EVENT_LOG_2))))
          )
          fs
        }
      }

      sparkLogClient.fetchData(appId, attemptId).map { logDerivedData =>
        val expectedProperties = Map(
          "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.storage.memoryFraction" -> "0.3",
          "spark.driver.memory" -> "2G",
          "spark.executor.instances" -> "900",
          "spark.executor.memory" -> "1g",
          "spark.shuffle.memoryFraction" -> "0.5"
        )
        val actualProperties = logDerivedData.appConfigurationProperties
        actualProperties should be(expectedProperties)
      }
    }
  }
}

object SparkLogClientTest {
  class FakeCompressionInputStream(in: InputStream) extends CompressionInputStream(in) with PositionedReadable {
    override def read(): Int = in.read()
    override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
    override def read(pos: Long, buffer: Array[Byte], off: Int, len: Int): Int = ???
    override def readFully(pos: Long, buffer: Array[Byte], off: Int, len: Int): Unit = ???
    override def readFully(pos: Long, buffer: Array[Byte]): Unit = ???
    override def resetState(): Unit = ???
  }

  private val EVENT_LOG_2 = Resources.toByteArray(
    Resources.getResource("spark_event_logs/event_log_2"))
}
