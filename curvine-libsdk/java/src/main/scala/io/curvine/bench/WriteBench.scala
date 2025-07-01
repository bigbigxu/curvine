/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.curvine.bench

import io.curvine.bench.WriteBench.LOGGER
import io.curvine.executor.FixedAllocationExecutor
import org.apache.commons.lang3.RandomStringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.slf4j.Logger

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer

class WriteBench(params: BenchParams) {
  val path: Path = new Path(params.dataDir)

  val fs: FileSystem = FileSystem.get(path.toUri, Utils.getCurvineConf)

  fs.mkdirs(path)

  val executor = new FixedAllocationExecutor(params.threads, 100, 0);

  val writers: ListBuffer[(FSDataOutputStream, Array[Byte])] = createAllWriter()

  private val checksum = new LongAdder

  private val totalReadLen = new LongAdder

  def clear(): Unit = {
    fs.delete(new Path(params.dataDir), true)
  }

  def createAllWriter():  ListBuffer[(FSDataOutputStream, Array[Byte])] = {
    val writers = ListBuffer[(FSDataOutputStream, Array[Byte])]()
    0.until(params.fileNum).foreach(index => {
      val bytes =  RandomStringUtils.randomAscii(params.bufferSize).getBytes
      val start = System.currentTimeMillis()
      val path = new Path(params.dataDir, index.toString)
      writers.append((fs.create(path, true), bytes))
      LOGGER.info(s"create writer $index cost ${System.currentTimeMillis() - start} ms")
    })

    writers
  }

  def writeMulti(): Unit = {
    val loopNum = params.fileSize / params.bufferSize
    0.until(loopNum.toInt).foreach(i => {
      0.until(params.fileNum).foreach(index => {
        executor.execute(index, () => {
          val (output, bytes) = writers(index)
          output.write(bytes)

          if (params.checksum) {
            totalReadLen.add(bytes.length)
            checksum.add(Utils.crc32(bytes))
          }

          if (i == loopNum - 1) {
            //val start = System.currentTimeMillis()
            output.close()
            //LOGGER.info(s"close writer $index cost ${System.currentTimeMillis() - start} ms")
          }
        })
      })
    })

    executor.shutdown()
  }

  def getChecksum: Long = {
    checksum.longValue()
  }

  def getReadSizeString: String = {
    Utils.bytesToString(totalReadLen.longValue())
  }
}

object WriteBench {
  val LOGGER: Logger = CurvineBench.LOGGER

  def run(params: BenchParams): Unit = {
    val bench = new WriteBench(params)

    LOGGER.info(s"write-bench start")
    val start = System.currentTimeMillis()
    bench.writeMulti()
    val cost = (System.currentTimeMillis() - start) / 1000D

    val base = (params.totalSize() / cost).toLong

    val spreed = Utils.bytesToString(base)
    val width = Utils.bytesToString(base * 8)

    LOGGER.info(s"\nwrite size: ${params.totalSizeString()}, cost: $cost s, speed: $spreed/s, width: $width/s; " +
      s"\nchecksum: ${bench.getChecksum}, readSize: ${bench.getReadSizeString};" +
      s"\nparams: $params")

    if (params.clearDir) {
      bench.clear()
    }
  }
}