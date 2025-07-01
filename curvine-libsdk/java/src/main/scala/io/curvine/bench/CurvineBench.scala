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

import io.curvine.{CurvineFsMount, CurvineNative, FilesystemConf}
import org.slf4j.{Logger, LoggerFactory}

object CurvineBench {
  val LOGGER: Logger = LoggerFactory.getLogger(CurvineBench.getClass)

  def main(args: Array[String]): Unit = {
    val params = BenchParams.buildFromArgs(args);
    params.action match {
      case "write" => WriteBench.run(params)
      case "read" => ReadBench.run(params)
      // case "jni" => testJni()
      case _ => throw new RuntimeException(s"Unsupported operations ${params.action}")
    }
  }

/*  def testJni(): Unit = {
    println("start test jni")

    val str = "1"
    CurvineNative.objectAddr(str)
    val lib = new CurvineFsMount(new FilesystemConf(Utils.getCurvineConf)).getCurvineFsLib

    var start = System.nanoTime()
    0.until(100000).foreach(_ => {
      CurvineNative.objectAddr(str)
    })
    println(s"jni avg cost ${(System.nanoTime() - start) / 1000 / 100000} us")


    start = System.nanoTime()
    0.until(100000).foreach(_ => {
      lib.fs_object_addr(str)
    })
    println(s"jna avg cost ${(System.nanoTime() - start) / 1000 / 100000} us")
  }*/
}
