/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.hash

import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.{Logging, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.storage.{BlockObjectWriter}
import org.apache.spark.serializer.Serializer
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus

private[spark] class HashShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency   //依赖
  private val numOutputSplits = dep.partitioner.numPartitions     //partition的个数就是split的个数
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val shuffleBlockManager = blockManager.shuffleBlockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,        //important
    writeMetrics)

  /** Write a bunch of records to this task's output
    * 写一组记录到task的输出
    * */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)    //获得输出的partition的id
      shuffle.writers(bucketId).write(elem)            //向相应的partition里面写入元素
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        try {
          Some(commitWritesAndBuildStatus())
        } catch {
          case e: Exception =>
            success = false
            revertWrites()
            throw e
        }
      } else {
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).   //提交写，获得每个bucket block的大小
    val compressedSizes = shuffle.writers.map { writer: BlockObjectWriter =>
      writer.commitAndClose()
      val size = writer.fileSegment().length
      MapOutputTracker.compressSize(size)          //计算出压缩后的大小
    }

    new MapStatus(blockManager.blockManagerId, compressedSizes)
  }

  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
