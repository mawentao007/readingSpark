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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
 * 将RDD分成多个buckets，基于shuffleDependency的partitioner
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling    //局部性调度，task执行的位置的局部性要求
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,                              //task包含partition
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {                //由executor的调用task的run方法来执行
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()                //创建新的序列器的实例
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](           //反序列化，返回RDD和SD
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)   //参数是二进制文件和当前线程的classloader

    metrics = Some(context.taskMetrics)          //监控器
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)   //map task写入
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])   //写iterator
      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        if (writer != null) {
          writer.stop(success = false)
        }
        throw e
    } finally {
      context.markTaskCompleted()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
