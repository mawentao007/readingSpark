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

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * stage是一组独立的tasks，都计算相同的函数，作为一个spark job的一部分，所有的task有相同的shuffle依赖，每个tasks的DAG
 * 被划分成很多stages，边界就是shuffle发生的时候，然后执行这些stages，以拓扑顺序。
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * 每个dag可以是shuffle map dag，所有的task的输入都是另一个stage的输入，也可以是resultstage，task之间计算结果来初始化一个job
 * 对于shuffle map stages，要跟踪每个输入partition在哪个节点上。
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * 每个stage都有jobId，就是第一高提交这个stage的job。当使用FIFO调度，jobs越早，stages被计算的越早，恢复越早
 * Each Stage also has a jobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * callSite提供一个位置，与stage相关，对于shuffle map stage，callSite提供一个位置，创建之后的rdd shuffle的的位置，对于result stage，
 * callSite提供执行相应操作。
 * The callSite provides a location in user code which relates to the stage. For a shuffle map
 * stage, the callSite gives the user code that created the RDD being shuffled. For a result
 * stage, the callSite gives the user code that executes the associated action (e.g. count()).
 *
 * A single stage can consist of multiple attempts. In that case, the latestInfo field will
 * be updated for each attempt.
 *
 */
private[spark] class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val shuffleDep: Option[ShuffleDependency[_, _, _]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val jobId: Int,
    val callSite: CallSite)
  extends Logging {
  logInfo("***********************Marvin****************************  Stage")

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions = rdd.partitions.size
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]         //这个stage属于哪些jobs

  /** For stages that are the final (consists of only ResultTasks), link to the ActiveJob. */
  var resultOfJob: Option[ActiveJob] = None    //对于final stage，和ActiveJob联系
  var pendingTasks = new HashSet[Task[_]]

  private var nextAttemptId = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  /** Pointer to the latest [StageInfo] object, set by DAGScheduler. */
  var latestInfo: StageInfo = StageInfo.fromStage(this)

  def isAvailable: Boolean = {
    if (!isShuffleMap) {
      true
    } else {
      numAvailableOutputs == numPartitions
    }
  }

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)     //输出位置，就是MapStatus
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }

  /** Return a new attempt id, starting with 0. */
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def attemptId: Int = nextAttemptId

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}
