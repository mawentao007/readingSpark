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

import java.io.NotSerializableException
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.max
import scala.math.min

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.{Clock, SystemClock}

/**
 * 在一个TaskSet中调度一个一组task。这个类跟中每一个task，如该task失败了，就重试，通过延迟调度来进行数据局部性调度。主要的结构是resourceOffer，询问TaskSet
 * 是否需要执行一个task，还有更新状态，告诉它task的状态已经改变。
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails more than this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    clock: Clock = SystemClock)
  extends Schedulable with Logging {

  val conf = sched.sc.conf

  /*
   * Sometimes if an executor is dead or in an otherwise invalid state, the driver
   * does not realize right away leading to repeated task failures. If enabled,
   * this temporarily prevents a task from re-launching on an executor where
   * it just failed.
   * 有时候executor的失效无法立刻被察觉导致反复失败，这个字段可以阻止task立刻在刚刚失败的executor上重启
   */
  private val EXECUTOR_TASK_BLACKLIST_TIMEOUT =
    conf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L)

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val successful = new Array[Boolean](numTasks)
  private val numFailures = new Array[Int](numTasks)
  // key is taskId, value is a Map of executor id to when it failed
  private val failedExecutors = new HashMap[Int, HashMap[String, Long]]()        //key是taskID，value是失败的executorid到失效时间的映射

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)      //一个队列，其中存放的都是TaskInfo列表,队列元素的个数就是task的个数
  var tasksSuccessful = 0

  var weight = 1
  var minShare = 0
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  var name = "TaskSet_" + taskSet.stageId.toString
  var parent: Pool = null

  val runningTasksSet = new HashSet[Long]
  override def runningTasks = runningTasksSet.size

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  //当这个taskSetManager上没有task应该被启动时候这个字段为真，当每个task都至少成功运行一次，或者这个taskSet被废除，当所有的task都完成运行，状态解除。
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  //每个executor上的task数量，作为栈来处理，可以尽快发现task失败
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]        //key是executorid，value是task数组

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]        //没有局部性倾向的tasks

  // Set containing all pending tasks (also used as a stack, as above).           //所有的task
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  val taskInfos = new HashMap[Long, TaskInfo]       //索引是taskId，映射到taskInfo类

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  //给当前taskSetManager中所有的task分配一个epoch
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  //将所有的tasks加入到pending 列表，逆序加入是因为列表作为一个栈，后进先出，这样可以保证索引小的先执行
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  var myLocalityLevels = computeValidLocalityLevels()
  var localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  var lastLaunchTime = clock.getTime()  // Time we last launched a task at this level

  override def schedulableQueue = null

  override def schedulingMode = SchedulingMode.NONE

  var emittedTaskSizeWarning = false

  /**
   * Add a task to all the pending-task lists that it should be on. If readding is set, we are
   * re-adding the task so only include it in each list if it's not already there.
   * 将task加入到所有要加入的列表，如果是重新加入，那么只需要加入到还没加入果的列表
   */
  private def addPendingTask(index: Int, readding: Boolean = false) {
    // Utility method that adds `index` to a list only if readding=false or it's not already there
    def addTo(list: ArrayBuffer[Int]) {
      if (!readding || !list.contains(index)) {
        list += index
      }
    }

    for (loc <- tasks(index).preferredLocations) {            //perferloc是个队列
      for (execId <- loc.executorId) {    //executorId这个字段是option的，下面rack同理。for循环就可以简单处理option类型
        addTo(pendingTasksForExecutor.getOrElseUpdate(execId, new ArrayBuffer))  //根据preferloc的executorId，放到对应executor的任务队列
      }                                                                          //存在execId对应的任务队列，则把task放入，没有的话给相应的executor创建一个队列
      addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer))    //根据preferoloc的host，放到host队列
      for (rack <- sched.getRackForHost(loc.host)) {
        addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer))
      }
    }

    if (tasks(index).preferredLocations == Nil) {
      addTo(pendingTasksWithNoPrefs)
    }

    if (!readding) {
      allPendingTasks += index  // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   * 返回某个executor的tasks的列表或者是一个空的列表
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())   //列表中有executorId，返回对应的task列表，否则返回空的列表
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   * 从一个list列表中取出一个task并返回taskId，如果列表为空则返回None
   */
  private def findTaskFromList(execId: String, list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size                     //队列大小
    while (indexOffset > 0) {
      indexOffset -= 1                                        //取出一个
      val index = list(indexOffset)                           //返回相应taskId
      if (!executorIsBlacklisted(execId, index)) {           //如果不在黑名单中
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)              //从列表中移除一个task
        if (copiesRunning(index) == 0 && !successful(index)) {       //如果当前的task没有running和成功，返回taskId
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host
    * 检查是否一个task正在运行在一个host上
    * */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Is this re-execution of a failed task on an executor it already failed in before
   * EXECUTOR_TASK_BLACKLIST_TIMEOUT has elapsed ?
   * 是否当前的task已经失败
   */
  private def executorIsBlacklisted(execId: String, taskId: Int): Boolean = {
    if (failedExecutors.contains(taskId)) {              //是否失败的executors包含相应的taskId
      val failed = failedExecutors.get(taskId).get       //找到（executor，失败时间）


      return failed.contains(execId) &&            //在失败列表并且失败时间小于时间窗口
        clock.getTime() - failed.get(execId).get < EXECUTOR_TASK_BLACKLIST_TIMEOUT
    }

    false
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  private def findSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean =
      !hasAttemptOnHost(index, host) && !executorIsBlacklisted(execId, index)

    if (!speculatableTasks.isEmpty) {
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_.executorId)
        if (executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * 从一个节点上取出一个task，返回它的index和局部性级别，只要搜索符合局部性要求的tasks
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def findTask(execId: String, host: String, maxLocality: TaskLocality.Value)        //TaskLocality.Value是枚举类型
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    for (index <- findTaskFromList(execId, getPendingTasksForExecutor(execId))) {    //从executor对应的task列表中查找
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {        //如果满足host优先
      for (index <- findTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- findTaskFromList(execId, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- findTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {          //如果没有特别要求
      for (index <- findTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
    findSpeculativeTask(execId, host, maxLocality).map {                       //如果其它的takss都被调度了，找一个特别的？？？
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * 反馈一个executor发来的offer请求，从调度中找到一个task，由taskSetManager调用，
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   * 处理task相关的信息，本task的内容
   */
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (!isZombie) {       //task set manager becomes a zombie
      val curTime = clock.getTime()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {           //根据当前情况设定允许的局部性级别，级别越低，要求越严格
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          //允许的级别高，不用再找其它的了
          allowedLocality = maxLocality
        }
      }

      findTask(execId, host, allowedLocality) match {               //根据要求开始寻找task
        case Some((index, taskLocality, speculative)) => {           //找到一个task，返回一个task描述符
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.newTaskId()            //创建新的taskId？
          // Do various bookkeeping
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size        //已经尝试的次数
          val info = new TaskInfo(taskId, index, attemptNum, curTime,
            execId, host, taskLocality, speculative)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)          //将新的taskInfo加入task对应的尝试执行的列表中
          // Update our locality level for delay scheduling
          // NO_PREF will not affect the variables related to delay scheduling
          if (maxLocality != TaskLocality.NO_PREF) {
            currentLocalityIndex = getLocalityIndex(taskLocality)
            lastLaunchTime = curTime
          }
          // Serialize and return the task
          val startTime = clock.getTime()
          // We rely on the DAGScheduler to catch non-serializable closures and RDDs, so in here
          // we assume the task can be serialized without exceptions.
          val serializedTask = Task.serializeWithDependencies(            //序列化相应task
            task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          addRunningTask(taskId)                  //增加running task的个数

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo("Starting %s (TID %d, %s, %s, %d bytes)".format(
              taskName, taskId, host, taskLocality, serializedTask.limit))

          sched.dagScheduler.taskStarted(task, info)        //给dag发送任务开始消息
          return Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))   //返回task描述符
        }
        case _ =>
      }
    }
    None
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   * 根据调度延迟，决定task的调度级别
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    while (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex) &&
        currentLocalityIndex < myLocalityLevels.length - 1)
    {
      // Jump to the next locality level, and remove our waiting time for the current one since
      // we don't want to count it again on the next one
      //跳到下一个级别，删除当前的等待时间
      lastLaunchTime += localityWaits(currentLocalityIndex)
      currentLocalityIndex += 1
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  def handleTaskGettingResult(tid: Long) = {
    val info = taskInfos(tid)
    info.markGettingResult()    //标记获得结果的时间
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Marks the task as successful and notifies the DAGScheduler that a task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]) = {
    val info = taskInfos(tid)
    val index = info.index
    info.markSuccessful()
    removeRunningTask(tid)
    sched.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, info, result.metrics)
    if (!successful(index)) {
      tasksSuccessful += 1
//      logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
//        info.id, taskSet.id, info.taskId, info.duration, info.host, tasksSuccessful, numTasks))
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    failedExecutors.remove(index)
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   * 将task标记为failed，重新加入到pending列表中，并通知DAG Scheduler
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskEndReason) {
    val info = taskInfos(tid)
    if (info.failed) {
      return
    }
    removeRunningTask(tid)
    info.markFailed()
    val index = info.index
    copiesRunning(index) -= 1
    var taskMetrics : TaskMetrics = null

    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}): " +
      reason.asInstanceOf[TaskFailedReason].toErrorString
    reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // Not adding to failed executors for FetchFailed.
        isZombie = true

      case ef: ExceptionFailure =>
        taskMetrics = ef.metrics.orNull
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTime()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on executor ${info.host}: " +
            s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)

      case e: TaskEndReason =>
        logError("Unknown TaskEndReason: " + e)
    }
    // always add to failed executors
    failedExecutors.getOrElseUpdate(index, new HashMap[String, Long]()).
      put(info.executorId, clock.getTime())
    sched.dagScheduler.taskEnded(tasks(index), reason, null, null, info, taskMetrics)    //通知调度器这个task结束了
    addPendingTask(index)                 //添加到pending队列中
    if (!isZombie && state != TaskState.KILLED) {
      assert (null != failureReason)
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason))
        return
      }
    }
    maybeFinishTaskSet()
  }

  def abort(message: String) {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
    * 添加task到running tasks集合中，如果存在父调度管理器（pool or tsm，更新相应的执行任务数量）
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // Re-enqueue pending tasks for this host based on the status of the cluster. Note
    // that it's okay if we add a task to the same queue twice (if it had multiple preferred
    // locations), because findTaskFromList will skip already-running tasks.
    for (index <- getPendingTasksForExecutor(execId)) {
      addPendingTask(index, readding=true)
    }
    for (index <- getPendingTasksForHost(host)) {
      addPendingTask(index, readding=true)
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask]) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(tasks(index), Resubmitted, null, null, info, null)
        }
      }
    }
    // Also re-enqueue any tasks that were running on the node
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure)
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTime()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.size - 1))
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, 100)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get("spark.locality.wait", "3000")
    level match {
      case TaskLocality.PROCESS_LOCAL =>
        conf.get("spark.locality.wait.process", defaultWait).toLong
      case TaskLocality.NODE_LOCAL =>
        conf.get("spark.locality.wait.node", defaultWait).toLong
      case TaskLocality.RACK_LOCAL =>
        conf.get("spark.locality.wait.rack", defaultWait).toLong
      case _ => 0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   * 计算taskset中的局部性
   *
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def recomputeLocality() {
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}
