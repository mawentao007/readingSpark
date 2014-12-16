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

package org.apache.spark

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.{HashSet, HashMap, Map}
import scala.concurrent.Await

import akka.actor._
import akka.pattern.ask

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

/** Actor class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterActor(tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends Actor with ActorLogReceive with Logging {
  val maxAkkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  override def receiveWithLogging = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = sender.path.address.hostPort        //发送者的port
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      val mapOutputStatuses = tracker.getSerializedMapOutputStatuses(shuffleId)
      val serializedSize = mapOutputStatuses.size
      if (serializedSize > maxAkkaFrameSize) {
        val msg = s"Map output statuses were $serializedSize bytes which " +
          s"exceeds spark.akka.frameSize ($maxAkkaFrameSize bytes)."

        /* For SPARK-1244 we'll opt for just logging an error and then throwing an exception.
         * Note that on exception the actor will just restart. A bigger refactoring (SPARK-1239)
         * will ultimately remove this entire code path. */
        val exception = new SparkException(msg)
        logError(msg, exception)
        throw exception
      }
      sender ! mapOutputStatuses

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerActor stopped!")
      sender ! true
      context.stop(self)
  }
}

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and worker) use different HashMap to store its metadata.
 * 跟踪一个stage的输出。
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  private val timeout = AkkaUtils.askTimeout(conf)

  /** Set to the MapOutputTrackerActor living on the driver. */
  //这个driver上面活跃的MapOutputTrackerActor
  var trackerActor: ActorRef = _

  /**
   * This HashMap has different behavior for the master and the workers.
   *
   * On the master, it serves as the source of map outputs recorded from ShuffleMapTasks.
   * 在master端，记录shuffleMapTasks的输出结果，作为map outputs记录的数据源
   * On the workers, it simply serves as a cache, in which a miss triggers a fetch from the
   * master's corresponding HashMap.
   * 在worker端作为缓存
   */
  protected val mapStatuses: Map[Int, Array[MapStatus]]      //shuffleid到MapStatus的映射

  /**
   * Incremented every time a fetch fails so that client nodes know to clear
   * their cache of map output locations if this happens.
   * 每次fetch失败的时候都会改变，客户端节点就知道自己缓存的map output locations已经失效
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /** Remembers which map output locations are currently being fetched on a worker.
    * 在一个worker上，记录是哪一个输出位置正在被获取
    * */
  private val fetching = new HashSet[Int]     //内容是shuffleId

  /**
   * Send a message to the trackerActor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   * 发送消息给trackerActor，在超时时间内获取它的结果
   */
  protected def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      Await.result(future, timeout)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerActor, to which we expect it to reply with true.
    * 发送一个单向消息给trackerActor
    * */
  protected def sendTracker(message: Any) {
    val response = askTracker(message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  /**
   * Called from executors to get the server URIs and output sizes of the map outputs of
   * a given shuffle.
   * executors调用，获得server 资源地址，一个给定shuffle的map输出的大小
   */
  def getServerStatuses(shuffleId: Int, reduceId: Int): Array[(BlockManagerId, Long)] = {
    val statuses = mapStatuses.get(shuffleId).orNull     //查看shuffleId相应的mapStatus是否存在
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {             //fetching包含正在fetching的shuffleId
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {  //fetching包含其它的shuffleid，说明正在进行fetching，我们不能进入，只能等待

            try {
              fetching.wait()
            } catch {
              case e: InterruptedException =>
            }
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        //或者在等待的时候fetch成功，或者有人已经fetch到了
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.我们必须进行fetch，让其他人等
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the output locs; do so 成功的进行fetch
        logInfo("Doing the fetch; tracker actor = " + trackerActor)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes =                 //fetch的实际的过程，获得字节队列
            askTracker(GetMapOutputStatuses(shuffleId)).asInstanceOf[Array[Byte]]
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId  //释放掉fetching权限
            fetching.notifyAll()
          }
        }
      }
      if (fetchedStatuses != null) {
        fetchedStatuses.synchronized {
          return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, fetchedStatuses)
        }
      } else {
        throw new MetadataFetchFailedException(
          shuffleId, reduceId, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      statuses.synchronized {                                     //将mapStatus转换为[(BlockManagerId, Long)]
        return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
      }
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each worker task calls this with the latest epoch
   * number on the master at the time it was created.
   * 更新epoch号，清除因为fetch失败而产生的旧的输出。每个worker task更新的时候都是用被创建时master端最新的数据
   */
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
  }

  /** Stop the tracker. */
  def stop() { }
}

/**
 * MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map
 * output information, which allows old output information based on a TTL.
 * driver端的MapOutputTracker 时间戳哈希，记录map output信息
 */
private[spark] class MapOutputTrackerMaster(conf: SparkConf)
  extends MapOutputTracker(conf) {

  /** Cache a serialized version of the output statuses for each shuffle to send them out faster */
  private var cacheEpoch = epoch     //版本号，记录每个shuffle的输出状态，是发送速度加快

  /**
   * Timestamp based HashMap for storing mapStatuses and cached serialized statuses in the master,
   * so that statuses are dropped only by explicit de-registering or by TTL-based cleaning (if set).
   * Other than these two scenarios, nothing should be dropped from this HashMap.
   * 时间戳哈系映射，用来存储map状态，缓存序列化状态
   */
  protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]()   //序列化的mapstatus信息
  private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]()   //时间戳映射，保存序列化的mapstatus信息

  // For cleaning up TimeStampedHashMaps
  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.MAP_OUTPUT_TRACKER, this.cleanup, conf)   //清除TimeStampedHashMaps

  def registerShuffle(shuffleId: Int, numMaps: Int) {               //将相应的shuffle放入时间戳映射表
    if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val array = mapStatuses(shuffleId)   //shuffleId对应的mapstatus队列
    array.synchronized {
      array(mapId) = status          //队列中添加相应的map的状态
    }
  }

  /** Register multiple map output information for the given shuffle
    * 给某个特定的shuffle注册多个输出消息
    * */
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager
    * 给某个shuffle解除注册map输出消息
    * */
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data
    * 解除注册shuffle
    * */
  override def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
  }

  /** Check if the given shuffle is being tracked
    * 检查特定的shuffle是否被追踪
    * */
  def containsShuffle(shuffleId: Int): Boolean = {
    cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {       //获得已经序列化的shuffle对应的mapStatus
    var statuses: Array[MapStatus] = null
    var epochGotten: Long = -1
    epochLock.synchronized {
      if (epoch > cacheEpoch) {
        cachedSerializedStatuses.clear()
        cacheEpoch = epoch
      }
      cachedSerializedStatuses.get(shuffleId) match {    //从时间戳哈希表中获得相应的shuffle
        case Some(bytes) =>
          return bytes
        case None =>
          statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
          epochGotten = epoch
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "statuses"; let's serialize and return that
    val bytes = MapOutputTracker.serializeMapStatuses(statuses)
    logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
    // Add them into the table only if the epoch hasn't changed while we were working
    epochLock.synchronized {
      if (epoch == epochGotten) {
        cachedSerializedStatuses(shuffleId) = bytes
      }
    }
    bytes
  }

  override def stop() {
    sendTracker(StopMapOutputTracker)
    mapStatuses.clear()
    trackerActor = null
    metadataCleaner.cancel()
    cachedSerializedStatuses.clear()
  }

  private def cleanup(cleanupTime: Long) {
    mapStatuses.clearOldValues(cleanupTime)
    cachedSerializedStatuses.clearOldValues(cleanupTime)
  }
}

/**
 * MapOutputTracker for the workers, which fetches map output information from the driver's
 * MapOutputTrackerMaster.
 * worker端，从驱动端MapOutputTrackerMaster获得map输出消息
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  protected val mapStatuses = new HashMap[Int, Array[MapStatus]]
}

private[spark] object MapOutputTracker {
  private val LOG_BASE = 1.1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  //将一组map output位置序列化为更高效的格式，用来发送到reduce 的task
  def serializeMapStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    // Since statuses can be modified in parallel, sync on it
    statuses.synchronized {
      objOut.writeObject(statuses)
    }
    objOut.close()
    out.toByteArray
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {          //返回一列MapStatus结果
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    objIn.readObject().asInstanceOf[Array[MapStatus]]
  }

  // Convert an array of MapStatuses to locations and sizes for a given reduce ID. If
  // any of the statuses is null (indicating a missing location due to a failed mapper),
  // throw a FetchFailedException.
  //转换一组mapstatuses为位置信息，给特定的reduceid。如果任何statuses是空，说明这个mapper失败了
  private def convertMapStatuses(
      shuffleId: Int,
      reduceId: Int,
      statuses: Array[MapStatus]): Array[(BlockManagerId, Long)] = {
    assert (statuses != null)
    statuses.map {
      status =>
        if (status == null) {
          throw new MetadataFetchFailedException(
            shuffleId, reduceId, "Missing an output location for shuffle " + shuffleId)
        } else {
          (status.location, decompressSize(status.compressedSizes(reduceId)))
        }
    }
  }

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}
