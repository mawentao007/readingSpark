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

package org.apache.spark.storage

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.ShuffleBlockManager.ShuffleFileGroup
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.executor.ShuffleWriteMetrics

/** A group of writers for a ShuffleMapTask, one writer per reducer.
  * 代表一个ShuffleMapTask的一组接口，每个reducer一个writer。
  * */
private[spark] trait ShuffleWriterGroup {
  val writers: Array[BlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * 作为一个优化，用来减少物理shuffle的文件生成，多个shuffle块被聚合成同一个文件。每个reducer有一个文件，并发执行shuffle task
 * 当一个task完成写shuffle文件，它释放该文件给其他task
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.     //一个shuffle stage的唯一的标志
 *   - bucketId: The id of the output partition (i.e., reducer id)     //输出的块的id
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 *       一组合并文件唯一拥有的，一个时间内只有一个task拥有fileId，当这个task完成时，这个fileId被返回给pool
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 * 每个shuffle file最后被合并到一个文件片。
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 * shuffle file的元数据用一种高效的方式存储。比起直接将shuffleblockids映射到文件快，每个shufflefilegroup维护一个偏移列表，给存储在每个文件中的块
 * 在寻找相应shuffle块的时候，我们查找相应的文件。
 */
// TODO: Factor this into a separate class for each ShuffleManager implementation
private[spark]
class ShuffleBlockManager(blockManager: BlockManager,
                          shuffleManager: ShuffleManager) extends Logging {
  def conf = blockManager.conf

  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  //关闭这个特性可以是每个shuffle Block是有自己的文件
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  val consolidateShuffleFiles =
    conf.getBoolean("spark.shuffle.consolidateFiles", false)

  // Are we using sort-based shuffle?
  val sortBasedShuffle = shuffleManager.isInstanceOf[SortShuffleManager] //查看是否是sort shuffle实例

  private val bufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
   * 包含一个shuffle相关的所有的状态。包括没有用的ShuffleFileGroups池，所有的ShuffleFileGroups，给这个shuffle创建的
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()   //未使用的shuffle文件组
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()        //已经创建的所有文件的组

    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     * 这个shuffle的所有的在当前的executor上已经完成的map tasks
     * NB: This is only populated if consolidateShuffleFiles is FALSE. We don't need it otherwise.
     * 只有联合文件的标记为假的时候才有用
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  type ShuffleId = Int
  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]     //shuffleId到shuffleState的映射

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)     //清理元数据

  /**
   * Register a completed map without getting a ShuffleWriterGroup. Used by sort-based shuffle
   * because it just writes a single file by itself.
   * 注册一个完成的map，不用获得ShuffleWriterGroup。sort-basedshuffle来使用，因为它的每个map都要写一个独立的文件
   *
   */
  def addCompletedMap(shuffleId: Int, mapId: Int, numBuckets: Int): Unit = {
    shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))   //一个shuffleId，对应一组sst
    val shuffleState = shuffleStates(shuffleId)
    shuffleState.completedMapTasks.add(mapId)    //取出一个shuffleId对应的sst，将完成的mapId添加进去
  }

  /**
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
   * 给一个map任务获取一个ShuffleWriterGroup，当writers完成的时候会被注册为成功。
   */
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,   //partition个数就是numBuckets
      writeMetrics: ShuffleWriteMetrics) = {
    new ShuffleWriterGroup {                                  //创建了一个ShuffleWriterGroup，返回值就是这个group
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))        //创建当前shuffleId的shuffleState并放入
      private val shuffleState = shuffleStates(shuffleId)
      private var fileGroup: ShuffleFileGroup = null

      val writers: Array[BlockObjectWriter] = if (consolidateShuffleFiles) {  //返回BlockObjectWriter队列
        fileGroup = getUnusedFileGroup()
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>              //tabulate，返回一个数组，numBuckets个，值是后面函数计算的
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)  //bucketId就是partitionId；block是根据参数简单合并生成
          blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializer, bufferSize,
            writeMetrics)                    //对于每个bucket，都有一个writer，负责往里面写数据，还能计算出大小，就是mapstatus中的size之一
        }
      } else {                  //对于不能合并文件的情况，给每个block一个文件
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)    //生成shuffleBlockId，很简单，利用参数连接即可
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          // Because of previous failures, the shuffle file may already exist on this machine.
          // If so, remove it.
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize, writeMetrics)
        }
      }

      override def releaseWriters(success: Boolean) {        //释放writer
        if (consolidateShuffleFiles) {
          if (success) {
            val offsets = writers.map(_.fileSegment().offset)    //返回一组偏移
            val lengths = writers.map(_.fileSegment().length)
            fileGroup.recordMapOutput(mapId, offsets, lengths)
          }
          recycleFileGroup(fileGroup)
        } else {
          shuffleState.completedMapTasks.add(mapId)
        }
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()          //从shuffleState中去除一个没用过的fileGroups，如果没有空的，则创建一个。
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        val files = Array.tabulate[File](numBuckets) { bucketId =>
          val filename = physicalFileName(shuffleId, bucketId, fileId)
          blockManager.diskBlockManager.getFile(filename)
        }
        val fileGroup = new ShuffleFileGroup(fileId, shuffleId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   * 返回BlockId所在的物理文件段，必须在支持文件consolidation的条件下才有用
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all file groups associated with this shuffle.
    val shuffleState = shuffleStates(id.shuffleId)
    for (fileGroup <- shuffleState.allFileGroups) {
      val segment = fileGroup.getFileSegmentFor(id.mapId, id.reduceId)
      if (segment.isDefined) { return segment.get }
    }
    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle.
    * 移除所有的和某个shuffle相关的blocks/files和元数据*/
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle.
    * 移除所有blocks /files 和一个特定shuffle相关的，由removeShuffle来调用 */
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        if (sortBasedShuffle) {     //如果是sortBased的Shuffle，那么每个map有一个唯一的blockId，和一个索引文件
          // There's a single block ID for each map, plus an index file for it
          for (mapId <- state.completedMapTasks) {           //已经完成的map，会产生相应的输出，要将他们获取并删除
            val blockId = new ShuffleBlockId(shuffleId, mapId, 0)
            blockManager.diskBlockManager.getFile(blockId).delete()
            blockManager.diskBlockManager.getFile(blockId.name + ".index").delete()
          }
        } else if (consolidateShuffleFiles) {            //一个shuffleState对应一个shuffle，删除其中的所有文件组中的文件
          for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
            file.delete()
          }
        } else {
          for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
            blockManager.diskBlockManager.getFile(blockId).delete()
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  def stop() {
    metadataCleaner.cancel()
  }
}

private[spark]
object ShuffleBlockManager {
  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   * 一组shuffle 文件，每个reducer一个
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) { //放在伙伴对象中可以被其它文件访问
    private var numBlocks: Int = 0

    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     * 记录一个group中的每个mapId的绝对索引
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets and lengths of blocks into each reducer file, ordered by
     * position in the file.
     * 保存块到每个reducer的连续的偏移量和长度，根据文件的位置
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }
    private val blockLengthsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def apply(bucketId: Int) = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long], lengths: Array[Long]) {    //记录map的输出
      assert(offsets.length == lengths.length)
      mapIdToIndex(mapId) = numBlocks        //为何对应的是块数
      numBlocks += 1
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
        blockLengthsByReducer(i) += lengths(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists.
      * 返回和给定map task关联的文件段
      * */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val blockLengths = blockLengthsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length = blockLengths(index)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
