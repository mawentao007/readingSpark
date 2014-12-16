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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation. 创建aggregation的初始值
 * @param mergeValue function to merge a new value into the aggregation result.  将一个新值merge到aggregation中
 * @param mergeCombiners function to merge outputs from multiple mergeValue function. 将多个mergeValue的输出merge
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  private val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K,C]               //创建map
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {         //这是一个lambda函数
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)    //如果有值，根据函数来合并值，没有的话创建一个
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)  //根据更新函数来改变数据
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
      : Iterator[(K, C)] =
  {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K,C]     //创建一个映射表，利用这个映射表和combiner函数，根据key将iter中的元素进行合并，返回一个合并后的。
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeCombiners(oldValue, kc._2) else kc._2    //如果有combiner，合并，如果没有，还用原来的
      }
      while (iter.hasNext) {       //从iter里面取出一个，根据key来合并
        kc = iter.next()
        combiners.changeValue(kc._1, update)      //这里很绕，update访问一个局部变量，但是在另一个函数中被调用，这时这个变量已经不在调用时的
                                                //上下文，那么相关变量依旧可以访问并且被使用到？
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      while (iter.hasNext) {
        val pair = iter.next()
        combiners.insert(pair._1, pair._2)
      }
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }
}
