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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

private[spark]
class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)        //参数为rdd和func
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions    //选择第一个依赖的partitions,mappedRdd的partition个数一定不会
                                                                              //比父rdd多，否则就会出问题（一个父partition被多个子partition利用）

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(f)                    //先进入iterator，对父rdd进行计算,因为这是个mapped rdd，实际上以来的只有一个
                                                                      //rdd，如果有多个就不是mappedRdd了，firstParent就是为了简单
                                                                      //否则就要写dep.head...
                                                                      //返回值类型为iterator
}
