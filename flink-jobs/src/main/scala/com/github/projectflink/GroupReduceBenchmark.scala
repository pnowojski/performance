/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.projectflink

import java.lang

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

object GroupReduceBenchmarkGenerateData {
  val rnd = new Random(System.currentTimeMillis())

  private final def skewedSample(skew: Double, max: Long): Long = {
    val uniform = rnd.nextDouble()
    val `var` = Math.pow(uniform, skew)
    val pareto = 0.2 / `var`
    val `val` = pareto.toInt
    if (`val` > max) `val` % max else `val`
  }

  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: [master] [dop] [numCountries] [numBooks] [numReads] [skew] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var numCountries = args(2).toLong
    var numBooks = args(3).toInt
    var numReads = args(4).toLong
    var skew = args(5).toFloat
    val outputPath = if (args.length > 6) {
      args(6)
    } else {
      null
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(dop)

    val countryIds = env.generateSequence(0, numCountries - 1)
    val bookIds = env.generateSequence(0, numBooks - 1)

    val countryNames = countryIds map { id => (id, "No. " + id + " The awesome country") }
    val bookNames = bookIds map { id => (id, "No. " + id + " This is the most special song") }

    val reads = env.generateSequence(0, numReads - 1) map {
      i =>
        (skewedSample(skew, numCountries - 1), rnd.nextInt(numBooks).toLong)
    }

    val readsWithCountry = reads.joinWithTiny(countryNames).where("_1").equalTo("_1") {
      (left, right) =>
        (right._2, left._2)
    }

    val readsWithCountryAndBook = readsWithCountry.join(bookNames, JoinHint.REPARTITION_HASH_SECOND)
      .where("_2")
      .equalTo("_1") {
      (left, right) =>
        (left._1, right._2)
    }.rebalance()



    if (outputPath == null) {
      readsWithCountryAndBook.print()
    } else {
      readsWithCountryAndBook.writeAsCsv(outputPath + s"$numCountries-$numBooks-$numReads-$skew",
        rowDelimiter = "\n",
        fieldDelimiter = ",",
        writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Data Generator")
  }
}

object GroupReduceBenchmarkFlink {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: [master] [dop] [k] [input] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var k = args(2).toInt
    var inputPath = args(3)
    val outputPath = if (args.length > 4) {
      args(4)
    } else {
      null
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => (in._1, in._2, 1L) }
      .groupBy("_2", "_1")
      .sum("_3")
      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
      .first(k)
      .groupBy("_1")
      .reduceGroup {
      in =>
        val it = in.toIterator.buffered
        val first = it.head
        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink")
//    println(env.getExecutionPlan())
  }
}

object GroupReduceBenchmarkFlinkHashCombine {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: [master] [dop] [k] [input] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var k = args(2).toInt
    var inputPath = args(3)
    val outputPath = if (args.length > 4) {
      args(4)
    } else {
      null
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => (in._1, in._2, 1L) }
      .mapPartition(new HashAggregator("COMBINER"))
      .partitionByHash("_2", "_1")
      .groupBy("_2", "_1")
      .sum("_3")
      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
      .first(k)
      .groupBy("_1")
      .reduceGroup {
      in =>
        val it = in.toIterator.buffered
        val first = it.head
        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink (HashCombine)")
    //        println(env.getExecutionPlan())
  }
}

object GroupReduceBenchmarkFlinkHash {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: [master] [dop] [k] [input] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var k = args(2).toInt
    var inputPath = args(3)
    val outputPath = if (args.length > 4) {
      args(4)
    } else {
      null
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => (in._1, in._2, 1L) }
      .mapPartition(new HashAggregator("COMBINER"))
      .partitionByHash("_2", "_1")
      .mapPartition(new HashAggregator("COMBINER"))
      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
      .first(k)
      .groupBy("_1")
      .reduceGroup {
      in =>
        val it = in.toIterator.buffered
        val first = it.head
        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink (HashCombine)")
//        println(env.getExecutionPlan())
  }
}

class HashAggregator(combine: String)
  extends RichMapPartitionFunction[(String, String, Long), (String, String, Long)] {

  private var aggregate: mutable.HashMap[(String, String), Long] = null

  override def open(config: Configuration) {
    aggregate = mutable.HashMap[(String, String), Long]()
  }

  override def mapPartition(
      records: lang.Iterable[(String, String, Long)],
      out: Collector[(String, String, Long)]): Unit = {
    val it = records.iterator
    while (it.hasNext) {
      val value = it.next()
      val key = (value._1, value._2)
      val previous = aggregate.getOrElse(key, 0L)
      aggregate.put(key, previous + value._3)
    }
    val outIt = aggregate.iterator

    while (outIt.hasNext) {
      val value = outIt.next()
      out.collect((value._1._1, value._1._2, value._2))
    }
  }
}

class HashGrouper(k: Int)
  extends RichMapPartitionFunction[(String, String, Long), (String, String)] {

  private var groups: mutable.HashMap[String, mutable.MutableList[(String, Long)]] = null

  override def open(config: Configuration) {
    groups = mutable.HashMap[String, mutable.MutableList[(String, Long)]]()
  }

  override def mapPartition(
      records: lang.Iterable[(String, String, Long)],
      out: Collector[(String, String)]): Unit = {
    val it = records.iterator()
    while (it.hasNext) {
      val value = it.next()
      val list = groups.getOrElse(value._1, mutable.MutableList[(String, Long)]())
      list += ((value._2, value._3))
      groups.put(value._1, list)
    }

    val listIt = groups.iterator
    while (listIt.hasNext) {
      val value = listIt.next()
      val key = value._1
      val list = value._2
      val sorted = list.sortBy(- _._2)
//      println("SORTED " + sorted.take(5))
      out.collect((key, sorted.take(k).mkString(", ") ))
    }
  }
}


