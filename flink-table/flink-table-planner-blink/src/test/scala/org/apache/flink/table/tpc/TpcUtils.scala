/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.tpc

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.text.DecimalFormat
import java.util
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._

object TpcUtils {

  def getTpcHQuery(caseName: String): String = {
    TpcUtils.resourceToString(s"/tpch/queries/$caseName.sql")
  }

  def getTpcHResult(caseName: String): String = {
    resourceToString(s"/tpch/result/${caseName.replace("_1", "")}.out")
  }

  def getTpcDsQuery(caseName: String, factor: Int): String = {
    TpcUtils.resourceToString(s"/tpcds/queries/$factor/$caseName.sql")
  }

  def resourceToString(resource: String): String = {
    scala.io.Source.fromFile(new File(getClass.getResource(resource).getFile)).mkString
  }

  def formatResult(result: Seq[Row]): JList[String] = {
    result.map(row => {
      val list = new JArrayList[Any]()
      for (i <- 0 until row.getArity) {
        val v = row.getField(i)
        val newV = v match {
          case b: JBigDecimal => new DecimalFormat("0.0000").format(b)
          case d: java.lang.Double => new DecimalFormat("0.0000").format(d)
          case _ => v
        }
        list.add(newV)
      }
      list.toString
    })
  }

  def disableRangeSort(tEnv: TableEnvironment): Unit = {
    val conf = tEnv.getConfig
    conf.getConfiguration.setBoolean("sql.exec.sort.range.enabled", false)
  }
}

trait Schema {

  def getFieldNames: Array[String]

  def getFieldTypes: Array[DataType]

  def getUniqueKeys: util.Set[util.Set[String]] = null
}

trait TpchSchema extends Schema {

}
