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

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.sources.{CsvTableSource, CsvTableSource2}
import org.apache.flink.table.tpc.TpcUtils._
import org.apache.flink.test.util.TestBaseUtils

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util

@RunWith(classOf[Parameterized])
class TpcHBatchExecITCase(caseName: String) extends BatchTestBase {

  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/tpch/csv-data/$tableName/$tableName.tbl").getFile
  }

  @Before
  def prepareOp(): Unit = {
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
      lazy val builder = CsvTableSource.builder()
        .path(getDataFile(tableName))
        .fieldDelimiter("|")
        .lineDelimiter("\n")
      schema.getFieldNames.zip(schema.getFieldTypes).foreach {
        case (fieldName, fieldType) =>
          builder.field(fieldName, TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(fieldType))
      }

      lazy val tableSource = builder.build()
      tEnv.registerTableSource(tableName, tableSource)
    }

    tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)
    /*TpcUtils.disableRangeSort(tEnv)*/
  }

  def execute(caseName: String): Unit = {
    val table = parseQuery(getTpcHQuery(caseName))
    val resultSeq = executeQuery(table)
    // System.out.println(resultSeq.size)
    val result = formatResult(resultSeq)
    // System.out.println(explainLogical(table))
    TestBaseUtils.compareResultAsText(result, getTpcHResult(caseName))
  }

  @Test
  def test(): Unit = {
    execute(caseName)
  }
}

object TpcHBatchExecITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[String] = {
    // 15 plan: VIEW is unsupported
    util.Arrays.asList(
      "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
      "11", "12", "13", "14", "15_1", "16", "17", "18", "19",
      "20", "21", "22"
    )
  }
}
