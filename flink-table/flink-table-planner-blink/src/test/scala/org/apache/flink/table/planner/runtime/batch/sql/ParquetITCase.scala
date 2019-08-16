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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.sinks.ParquetTableSink

import org.junit._

import java.nio.file.Files

class ParquetITCase extends BatchTestBase {

  @Test
  def testParquetTableSinkOverWrite():Unit = {
    // write
    val parquetTable1 = CommonParquetTestData.getParquetTableSource
    tEnv.registerTableSource("parquetTable1", parquetTable1)
    val tempFile = Files.createTempFile("parquet-sink", "test")
    tempFile.toFile.deleteOnExit()

    val sink = new ParquetTableSink(
      parquetTable1.getTableSchema, tempFile.toFile.getAbsolutePath, Some(WriteMode.OVERWRITE))
    tEnv.registerTableSink("mySink", sink)

    tEnv.sqlUpdate("INSERT INTO mySink SELECT `first`, id, score, `last` FROM parquetTable1")
    tEnv.execute("")
  }
}
