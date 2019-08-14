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
package org.apache.flink.table.planner.sinks

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.Test

import scala.collection.Seq

class ParquetTableSinkTest extends BatchTestBase{
  lazy val data = Seq(
    row(1, 2.0d),
    row(1, 2.0d),
    row(2, 1.0d),
    row(2, 1.0d),
    row(3, 3.0d)
  )

  @Test
  def testParquetSink(): Unit ={
    registerCollection("MyTable",data,
      new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO),"a,b")
    val table = tEnv.sqlQuery("select a,b from MyTable")

//    val sink = new CsvTableSink("/tmp/test.txt",",",1,WriteMode.OVERWRITE);
    val sink = new ParquetTableSink("/tmp/", WriteMode.OVERWRITE,
      CompressionCodecName.SNAPPY,1024,true)
    val schema = table.getSchema
    val configuredSink = sink.configure(schema.getFieldNames,schema.getFieldTypes)
    tEnv.registerTableSink("parquet_sink",configuredSink)

    table.insertInto("parquet_sink")
    tEnv.execute("test")


  }
}
