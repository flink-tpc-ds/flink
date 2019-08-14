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

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.dataformat.GenericRow.of
import org.apache.flink.table.planner.sources.ParquetTableSource
import org.apache.flink.table.runtime.parquet.RowParquetOutputFormat
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType

import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.io.File
import java.lang.{Double => JDouble, Integer => JInt}

object CommonParquetTestData {

  def getParquetTableSource: ParquetTableSource = {
    val records = Seq(
      of(fromString("Mike"), 1: JInt, 12.3d: JDouble, fromString("Smith")),
      of(fromString("Bob"), 2: JInt, 45.6d: JDouble, fromString("Taylor")),
      of(fromString("Sam"), 3: JInt, 7.89d: JDouble, fromString("Miller")),
      of(fromString("Peter"), 4: JInt, 0.12d: JDouble, fromString("Smith")),
      of(fromString("Liz"), 5: JInt, 34.5d: JDouble, fromString("Williams")),
      of(fromString("Sally"), 6: JInt, 6.78d: JDouble, fromString("Miller")),
      of(fromString("Alice"), 7: JInt, 90.1d: JDouble, fromString("Smith")),
      of(fromString("Kelly"), 8: JInt, 2.34d: JDouble, fromString("Williams"))
    )

    val names = Array("first", "id", "score", "last")
    val types: Array[DataType] = Array(
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.STRING
    )
    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetTableSource(
      TableSchema.builder().fields(names, types).build(),
      new Path(tempFilePath),
      true
    )
  }

  private def writeToTempFile(
                               contents: Seq[GenericRow],
                               fieldTypes: Array[DataType],
                               fieldNames: Array[String],
                               filePrefix: String,
                               fileSuffix: String): String = {
    writeToTempFile(contents, fieldTypes, fieldNames, filePrefix, fileSuffix, false)
  }

  private def writeToTempFile(
      contents: Seq[GenericRow],
      fieldTypes: Array[DataType],
      fieldNames: Array[String],
      filePrefix: String,
      fileSuffix: String,
      enableDictionary: Boolean
  ): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.delete()
    val outFormat = new RowParquetOutputFormat(
      tempFile.getAbsolutePath,
      fieldTypes.map(fromDataTypeToLogicalType), fieldNames,
      CompressionCodecName.UNCOMPRESSED, 128 * 1024 * 1024, true)
    outFormat.open(1, 1)
    contents.foreach(outFormat.writeRecord(_))
    outFormat.close()
    tempFile.deleteOnExit()
    tempFile.getAbsolutePath
  }
}
