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

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.parquet.RowParquetOutputFormat
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.sinks.OutputFormatTableSink
import org.apache.flink.table.types.DataType

import org.apache.hadoop.fs.FileUtil
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.io.File

/**
  * A subclass of [[OutputFormatTableSink]] to write [[BaseRow]] to Parquet files.
  *
  * @param dir         The output path to write the Table to.
  * @param writeMode   The write mode to specify whether existing files are overwritten or not.
  * @param compression compression algorithms.
  */
class ParquetTableSink(
    schema: TableSchema,
    dir: String,
    writeMode: Option[WriteMode] = None,
    compression: CompressionCodecName = CompressionCodecName.UNCOMPRESSED,
    blockSize: Int = 128 * 1024 * 1024,
    enableDictionary: Boolean = true)
    extends OutputFormatTableSink[BaseRow] {

  override def getConsumedDataType: DataType = schema.toRowDataType.bridgedTo(classOf[BaseRow])

  override def getOutputFormat: OutputFormat[BaseRow] = createOutputFormat()

  def createOutputFormat(): RowParquetOutputFormat = {
    writeMode match {
      case Some(wm) if wm == WriteMode.OVERWRITE =>
        FileUtil.fullyDelete(new File(dir))
      case _ =>
        val path = new Path(dir)
        if (path.getFileSystem.exists(path) && !path.getFileSystem.getFileStatus(path).isDir) {
          throw new RuntimeException( "output dir [" + dir + "] already existed.")
        }
    }
    new RowParquetOutputFormat(
      dir, schema.getFieldDataTypes.map(fromDataTypeToLogicalType),
      schema.getFieldNames, compression, blockSize, enableDictionary)
  }

  override def configure(
      fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): ParquetTableSink = this

  override def getTableSchema: TableSchema = schema
}
