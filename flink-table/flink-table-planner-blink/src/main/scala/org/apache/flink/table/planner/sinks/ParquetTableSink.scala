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
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.runtime.parquet.RowParquetOutputFormat
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, TableSink, TableSinkBase}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ParquetTableSink( dir: String,
                        writeMode: WriteMode,
                        compression: CompressionCodecName = CompressionCodecName.UNCOMPRESSED,
                        blockSize:Int,
                        supportUpdate: Boolean = false
                      )
    extends TableSinkBase[Row]
      with BatchTableSink[Row]
      with AppendStreamTableSink[Row]{

  /** Emits the DataSet. */
  override def emitDataSet(dataSet: DataSet[Row]): Unit = {
    val outputFormat = new RowParquetOutputFormat(dir,
      TypeConversions.fromDataToLogicalType(getTableSchema.getFieldDataTypes),
      getFieldNames, compression,blockSize,supportUpdate)

    dataSet.output(outputFormat.asInstanceOf[OutputFormat[Row]])
  }
  /** Return a deep copy of the [[org.apache.flink.table.sinks.TableSink]]. */
  override protected def copy: TableSinkBase[Row] = {
    val sink = new ParquetTableSink(dir, writeMode, compression, blockSize,supportUpdate)
    sink
  }

  /**
    * Consumes the DataStream and return the sink transformation {@link DataStreamSink}.
    * The returned {@link DataStreamSink} will be used to set resources for the sink operator.
    */
  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    val a = 1
    val outputFormat = new RowParquetOutputFormat(dir,
      TypeConversions.fromDataToLogicalType(getTableSchema.getFieldDataTypes),
      getFieldNames, compression,blockSize,supportUpdate)

    outputFormat
    outputFormat.open(1, 1)
    dataStream.writeUsingOutputFormat(outputFormat.asInstanceOf[OutputFormat[Row]])
      .name("parquet sink:" + dir)
  }

  /**
    * Emits the DataStreams
    */
  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    consumeDataStream(dataStream)
  }

  /**
    * return the output typeInformation
    * @return
    */
  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes,getFieldNames)
  }

}
