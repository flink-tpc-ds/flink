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

package org.apache.flink.table.planner.sources

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.JSet
import org.apache.flink.table.runtime.parquet.VectorizedColumnRowInputParquetFormat
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.sources._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.slf4j.{Logger, LoggerFactory}

/**
  * A [[StreamTableSource]] for Parquet files.
  *
  * @param filePath The path to the parquet file.
  * @param enumerateNestedFiles The flag to specify whether recursive traversal
  *                             of the input directory structure is enabled.
  */
class ParquetTableSource(
    schema: TableSchema,
    filePath: Path,
    enumerateNestedFiles: Boolean,
    numTimes: Int = 1,
    sourceName: String = "",
    uniqueKeySet: JSet[JSet[String]] = null,
    selectFields: Array[Int] = null)
  extends InputFormatTableSource[BaseRow]
  with ProjectableTableSource[BaseRow] {

  lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  private var cachedStats: Option[TableStats] = None

  protected var limit: Long = Long.MaxValue
  protected var filterPredicate: FilterPredicate = _

  override def projectFields(fields: Array[Int]): ParquetTableSource = {
    new ParquetTableSource(
      schema, filePath, enumerateNestedFiles, numTimes, sourceName, uniqueKeySet, fields)
  }

  private def selectFieldDataTypes(): Array[DataType] = {
    if (selectFields == null) {
      schema.getFieldDataTypes
    } else {
      selectFields.map(schema.getFieldDataTypes()(_))
    }
  }

  private def selectFieldTypes(): Array[LogicalType] = {
    selectFieldDataTypes().map(fromDataTypeToLogicalType)
  }

  private def selectFieldNames(): Array[String] = {
    if (selectFields == null) {
      schema.getFieldNames
    } else {
      selectFields.map(schema.getFieldNames()(_))
    }
  }

  override def getInputFormat: InputFormat[BaseRow, _] = {
    val inputFormat = new VectorizedColumnRowInputParquetFormat(
      filePath, selectFieldTypes(), selectFieldNames(), limit)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    inputFormat.asInstanceOf[InputFormat[BaseRow, _]]
  }

  override def getTableSchema: TableSchema = {
    schema
  }

  override def getProducedDataType: DataType = {
    DataTypes.ROW(selectFieldNames().zip(selectFieldDataTypes()).map{
      case (name, t) =>
        DataTypes.FIELD(name, t)
    }: _*).bridgedTo(classOf[BaseRow])
  }

  def getTableStats: TableStats = {
    cachedStats match {
      case Some(s) => s
      case _ =>
        val stats = try {
          ParquetTableStatsCollector.collectTableStats(
            filePath,
            enumerateNestedFiles,
            schema.getFieldNames,
            schema.getFieldDataTypes.map(fromDataTypeToLogicalType),
            filter = Option(filterPredicate),
            hadoopConf = None,
            maxThreads = None) // TODO get value from config
        } catch {
          case t: Throwable =>
            LOG.error(s"collectTableStats error: $t")
            null
        }
        cachedStats = Some(stats)
        stats
    }
  }
}
