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

package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.sinks.{CollectRowTableSink, CollectTableSink}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID

import java.util.{UUID, ArrayList => JArrayList, List => JList}

object CollectResultUtil {

  def collect(tEnv: TableEnvironment, table: Table, jobName: String): JList[Row] = {
    val schema = table.getSchema
    val fieldNames = schema.getFieldNames
    val fieldTypes = schema.getFieldDataTypes.map {
      t => TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo(t.getLogicalType)
    }

    val sink = new CollectRowTableSink()
      .configure(fieldNames, fieldTypes)
      .asInstanceOf[CollectTableSink[Row]]

    val execConfig = tEnv.asInstanceOf[TableEnvironmentImpl]
      .getPlanner.asInstanceOf[PlannerBase].getExecEnv.getConfig
    val typeSerializer = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[Row]]
      .createSerializer(execConfig)

    val id = new AbstractID().toString
    sink.init(typeSerializer, id)
    val sinkName = UUID.randomUUID().toString
    tEnv.registerTableSink(sinkName, sink)
    tEnv.insertInto(table, sinkName)

    val res = tEnv.execute(jobName)
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer)
  }

}
