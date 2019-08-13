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

package org.apache.flink.table.runtime.parquet;

import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;

import org.apache.parquet.io.api.Binary;

/**
 * Interface for value decoding that supports vectorized (aka batched) decoding.
 */
public interface VectorizedValuesReader {

	boolean readBoolean();

	byte readByte();

	int readInteger();

	long readLong();

	float readFloat();

	double readDouble();

	Binary readBinary(int len);

	/*
	 * Reads `total` values into `c` start at `c[rowId]`
	 */
	void readBooleans(int total, HeapBooleanVector c, int rowId);

	void readBytes(int total, HeapByteVector c, int rowId);

	void readIntegers(int total, HeapIntVector c, int rowId);

	void readLongs(int total, HeapLongVector c, int rowId);

	void readFloats(int total, HeapFloatVector c, int rowId);

	void readDoubles(int total, HeapDoubleVector c, int rowId);

	void readBinaries(int total, HeapBytesVector c, int rowId);
}
