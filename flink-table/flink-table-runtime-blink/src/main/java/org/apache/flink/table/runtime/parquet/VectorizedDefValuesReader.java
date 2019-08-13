/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.parquet;

import org.apache.flink.table.dataformat.vector.heap.AbstractHeapVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;

/**
 * A values reader for Parquet's run-length encoded data for definition ids and reading actual data
 * according to the definition ids.
 */
public final class VectorizedDefValuesReader extends VectorizedRleValuesReaderBase {

	public VectorizedDefValuesReader(int bitWidth) {
		super(bitWidth);
	}

	@Override
	public boolean readBoolean() {
		return this.readInteger() != 0;
	}

	@Override
	public void skip() {
		this.readInteger();
	}

	public void readBooleans(
			int total,
			HeapBooleanVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readBooleans(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readBoolean();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readBytes(
			int total,
			HeapByteVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readBytes(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readByte();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readShorts(
			int total,
			HeapShortVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						for (int i = 0; i < n; i++) {
							c.vector[rowId + i] = (short) data.readInteger();
						}
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = (short) data.readInteger();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readLongs(
			int total,
			HeapLongVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readLongs(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readLong();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readFloats(
			int total,
			HeapFloatVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readFloats(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readFloat();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readDoubles(
			int total,
			HeapDoubleVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readDoubles(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readDouble();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readBinaries(
			int total,
			HeapBytesVector c,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readBinaries(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							data.readBinaries(1, c, rowId + i);
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readIntegers(
			int total,
			HeapIntVector c,
			AbstractHeapVector nulls,
			int rowId,
			int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readIntegers(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readInteger();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	public void readIntegers(int total, HeapIntVector c, int rowId, int level,
			VectorizedValuesReader data) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				this.readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					if (currentValue == level) {
						data.readIntegers(n, c, rowId);
					} else {
						for (int index = rowId; index < rowId + n; index++) {
							c.setNullAt(index);
						}
					}
					break;
				case PACKED:
					for (int i = 0; i < n; ++i) {
						if (currentBuffer[currentBufferIdx++] == level) {
							c.vector[rowId + i] = data.readInteger();
						} else {
							c.setNullAt(rowId + i);
						}
					}
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}
}

