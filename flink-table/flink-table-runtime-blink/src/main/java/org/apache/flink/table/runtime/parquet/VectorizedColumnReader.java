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

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.dataformat.vector.heap.AbstractHeapVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * Read a column.
 */
public class VectorizedColumnReader {

	private static final Logger LOG = LoggerFactory.getLogger(VectorizedColumnReader.class);

	/**
	 * The dictionary, if this column has dictionary encoding.
	 */
	protected final Dictionary dictionary;
	/**
	 * Maximum definition level for this column.
	 */
	protected final int maxDefLevel;
	private final PageReader pageReader;
	private final ColumnDescriptor descriptor;
	protected ValuesReader dataColumn;
	// Only set if vectorized decoding is true. This is used instead of the row by row decoding
	// with `definitionLevelColumn`.
	public VectorizedDefValuesReader defColumn;
	/**
	 * Total number of values read.
	 */
	private long valuesRead;
	/**
	 * value that indicates the end of the current page. That is,
	 * if valuesRead == endOfPageValueCount, we are at the end of the page.
	 */
	private long endOfPageValueCount;
	/**
	 * If true, the current page is dictionary encoded.
	 */
	private boolean isCurrentPageDictionaryEncoded;

	/**
	 * Total values in the current page.
	 */
	private int pageValueCount;

	public VectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader) throws IOException {
		this.descriptor = descriptor;
		this.pageReader = pageReader;
		this.maxDefLevel = descriptor.getMaxDefinitionLevel();

		DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
		if (dictionaryPage != null) {
			try {
				this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
				this.isCurrentPageDictionaryEncoded = true;
			} catch (IOException e) {
				throw new IOException("could not decode the dictionary for " + descriptor, e);
			}
		} else {
			this.dictionary = null;
			this.isCurrentPageDictionaryEncoded = false;
		}
	}

	public void readColumnBatch(int total, AbstractHeapVector column, LogicalType fieldType) throws IOException {
		int rowId = 0;
		HeapIntVector dictionaryIds = null;
		if (dictionary != null) {
			dictionaryIds = column.reserveDictionaryIds(VectorizedColumnBatch.DEFAULT_SIZE);
		}
		while (total > 0) {
			// Compute the number of values we want to read in this page.
			int leftInPage = (int) (endOfPageValueCount - valuesRead);
			if (leftInPage == 0) {
				readPage();
				leftInPage = (int) (endOfPageValueCount - valuesRead);
			}
			int num = Math.min(total, leftInPage);
			if (isCurrentPageDictionaryEncoded) {
				// Read and decode dictionary ids.
				defColumn.readIntegers(
						num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

				// Timestamp values encoded as INT64 can't be lazily decoded as we need to post process
				// the values to add microseconds precision.
				if (column.hasDictionary() || (rowId == 0 &&
						(descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32 ||
								(descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64  &&
										!fieldType.getTypeRoot().equals(TIMESTAMP_WITHOUT_TIME_ZONE)) ||
								descriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT ||
								descriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE ||
								descriptor.getType() == PrimitiveType.PrimitiveTypeName.BINARY))) {
					// Column vector supports lazy decoding of dictionary values so just set the dictionary.
					// We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
					// non-dictionary encoded values have already been added).
					column.setDictionary(new ParquetDictionary(dictionary));
				} else {
					decodeDictionaryIds(rowId, num, column, dictionaryIds, fieldType);
				}
			} else {
				if (column.hasDictionary() && rowId != 0) {
					// This batch already has dictionary encoded values but this new page is not. The batch
					// does not support a mix of dictionary and not so we will decode the dictionary.
					decodeDictionaryIds(0, rowId, column, column.getDictionaryIds(), fieldType);
				}
				column.setDictionary(null);
				switch (descriptor.getType()) {
					case BOOLEAN:
						defColumn.readBooleans(num, (HeapBooleanVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						break;
					case INT32:
						switch (fieldType.getTypeRoot()) {
							case SMALLINT:
								defColumn.readShorts(num, (HeapShortVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
								break;
							case TINYINT:
								defColumn.readBytes(num, (HeapByteVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
								break;
							default:
								defColumn.readIntegers(num, (HeapIntVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						}
						break;
					case INT64:
						defColumn.readLongs(num, (HeapLongVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						break;
					case FLOAT:
						defColumn.readFloats(num, (HeapFloatVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						break;
					case DOUBLE:
						defColumn.readDoubles(num, (HeapDoubleVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						break;
					case BINARY:
						defColumn.readBinaries(num, (HeapBytesVector) column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
						break;
					case FIXED_LEN_BYTE_ARRAY:
						VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
						// This is where we implement support for the valid type conversions.
						// TODO: implement remaining type conversions
						if (fieldType instanceof DecimalType) {
							DecimalType decimalTypeInfo = (DecimalType) fieldType;
							if (Decimal.is32BitDecimal(decimalTypeInfo.getPrecision())) {
								for (int i = 0; i < num; i++) {
									if (defColumn.readInteger() == maxDefLevel) {
										((HeapIntVector) column).vector[rowId + i] = (int) binaryToUnscaledLong(data.readBinary(descriptor.getTypeLength()));
									} else {
										column.setNullAt(rowId + i);
									}
								}
							} else if (Decimal.is64BitDecimal(decimalTypeInfo.getPrecision())) {
								for (int i = 0; i < num; i++) {
									if (defColumn.readInteger() == maxDefLevel) {
										((HeapLongVector) column).vector[rowId + i] = binaryToUnscaledLong(data.readBinary(descriptor.getTypeLength()));
									} else {
										column.setNullAt(rowId + i);
									}
								}
							} else {
								for (int i = 0; i < num; i++) {
									if (defColumn.readInteger() == maxDefLevel) {
										((HeapBytesVector) column).setVal(rowId + i, data.readBinary(descriptor.getTypeLength()).getBytes());
									} else {
										column.setNullAt(rowId + i);
									}
								}
							}
						} else {
							throw new UnsupportedOperationException("Unimplemented type: " + fieldType);
						}
						break;
					default:
						throw new IOException("Unsupported type: " + descriptor.getType());
				}
			}

			valuesRead += num;
			rowId += num;
			total -= num;
		}
	}

	private void decodeDictionaryIds(int rowId, int num, AbstractHeapVector column,
			HeapIntVector dictionaryIds, LogicalType fieldType) {
		switch (descriptor.getType()) {
			case INT32:
				if (fieldType.getTypeRoot().equals(INTEGER) ||
						(fieldType instanceof DecimalType &&
								Decimal.is32BitDecimal(((DecimalType) fieldType).getPrecision()))) {
					for (int i = rowId; i < rowId + num; ++i) {
						if (!column.isNullAt(i)) {
							((HeapIntVector) column).vector[i] = dictionary.decodeToInt(dictionaryIds.vector[i]);
						}
					}
				} else if (fieldType.getTypeRoot().equals(TINYINT)) {
					for (int i = rowId; i < rowId + num; ++i) {
						if (!column.isNullAt(i)) {
							((HeapByteVector) column).vector[i] = (byte) dictionary.decodeToInt(dictionaryIds.vector[i]);
						}
					}
				} else if (fieldType.getTypeRoot().equals(SMALLINT)) {
					for (int i = rowId; i < rowId + num; ++i) {
						if (!column.isNullAt(i)) {
							((HeapShortVector) column).vector[i] = (short) dictionary.decodeToInt(dictionaryIds.vector[i]);
						}
					}
				} else {
					throw new UnsupportedOperationException("Unimplemented type: " + fieldType);
				}
				break;

			case INT64:
				if (fieldType.getTypeRoot().equals(BIGINT) || fieldType.getTypeRoot().equals(TIMESTAMP_WITHOUT_TIME_ZONE) ||
						(fieldType instanceof DecimalType && Decimal.is64BitDecimal(((DecimalType) fieldType).getPrecision()))) {
					for (int i = rowId; i < rowId + num; ++i) {
						if (!column.isNullAt(i)) {
							((HeapLongVector) column).vector[i] = dictionary.decodeToLong(dictionaryIds.vector[i]);
						}
					}
				} else {
					throw new UnsupportedOperationException("Unimplemented type: " + fieldType);
				}
				break;

			case FLOAT:
				for (int i = rowId; i < rowId + num; ++i) {
					if (!column.isNullAt(i)) {
						((HeapFloatVector) column).vector[i] = dictionary.decodeToFloat(dictionaryIds.vector[i]);
					}
				}
				break;

			case DOUBLE:
				for (int i = rowId; i < rowId + num; ++i) {
					if (!column.isNullAt(i)) {
						((HeapDoubleVector) column).vector[i] = dictionary.decodeToDouble(dictionaryIds.vector[i]);
					}
				}
				break;
			case BINARY:
				// TODO: this is incredibly inefficient as it blows up the dictionary right here. We
				// need to do this better. We should probably add the dictionary data to the AbstractColumnVector
				// and reuse it across batches. This should mean adding a ByteArray would just update
				// the length and offset.
				for (int i = rowId; i < rowId + num; ++i) {
					if (!column.isNullAt(i)) {
						Binary v = dictionary.decodeToBinary(dictionaryIds.vector[i]);
						((HeapBytesVector) column).setVal(i, v.getBytes());
					}
				}
				break;
			case FIXED_LEN_BYTE_ARRAY:
				// DecimalType written in the legacy mode
				if (fieldType instanceof DecimalType) {
					DecimalType decimalTypeInfo = (DecimalType) fieldType;
					if (Decimal.is32BitDecimal(decimalTypeInfo.getPrecision())) {
						for (int i = rowId; i < rowId + num; ++i) {
							if (!column.isNullAt(i)) {
								Binary v = dictionary.decodeToBinary(dictionaryIds.vector[i]);
								((HeapIntVector) column).vector[i] = (int) binaryToUnscaledLong(v);
							}
						}
					} else if (Decimal.is64BitDecimal(decimalTypeInfo.getPrecision())) {
						for (int i = rowId; i < rowId + num; ++i) {
							if (!column.isNullAt(i)) {
								Binary v = dictionary.decodeToBinary(dictionaryIds.vector[i]);
								((HeapLongVector) column).vector[i] = binaryToUnscaledLong(v);
							}
						}
					} else {
						for (int i = rowId; i < rowId + num; ++i) {
							if (!column.isNullAt(i)) {
								Binary v = dictionary.decodeToBinary(dictionaryIds.vector[i]);
								((HeapBytesVector) column).setVal(i, v.getBytes());
							}
						}
					}
				} else {
					throw new UnsupportedOperationException();
				}
				break;

			default:
				throw new UnsupportedOperationException("Unsupported type: " + descriptor.getType());
		}
	}

	private void readPage() throws IOException {
		DataPage page = pageReader.readPage();
		page.accept(new DataPage.Visitor<Void>() {
			@Override
			public Void visit(DataPageV1 dataPageV) {
				readPageV1(dataPageV);
				return null;
			}

			@Override
			public Void visit(DataPageV2 dataPageV2) {
				readPageV2(dataPageV2);
				return null;
			}
		});
	}

	private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
		this.endOfPageValueCount = valuesRead + pageValueCount;
		if (dataEncoding.usesDictionary()) {
			this.dataColumn = null;
			if (dictionary == null) {
				throw new IOException(
						"could not read page in col " + descriptor +
								" as the dictionary was missing for encoding " + dataEncoding);
			}
			this.dataColumn = new VectorizedRleValuesReader();
			this.isCurrentPageDictionaryEncoded = true;
		} else {
			if (dataEncoding != Encoding.PLAIN) {
				throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
			}
			this.dataColumn = new VectorizedPlainValuesReader();
			this.isCurrentPageDictionaryEncoded = false;
		}

		try {
			dataColumn.initFromPage(pageValueCount, bytes, offset);
		} catch (IOException e) {
			throw new IOException("could not read page in col " + descriptor, e);
		}
	}

	private void readPageV1(DataPageV1 page) {
		this.pageValueCount = page.getValueCount();
		ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
		// Initialize the decoders.
		if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
			throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
		}
		int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
		this.defColumn = new VectorizedDefValuesReader(bitWidth);
		ValuesReader dlReader = this.defColumn;
		try {
			byte[] bytes = page.getBytes().toByteArray();
			LOG.debug("page size " + bytes.length + " bytes and " + pageValueCount + " records");
			LOG.debug("reading repetition levels at 0");
			rlReader.initFromPage(pageValueCount, bytes, 0);
			int next = rlReader.getNextOffset();
			LOG.debug("reading definition levels at " + next);
			dlReader.initFromPage(pageValueCount, bytes, next);
			next = dlReader.getNextOffset();
			LOG.debug("reading data at " + next);
			initDataReader(page.getValueEncoding(), bytes, next);
		} catch (IOException e) {
			throw new ParquetDecodingException("could not read page " + page + " in col " + descriptor, e);
		}
	}

	private void readPageV2(DataPageV2 page) {
		this.pageValueCount = page.getValueCount();

		int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
		defColumn = new VectorizedDefValuesReader(bitWidth);
		try {
			defColumn.initFromBuffer(this.pageValueCount, page.getDefinitionLevels().toByteArray());
			initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
		} catch (IOException e) {
			throw new ParquetDecodingException("could not read page " + page + " in col " + descriptor, e);
		}
	}

	private long binaryToUnscaledLong(Binary binary) {
		// The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here
		// we are using `Binary.toByteBuffer.array()` to steal the underlying byte array without
		// copying it.
		ByteBuffer buffer = binary.toByteBuffer();
		byte[] bytes = buffer.array();
		int start = buffer.arrayOffset() + buffer.position();
		int end = buffer.arrayOffset() + buffer.limit();

		long unscaled = 0L;
		int i = start;

		while (i < end) {
			unscaled = (unscaled << 8) | (bytes[i] & 0xff);
			i += 1;
		}

		int bits = 8 * (end - start);
		unscaled = (unscaled << (64 - bits)) >> (64 - bits);
		return unscaled;
	}
}
