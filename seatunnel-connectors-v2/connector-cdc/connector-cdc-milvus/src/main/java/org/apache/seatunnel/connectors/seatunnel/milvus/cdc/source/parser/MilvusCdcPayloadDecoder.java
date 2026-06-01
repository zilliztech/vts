/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser;

import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.milvus.grpc.ArrayArray;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.VectorField;
import io.milvus.param.ParamUtils;
import milvus.proto.msg.Msg;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

class MilvusCdcPayloadDecoder {

    List<MilvusCdcDecodedRecord> decode(MilvusCdcMessageType messageType, ByteString payload) {
        try {
            switch (messageType) {
                case INSERT:
                    return decodeInsert(Msg.InsertRequest.parseFrom(payload));
                case DELETE:
                    return decodeDelete(Msg.DeleteRequest.parseFrom(payload));
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported Milvus CDC message type: " + messageType);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Failed to decode Milvus " + messageType.name() + " CDC payload.", e);
        }
    }

    private List<MilvusCdcDecodedRecord> decodeInsert(Msg.InsertRequest request) {
        if (request.getRowDataCount() > 0) {
            throw new UnsupportedOperationException(
                    "Milvus CDC row-based insert payload is not supported yet.");
        }
        int rowCount = insertRowCount(request);
        validateRequiredRowAlignedCount(
                "insert timestamps", request.getTimestampsCount(), rowCount);
        validateRequiredRowAlignedCount("insert rowIDs", request.getRowIDsCount(), rowCount);
        List<FieldRowReader> readers = new ArrayList<>(request.getFieldsDataCount());
        Set<String> fieldNames = new HashSet<>();
        Set<String> staticFieldNames = new HashSet<>();
        boolean dynamicFieldsPresent = false;
        for (FieldData fieldData : request.getFieldsDataList()) {
            FieldRowReader reader = new FieldRowReader(fieldData, rowCount);
            int fieldRowCount = reader.logicalRowCount();
            if (fieldRowCount != rowCount) {
                throw new IllegalArgumentException(
                        "Invalid Milvus CDC insert field row count for field "
                                + reader.fieldName()
                                + ": fieldRows="
                                + fieldRowCount
                                + ", numRows="
                                + rowCount);
            }
            if (!fieldNames.add(reader.fieldName())) {
                throw new IllegalArgumentException(
                        "Duplicate Milvus CDC insert field name: " + reader.fieldName() + ".");
            }
            if (reader.isDynamic()) {
                dynamicFieldsPresent = true;
            } else {
                staticFieldNames.add(reader.fieldName());
            }
            readers.add(reader);
        }
        staticFieldNames = Collections.unmodifiableSet(staticFieldNames);

        List<MilvusCdcDecodedRecord> records = new ArrayList<>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Map<String, Object> data = new LinkedHashMap<>();
            for (FieldRowReader reader : readers) {
                data.put(reader.fieldName(), reader.value(rowIndex));
            }
            records.add(
                    MilvusCdcDecodedRecord.builder()
                            .messageType(MilvusCdcMessageType.INSERT)
                            .database(requireNonBlank(request.getDbName(), "db_name"))
                            .collection(
                                    requireNonBlank(request.getCollectionName(), "collection_name"))
                            .partition(emptyToNull(request.getPartitionName()))
                            .rowIndex(rowIndex)
                            .rowId(valueAt(request.getRowIDsList(), rowIndex))
                            .eventTimestamp(valueAt(request.getTimestampsList(), rowIndex))
                            .data(data)
                            .staticFieldNames(staticFieldNames)
                            .dynamicFieldsPresent(dynamicFieldsPresent)
                            .build());
        }
        return records;
    }

    private List<MilvusCdcDecodedRecord> decodeDelete(Msg.DeleteRequest request) {
        List<Object> primaryKeys = primaryKeys(request);
        int rowCount = deleteRowCount(request, primaryKeys);
        validateRequiredRowAlignedCount("delete primary keys", primaryKeys.size(), rowCount);
        validateRequiredRowAlignedCount(
                "delete timestamps", request.getTimestampsCount(), rowCount);
        List<MilvusCdcDecodedRecord> records = new ArrayList<>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Object primaryKey = valueAt(primaryKeys, rowIndex);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("primary_key", primaryKey);
            records.add(
                    MilvusCdcDecodedRecord.builder()
                            .messageType(MilvusCdcMessageType.DELETE)
                            .database(requireNonBlank(request.getDbName(), "db_name"))
                            .collection(
                                    requireNonBlank(request.getCollectionName(), "collection_name"))
                            .partition(emptyToNull(request.getPartitionName()))
                            .rowIndex(rowIndex)
                            .eventTimestamp(valueAt(request.getTimestampsList(), rowIndex))
                            .primaryKey(primaryKey)
                            .data(data)
                            .build());
        }
        return records;
    }

    private int insertRowCount(Msg.InsertRequest request) {
        if (request.getNumRows() <= 0) {
            throw new IllegalArgumentException(
                    "Milvus CDC insert payload num_rows must be positive.");
        }
        if (request.getFieldsDataCount() == 0) {
            throw new IllegalArgumentException(
                    "Milvus CDC insert payload fields_data must not be empty.");
        }
        return Math.toIntExact(request.getNumRows());
    }

    private int deleteRowCount(Msg.DeleteRequest request, List<Object> primaryKeys) {
        if (request.getNumRows() <= 0) {
            throw new IllegalArgumentException(
                    "Milvus CDC delete payload num_rows must be positive.");
        }
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC delete payload primary keys must not be empty.");
        }
        return Math.toIntExact(request.getNumRows());
    }

    private void validateRequiredRowAlignedCount(String name, int count, int rowCount) {
        if (count != rowCount) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC "
                            + name
                            + " count: count="
                            + count
                            + ", numRows="
                            + rowCount);
        }
    }

    private class FieldRowReader {
        private final FieldData fieldData;
        private final int rowCount;

        private FieldRowReader(FieldData fieldData, int rowCount) {
            this.fieldData = fieldData;
            this.rowCount = rowCount;
        }

        private String fieldName() {
            return MilvusCdcPayloadDecoder.this.fieldName(fieldData);
        }

        private boolean isDynamic() {
            return fieldData.getIsDynamic();
        }

        private int logicalRowCount() {
            if (fieldData.getValidDataCount() > 0) {
                return fieldData.getValidDataCount();
            }
            if (fieldData.hasScalars()) {
                return scalarValueCount(fieldData.getScalars());
            }
            if (fieldData.hasVectors()) {
                return vectorRowCount(fieldData.getVectors());
            }
            if (fieldData.hasStructArrays()) {
                throw unsupportedStructArrays();
            }
            throw new IllegalArgumentException(
                    "Milvus CDC insert field " + fieldName() + " has no scalar or vector data.");
        }

        private Object value(int rowIndex) {
            if (!isValid(rowIndex)) {
                return null;
            }
            if (fieldData.hasScalars()) {
                return scalarValue(
                        fieldData.getScalars(), scalarIndex(fieldData.getScalars(), rowIndex));
            }
            if (fieldData.hasVectors()) {
                return vectorValue(fieldData.getVectors(), rowIndex);
            }
            if (fieldData.hasStructArrays()) {
                throw unsupportedStructArrays();
            }
            throw new IllegalArgumentException(
                    "Milvus CDC insert field " + fieldName() + " has no scalar or vector data.");
        }

        private boolean isValid(int rowIndex) {
            return fieldData.getValidDataCount() <= rowIndex || fieldData.getValidData(rowIndex);
        }

        private int validRowCount() {
            int count = 0;
            for (Boolean valid : fieldData.getValidDataList()) {
                if (Boolean.TRUE.equals(valid)) {
                    count++;
                }
            }
            return count;
        }

        private int validOrdinal(int rowIndex) {
            int ordinal = 0;
            for (int i = 0; i < rowIndex && i < fieldData.getValidDataCount(); i++) {
                if (fieldData.getValidData(i)) {
                    ordinal++;
                }
            }
            return ordinal;
        }

        private int physicalIndex(int physicalRowCount, int rowIndex, String valueKind) {
            if (fieldData.getValidDataCount() == 0) {
                if (physicalRowCount != rowCount) {
                    throw invalidPayloadLength(valueKind, physicalRowCount, rowCount);
                }
                return rowIndex;
            }
            if (physicalRowCount == rowCount) {
                return rowIndex;
            }
            int validRows = validRowCount();
            if (physicalRowCount == validRows) {
                return validOrdinal(rowIndex);
            }
            throw invalidPayloadLength(valueKind, physicalRowCount, validRows);
        }

        private IllegalArgumentException invalidPayloadLength(
                String valueKind, int physicalRowCount, int validRows) {
            return new IllegalArgumentException(
                    "Invalid Milvus CDC "
                            + valueKind
                            + " payload length for field "
                            + fieldName()
                            + ": physicalRows="
                            + physicalRowCount
                            + ", logicalRows="
                            + rowCount
                            + ", validRows="
                            + validRows);
        }

        private int scalarIndex(ScalarField scalars, int rowIndex) {
            return physicalIndex(scalarValueCount(scalars), rowIndex, "scalar");
        }

        private Object vectorValue(VectorField vectors, int rowIndex) {
            if (vectors.getDataCase() == VectorField.DataCase.VECTOR_ARRAY) {
                return vectorArrayValue(vectors, rowIndex);
            }

            int physicalRows = vectorRowCount(vectors);
            int physicalIndex = physicalIndex(physicalRows, rowIndex, "vector");
            return singleVectorValue(vectors, physicalIndex, physicalRows);
        }

        private Object vectorArrayValue(VectorField vectors, int rowIndex) {
            int physicalRows = vectors.getVectorArray().getDataCount();
            int physicalIndex = physicalIndex(physicalRows, rowIndex, "array-of-vector");
            VectorField nested = valueAt(vectors.getVectorArray().getDataList(), physicalIndex);
            if (nested == null) {
                throw new IllegalArgumentException(
                        "Invalid Milvus CDC array-of-vector payload for field "
                                + fieldName()
                                + ": missing physical row "
                                + physicalIndex);
            }
            validateVectorArrayElement(nested, vectors.getVectorArray().getElementType());
            return vectorArrayRowValue(nested, vectors.getVectorArray().getDim());
        }

        private UnsupportedOperationException unsupportedStructArrays() {
            return new UnsupportedOperationException(
                    "Milvus CDC insert payload should contain flattened struct sub-fields, "
                            + "but found struct_arrays for field "
                            + fieldName()
                            + ".");
        }
    }

    private int scalarValueCount(ScalarField scalars) {
        switch (scalars.getDataCase()) {
            case BOOL_DATA:
                return scalars.getBoolData().getDataCount();
            case INT_DATA:
                return scalars.getIntData().getDataCount();
            case LONG_DATA:
                return scalars.getLongData().getDataCount();
            case FLOAT_DATA:
                return scalars.getFloatData().getDataCount();
            case DOUBLE_DATA:
                return scalars.getDoubleData().getDataCount();
            case STRING_DATA:
                return scalars.getStringData().getDataCount();
            case BYTES_DATA:
                return scalars.getBytesData().getDataCount();
            case ARRAY_DATA:
                return scalars.getArrayData().getDataCount();
            case JSON_DATA:
                return scalars.getJsonData().getDataCount();
            case GEOMETRY_DATA:
                return scalars.getGeometryData().getDataCount();
            case TIMESTAMPTZ_DATA:
                return scalars.getTimestamptzData().getDataCount();
            case GEOMETRY_WKT_DATA:
                return scalars.getGeometryWktData().getDataCount();
            case MOL_DATA:
                return scalars.getMolData().getDataCount();
            case MOL_SMILES_DATA:
                return scalars.getMolSmilesData().getDataCount();
            default:
                throw new IllegalArgumentException("Milvus CDC scalar field has no data.");
        }
    }

    private Object scalarValue(ScalarField scalars, int index) {
        switch (scalars.getDataCase()) {
            case BOOL_DATA:
                return valueAt(scalars.getBoolData().getDataList(), index);
            case INT_DATA:
                return valueAt(scalars.getIntData().getDataList(), index);
            case LONG_DATA:
                return valueAt(scalars.getLongData().getDataList(), index);
            case FLOAT_DATA:
                return valueAt(scalars.getFloatData().getDataList(), index);
            case DOUBLE_DATA:
                return valueAt(scalars.getDoubleData().getDataList(), index);
            case STRING_DATA:
                return valueAt(scalars.getStringData().getDataList(), index);
            case BYTES_DATA:
                return base64(valueAt(scalars.getBytesData().getDataList(), index));
            case ARRAY_DATA:
                return arrayValue(scalars.getArrayData(), index);
            case JSON_DATA:
                return utf8(valueAt(scalars.getJsonData().getDataList(), index));
            case GEOMETRY_DATA:
                return byteBuffer(valueAt(scalars.getGeometryData().getDataList(), index));
            case TIMESTAMPTZ_DATA:
                return timestamptz(valueAt(scalars.getTimestamptzData().getDataList(), index));
            case GEOMETRY_WKT_DATA:
                return valueAt(scalars.getGeometryWktData().getDataList(), index);
            case MOL_DATA:
                return base64(valueAt(scalars.getMolData().getDataList(), index));
            case MOL_SMILES_DATA:
                return valueAt(scalars.getMolSmilesData().getDataList(), index);
            default:
                throw new IllegalArgumentException("Milvus CDC scalar field has no data.");
        }
    }

    private Object arrayValue(ArrayArray arrayArray, int rowIndex) {
        ScalarField rowArray = valueAt(arrayArray.getDataList(), rowIndex);
        return rowArray == null ? null : arrayValues(rowArray);
    }

    private List<Object> arrayValues(ScalarField rowArray) {
        if (rowArray.getDataCase() == ScalarField.DataCase.ARRAY_DATA
                && rowArray.getArrayData().getDataCount() == 1) {
            return arrayValues(rowArray.getArrayData().getData(0));
        }
        int valueCount = scalarValueCount(rowArray);
        List<Object> values = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.add(scalarValue(rowArray, i));
        }
        return values;
    }

    private int vectorRowCount(VectorField vectors) {
        switch (vectors.getDataCase()) {
            case FLOAT_VECTOR:
                return rowsFromFlatValueCount(
                        vectors.getFloatVector().getDataCount(), vectorDim(vectors, 0));
            case BINARY_VECTOR:
                return rowsFromBinaryByteValueCount(
                        vectors.getBinaryVector().size(), vectorDim(vectors, 0));
            case FLOAT16_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getFloat16Vector().size(), vectorDim(vectors, 0), 2);
            case BFLOAT16_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getBfloat16Vector().size(), vectorDim(vectors, 0), 2);
            case INT8_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getInt8Vector().size(), vectorDim(vectors, 0), 1);
            case SPARSE_FLOAT_VECTOR:
                return vectors.getSparseFloatVector().getContentsCount();
            case VECTOR_ARRAY:
                return vectors.getVectorArray().getDataCount();
            default:
                throw unsupportedVectorDataCase("vector field", vectors.getDataCase());
        }
    }

    private Object singleVectorValue(VectorField vectors, int rowIndex, int physicalRows) {
        switch (vectors.getDataCase()) {
            case FLOAT_VECTOR:
                return slice(
                        vectors.getFloatVector().getDataList(),
                        rowIndex,
                        vectorDim(vectors, physicalRows));
            case BINARY_VECTOR:
                return base64(
                        slice(vectors.getBinaryVector(), rowIndex, binaryBytesPerRow(vectors)));
            case FLOAT16_VECTOR:
                return base64(slice(vectors.getFloat16Vector(), rowIndex, bytesPerRow(vectors, 2)));
            case BFLOAT16_VECTOR:
                return base64(
                        slice(vectors.getBfloat16Vector(), rowIndex, bytesPerRow(vectors, 2)));
            case INT8_VECTOR:
                return base64(slice(vectors.getInt8Vector(), rowIndex, bytesPerRow(vectors, 1)));
            case SPARSE_FLOAT_VECTOR:
                return sparseFloatVector(
                        valueAt(vectors.getSparseFloatVector().getContentsList(), rowIndex));
            default:
                throw unsupportedVectorDataCase("vector field", vectors.getDataCase());
        }
    }

    private List<Object> vectorArrayRowValue(VectorField nested, long fallbackDim) {
        int physicalRows = vectorRowCountWithFallbackDim(nested, fallbackDim);
        List<Object> values = new ArrayList<>(physicalRows);
        for (int i = 0; i < physicalRows; i++) {
            values.add(singleVectorValueWithFallbackDim(nested, i, physicalRows, fallbackDim));
        }
        return values;
    }

    private int vectorRowCountWithFallbackDim(VectorField vectors, long fallbackDim) {
        switch (vectors.getDataCase()) {
            case FLOAT_VECTOR:
                return rowsFromFlatValueCount(
                        vectors.getFloatVector().getDataCount(), vectorDim(vectors, fallbackDim));
            case BINARY_VECTOR:
                return rowsFromBinaryByteValueCount(
                        vectors.getBinaryVector().size(), vectorDim(vectors, fallbackDim));
            case FLOAT16_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getFloat16Vector().size(), vectorDim(vectors, fallbackDim), 2);
            case BFLOAT16_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getBfloat16Vector().size(), vectorDim(vectors, fallbackDim), 2);
            case INT8_VECTOR:
                return rowsFromByteValueCount(
                        vectors.getInt8Vector().size(), vectorDim(vectors, fallbackDim), 1);
            default:
                throw unsupportedVectorDataCase("vector array element", vectors.getDataCase());
        }
    }

    private Object singleVectorValueWithFallbackDim(
            VectorField vectors, int rowIndex, int physicalRows, long fallbackDim) {
        switch (vectors.getDataCase()) {
            case FLOAT_VECTOR:
                return slice(
                        vectors.getFloatVector().getDataList(),
                        rowIndex,
                        vectorDim(vectors, fallbackDim));
            case BINARY_VECTOR:
                return base64(
                        slice(
                                vectors.getBinaryVector(),
                                rowIndex,
                                binaryBytesPerRow(vectorDim(vectors, fallbackDim))));
            case FLOAT16_VECTOR:
                return base64(
                        slice(
                                vectors.getFloat16Vector(),
                                rowIndex,
                                bytesPerRow(vectorDim(vectors, fallbackDim), 2)));
            case BFLOAT16_VECTOR:
                return base64(
                        slice(
                                vectors.getBfloat16Vector(),
                                rowIndex,
                                bytesPerRow(vectorDim(vectors, fallbackDim), 2)));
            case INT8_VECTOR:
                return base64(
                        slice(
                                vectors.getInt8Vector(),
                                rowIndex,
                                bytesPerRow(vectorDim(vectors, fallbackDim), 1)));
            default:
                throw unsupportedVectorDataCase("vector array element", vectors.getDataCase());
        }
    }

    private void validateVectorArrayElement(VectorField nested, DataType elementType) {
        VectorField.DataCase expected = vectorArrayElementDataCase(elementType);
        if (nested.getDataCase() != expected) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC vector array element for elementType="
                            + elementType
                            + ": expected "
                            + expected
                            + " but found "
                            + nested.getDataCase()
                            + ".");
        }
    }

    private VectorField.DataCase vectorArrayElementDataCase(DataType elementType) {
        if (elementType == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC vector array element type must not be null.");
        }
        switch (elementType) {
            case FloatVector:
                return VectorField.DataCase.FLOAT_VECTOR;
            case BinaryVector:
                return VectorField.DataCase.BINARY_VECTOR;
            case Float16Vector:
                return VectorField.DataCase.FLOAT16_VECTOR;
            case BFloat16Vector:
                return VectorField.DataCase.BFLOAT16_VECTOR;
            case Int8Vector:
                return VectorField.DataCase.INT8_VECTOR;
            case SparseFloatVector:
                throw new UnsupportedOperationException(
                        "Milvus CDC array-of-vector with SparseFloatVector element type is not supported.");
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Milvus CDC array-of-vector element type: "
                                + elementType
                                + ".");
        }
    }

    private IllegalArgumentException unsupportedVectorDataCase(
            String valueKind, VectorField.DataCase dataCase) {
        if (dataCase == VectorField.DataCase.DATA_NOT_SET) {
            return new IllegalArgumentException("Milvus CDC " + valueKind + " has no vector data.");
        }
        return new IllegalArgumentException(
                "Milvus CDC " + valueKind + " has unsupported vector data case: " + dataCase + ".");
    }

    private List<Object> primaryKeys(Msg.DeleteRequest request) {
        if (request.hasPrimaryKeys()) {
            IDs primaryKeys = request.getPrimaryKeys();
            switch (primaryKeys.getIdFieldCase()) {
                case INT_ID:
                    return new ArrayList<>(primaryKeys.getIntId().getDataList());
                case STR_ID:
                    return new ArrayList<>(primaryKeys.getStrId().getDataList());
                default:
                    throw new IllegalArgumentException(
                            "Milvus CDC delete payload primary_keys must contain int_id or str_id.");
            }
        }
        return new ArrayList<>(request.getInt64PrimaryKeysList());
    }

    private String fieldName(FieldData fieldData) {
        return requireNonBlank(fieldData.getFieldName(), "field_name");
    }

    private static <T> T valueAt(List<T> values, int index) {
        if (index < 0 || index >= values.size()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC payload index " + index + ", values=" + values.size());
        }
        return values.get(index);
    }

    private int rowsFromFlatValueCount(int valueCount, long dim) {
        int width = Math.toIntExact(dim);
        if (width <= 0) {
            throw new IllegalArgumentException("Milvus CDC vector dim must be positive.");
        }
        if (valueCount % width != 0) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC vector payload length: values="
                            + valueCount
                            + ", dim="
                            + width);
        }
        return valueCount / width;
    }

    private int rowsFromBinaryByteValueCount(int valueCount, long dim) {
        int bytesPerRow = binaryBytesPerRow(dim);
        if (valueCount % bytesPerRow != 0) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC binary vector payload length: bytes="
                            + valueCount
                            + ", bytesPerRow="
                            + bytesPerRow);
        }
        return valueCount / bytesPerRow;
    }

    private int rowsFromByteValueCount(int valueCount, long dim, int bytesPerDimension) {
        int bytesPerRow = bytesPerRow(dim, bytesPerDimension);
        if (bytesPerRow <= 0) {
            throw new IllegalArgumentException("Milvus CDC vector byte width must be positive.");
        }
        if (valueCount % bytesPerRow != 0) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC vector payload length: bytes="
                            + valueCount
                            + ", bytesPerRow="
                            + bytesPerRow);
        }
        return valueCount / bytesPerRow;
    }

    private int vectorDim(VectorField vectors, long fallbackDim) {
        if (vectors.getDim() > 0) {
            return Math.toIntExact(vectors.getDim());
        }
        if (fallbackDim > 0) {
            return Math.toIntExact(fallbackDim);
        }
        throw new IllegalArgumentException("Milvus CDC vector dim must be set.");
    }

    private int binaryBytesPerRow(VectorField vectors) {
        return binaryBytesPerRow(vectorDim(vectors, 0));
    }

    private int binaryBytesPerRow(long dim) {
        if (dim % 8 != 0) {
            throw new IllegalArgumentException(
                    "Milvus CDC binary vector dim must be a multiple of 8: dim=" + dim);
        }
        return Math.toIntExact(dim / 8);
    }

    private int bytesPerRow(VectorField vectors, int bytesPerDimension) {
        return bytesPerRow(vectorDim(vectors, 0), bytesPerDimension);
    }

    private int bytesPerRow(long dim, int bytesPerDimension) {
        return Math.toIntExact(dim) * bytesPerDimension;
    }

    private <T> List<T> slice(List<T> values, int rowIndex, int width) {
        if (width <= 0) {
            throw new IllegalArgumentException("Milvus CDC vector slice width must be positive.");
        }
        int from = rowIndex * width;
        int to = from + width;
        if (from < 0 || to > values.size()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC vector slice: from="
                            + from
                            + ", to="
                            + to
                            + ", values="
                            + values.size());
        }
        return new ArrayList<>(values.subList(from, to));
    }

    private ByteString slice(ByteString values, int rowIndex, int width) {
        if (width <= 0) {
            throw new IllegalArgumentException("Milvus CDC vector slice width must be positive.");
        }
        int from = rowIndex * width;
        int to = from + width;
        if (from < 0 || to > values.size()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC vector slice: from="
                            + from
                            + ", to="
                            + to
                            + ", bytes="
                            + values.size());
        }
        return values.substring(from, to);
    }

    private String base64(ByteString value) {
        if (value == null) {
            throw new IllegalArgumentException("Milvus CDC binary value must not be null.");
        }
        return Base64.getEncoder().encodeToString(value.toByteArray());
    }

    private ByteBuffer byteBuffer(ByteString value) {
        if (value == null) {
            throw new IllegalArgumentException("Milvus CDC bytes value must not be null.");
        }
        return ByteBuffer.wrap(value.toByteArray());
    }

    private SortedMap<Long, Float> sparseFloatVector(ByteString value) {
        return ParamUtils.decodeSparseFloatVector(byteBuffer(value));
    }

    private String timestamptz(Long value) {
        if (value == null) {
            throw new IllegalArgumentException("Milvus CDC timestamptz value must not be null.");
        }
        // Milvus stores Timestamptz internally as UTC Unix microseconds.
        long seconds = Math.floorDiv(value, 1_000_000L);
        long micros = Math.floorMod(value, 1_000_000L);
        return Instant.ofEpochSecond(seconds, micros * 1_000L).toString();
    }

    private String utf8(ByteString value) {
        if (value == null) {
            throw new IllegalArgumentException("Milvus CDC UTF-8 value must not be null.");
        }
        return value.toStringUtf8();
    }

    private String emptyToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC payload " + fieldName + " must not be empty.");
        }
        return value.trim();
    }
}
