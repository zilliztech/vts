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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.annotation.Experimental;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * GeometryType represents a geometry type in SeaTunnel.
 *
 * <p>Experimental feature, use with caution
 */
@Experimental
public class GeometryType<T> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final GeometryType<ByteBuffer> GEOMETRY_TYPE =
            new GeometryType<>(ByteBuffer.class, SqlType.GEOMETRY);

    // --------------------------------------------------------------------------------------------

    /** The physical type class. */
    private final Class<T> typeClass;

    private final SqlType sqlType;

    protected GeometryType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return this.typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof GeometryType)) {
            return false;
        }
        GeometryType<?> that = (GeometryType<?>) obj;
        return Objects.equals(typeClass, that.typeClass) && Objects.equals(sqlType, that.sqlType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass, sqlType);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
