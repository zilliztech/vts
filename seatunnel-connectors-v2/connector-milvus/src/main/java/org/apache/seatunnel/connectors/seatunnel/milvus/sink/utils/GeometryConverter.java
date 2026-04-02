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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTWriter;

import java.nio.ByteBuffer;

/**
 * Converts various geometry input formats to WKT (Well-Known Text) for Milvus.
 *
 * <p>VTS principle: this converter is a pure data transport — it does not validate
 * whether values are "valid" for Milvus or pre-empt Milvus's behavior. Its only
 * obligation is to convert losslessly. When lossless conversion is impossible (e.g.
 * WKB with M coordinates, which JTS cannot represent), it throws explicitly with
 * the reason being "VTS cannot losslessly convert", not "Milvus rejects".
 *
 * <p>Supported input formats:
 * <ul>
 *   <li>WKT: passed through unchanged (including 3D POINT Z, POINTZ, POINT M, POINT ZM, inline POINT(1 2 3))</li>
 *   <li>EWKT: SRID prefix is stripped, remaining WKT is passed through</li>
 *   <li>GeoJSON: parsed and re-emitted as WKT, supports 2D and 3D coordinate arrays</li>
 *   <li>WKB/EWKB hex: parsed via JTS and re-emitted as WKT (3D-aware)</li>
 *   <li>ES geo_point object: {"lat": x, "lon": y}</li>
 *   <li>ES geo_point array: [lon, lat] (GeoJSON convention, may include Z as third element)</li>
 *   <li>Lat/lon string: "lat,lon" by default (ES convention) — orderable via parameter</li>
 * </ul>
 *
 * <p>3D / Z coordinates are preserved end-to-end. Whether Milvus's spatial query
 * functions honor Z is the user's concern, not VTS's.
 */
public class GeometryConverter {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    /**
     * Convert mode for {@code String} Geometry inputs.
     *
     * <p>This is the most important knob in this class. The two modes implement
     * fundamentally different contracts:
     *
     * <ul>
     *   <li><b>{@link #PASSTHROUGH}</b> (production default): VTS does <em>nothing</em>
     *     to the input string — no trim, no SRID strip, no format detection. The
     *     string flows byte-for-byte to the destination. Use this when the source
     *     already produces destination-compatible WKT (Milvus → Milvus,
     *     PostgreSQL with {@code ST_AsText}, etc.). Cost: zero per record.
     *
     *   <li><b>{@link #PARSE}</b>: VTS detects the format and converts to WKT —
     *     handles WKT pass-through, EWKT (SRID strip), GeoJSON, WKB hex, ES
     *     geo_point object/array/string variants. Use this when the source
     *     produces non-WKT formats (Elasticsearch geo_point/geo_shape, PostgreSQL
     *     with {@code ST_AsEWKT}, etc.).
     * </ul>
     *
     * <p><b>Note:</b> this mode does <em>not</em> affect {@link #convertWkbBytes(byte[])}
     * — raw WKB byte input always goes through JTS because Milvus does not accept
     * WKB hex strings on the wire.
     */
    public enum ConvertMode {
        /** Hand the input string to the destination unchanged. Zero processing. */
        PASSTHROUGH,
        /** Detect the input format and convert to WKT (with SRID stripping etc.). */
        PARSE;

        /**
         * Parse the {@code geometry_convert_mode} sink config value.
         *
         * <p>{@code null} maps to {@link #PASSTHROUGH} — the production default.
         * Existing Milvus → Milvus configurations (the majority case) work without
         * any conversion overhead and without setting the option.
         *
         * @throws MilvusConnectorException if the value is non-null and not one of
         *     {@code passthrough} / {@code parse} (case-insensitive)
         */
        public static ConvertMode fromConfig(String raw) {
            if (raw == null) {
                return PASSTHROUGH;
            }
            switch (raw.trim().toLowerCase()) {
                case "passthrough":
                    return PASSTHROUGH;
                case "parse":
                    return PARSE;
                default:
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                            "Invalid value for geometry_convert_mode: '" + raw
                                    + "'. Allowed values: 'passthrough', 'parse'.");
            }
        }
    }

    /**
     * Pure-passthrough singleton (production default). Use when the source already
     * produces destination-compatible WKT strings.
     *
     * <p><b>Thread-safe.</b> All instance state is {@code final} and immutable; the
     * conversion methods only read that state, and {@link #writeWkt3D(Geometry)}
     * constructs a fresh {@code WKTWriter} per call, so this singleton can be
     * shared across all parallel sink subtasks without synchronization.
     */
    public static final GeometryConverter PASSTHROUGH =
            new GeometryConverter(ConvertMode.PASSTHROUGH, CoordinateOrder.LAT_LON);

    /**
     * Parsing singleton (Elasticsearch / PostGIS-EWKT / mixed-format sources).
     * Detects WKT/EWKT/GeoJSON/WKB hex/ES geo_point variants and converts to WKT.
     *
     * <p>Same thread-safety guarantees as {@link #PASSTHROUGH}.
     */
    public static final GeometryConverter PARSE =
            new GeometryConverter(ConvertMode.PARSE, CoordinateOrder.LAT_LON);

    /**
     * Convert mode (passthrough vs parse). Final and immutable: a converter
     * instance has one mode for its entire lifetime.
     */
    private final ConvertMode mode;

    /**
     * Per-pipeline configuration for how to interpret bare {@code "x,y"} coordinate
     * strings (the only geometry input shape with genuine ordering ambiguity).
     * Final and immutable. Only consulted when {@link #mode} is {@link ConvertMode#PARSE}.
     */
    private final CoordinateOrder stringCoordOrder;

    public GeometryConverter(ConvertMode mode, CoordinateOrder stringCoordOrder) {
        this.mode = mode;
        this.stringCoordOrder = stringCoordOrder;
    }

    /**
     * Build a {@code GeometryConverter} from the sink config.
     *
     * <p>This is the single place that knows which sink option keys control
     * geometry behavior; callers higher up the stack (writers, the dispatching
     * {@code MilvusSinkConverter}) hand the whole config to this method and stay
     * agnostic of geometry-specific keys.
     */
    public static GeometryConverter fromConfig(ReadonlyConfig config) {
        return new GeometryConverter(
                ConvertMode.fromConfig(config.get(MilvusSinkConfig.GEOMETRY_CONVERT_MODE)),
                CoordinateOrder.fromConfig(config.get(MilvusSinkConfig.GEOMETRY_STRING_COORD_ORDER)));
    }

    /**
     * Serialize a geometry to WKT preserving Z when present.
     *
     * <p>{@code WKTWriter(3)} is the 3D-aware constructor: it outputs Z when the
     * coordinate has it ({@code !Double.isNaN(z)}) and silently degrades to 2D output
     * otherwise, so the same helper handles both 2D and 3D geometries losslessly.
     *
     * <p><b>Why per-call construction.</b> A {@code WKTWriter} held in a static field
     * happens to be thread-safe in JTS 1.20.0 in our exact usage: we never call any
     * setter after construction (so the mutable instance fields are written once and
     * then only read), and {@code WKTWriter.getFormatter(Geometry)} returns a fresh
     * {@link org.locationtech.jts.io.OrdinateFormat} per call rather than caching one
     * on the instance. That safety, however, is an undocumented implementation
     * detail of JTS — a future JTS version that decides to lazily cache the
     * {@code OrdinateFormat} on the writer instance would silently introduce a race
     * across our parallel sink subtasks. Constructing a new writer per call costs a
     * handful of field assignments (no I/O, no parsing), pins our correctness to
     * something we control, and removes any need for the next reader to re-derive
     * the safety argument from JTS bytecode.
     */
    private static String writeWkt3D(Geometry geometry) {
        return new WKTWriter(3).write(geometry);
    }

    /**
     * Coordinate order for ambiguous "x,y" string inputs.
     *
     * <p>VTS does not have semantic knowledge of the source. The default {@link #LAT_LON}
     * matches Elasticsearch's geo_point string convention; sources that emit "lon,lat"
     * (e.g. some PostgreSQL/JDBC text representations) must opt in via {@link #LON_LAT}.
     */
    public enum CoordinateOrder {
        /** First number is latitude, second is longitude (Elasticsearch geo_point string). */
        LAT_LON,
        /** First number is longitude, second is latitude. */
        LON_LAT;

        /**
         * Parse the {@code geometry_string_coord_order} sink config value.
         *
         * <p>{@code null} maps to the {@link #LAT_LON} default (existing ES → Milvus
         * pipelines keep working without configuration).
         *
         * @throws MilvusConnectorException if the value is non-null and not one of
         *     {@code lat_lon} / {@code lon_lat} (case-insensitive)
         */
        public static CoordinateOrder fromConfig(String raw) {
            if (raw == null) {
                return LAT_LON;
            }
            switch (raw.trim().toLowerCase()) {
                case "lat_lon":
                    return LAT_LON;
                case "lon_lat":
                    return LON_LAT;
                default:
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                            "Invalid value for geometry_string_coord_order: '" + raw
                                    + "'. Allowed values: 'lat_lon', 'lon_lat'.");
            }
        }
    }

    /**
     * Main entry point for the Milvus sink. Handles a Geometry field value of
     * whatever shape the source connector produced.
     *
     * <p>Behavior depends on this converter's {@link #mode}:
     * <ul>
     *   <li>{@link ConvertMode#PASSTHROUGH}: returns {@code value} <b>unchanged,
     *     regardless of type</b>. Identical to the original connector behavior
     *     before any geometry conversion logic was added (it was literally
     *     {@code return value;}). No type dispatch, no null check, no format
     *     detection. If the source hands over something the destination can't
     *     digest, the destination reports the error. This is the production
     *     default.
     *
     *   <li>{@link ConvertMode#PARSE}: dispatches on value type.
     *     <ul>
     *       <li>{@code String} → {@link #convertToWkt(String)} — handles WKT,
     *         EWKT (SRID strip), GeoJSON, WKB hex, ES geo_point variants.</li>
     *       <li>{@code ByteBuffer} → {@link #convertWkbBytes(byte[])} via a
     *         non-mutating {@code duplicate().get()}. Ready for any future
     *         source that emits raw WKB bytes; no current VTS source connector
     *         does so today.</li>
     *       <li>{@code null} → {@code null}.</li>
     *       <li>Anything else → explicit error.</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    public Object convert(Object value) {
        if (mode == ConvertMode.PASSTHROUGH) {
            return value;
        }
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return convertToWkt((String) value);
        }
        if (value instanceof ByteBuffer) {
            // duplicate() so the caller's buffer position/limit stay untouched.
            ByteBuffer buf = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return convertWkbBytes(bytes);
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Unsupported Geometry value type in parse mode: "
                        + value.getClass().getName()
                        + ". Expected String or ByteBuffer.");
    }

    /**
     * Convert a geometry string to a destination-compatible WKT.
     *
     * <p>Behavior depends on this converter's {@link #mode}:
     * <ul>
     *   <li>{@link ConvertMode#PASSTHROUGH}: returns {@code input} unchanged. No
     *     trim, no SRID strip, no format detection. Zero allocations.
     *   <li>{@link ConvertMode#PARSE}: detects WKT / EWKT (SRID strip) / GeoJSON
     *     / WKB hex / ES geo_point object|array|string formats and converts to
     *     standard WKT.
     * </ul>
     *
     * <p>{@code null} and empty inputs are returned as-is in both modes.
     *
     * @param input geometry string in WKT (always supported) or, in PARSE mode,
     *     EWKT / GeoJSON / WKB hex / ES geo_point variants
     * @return WKT string, or null/empty if input is null/empty
     * @throws MilvusConnectorException if {@link ConvertMode#PARSE} mode is in
     *     effect and the format is unrecognized, or VTS cannot losslessly convert
     *     it (e.g. WKB with M coordinates)
     */
    public String convertToWkt(String input) {
        if (mode == ConvertMode.PASSTHROUGH) {
            // Hot path for Milvus → Milvus and any other source that already
            // produces destination-compatible WKT. One branch and a return —
            // null / empty / arbitrary strings all flow through unchanged,
            // which is exactly the contract PASSTHROUGH promises.
            return input;
        }
        if (input == null || input.isEmpty()) {
            return input;
        }
        try {
            return doConvertToWkt(input.trim());
        } catch (Exception e) {
            // Always wrap with the original input so the error has caller-level
            // context regardless of how deep the recursion went. In particular,
            // EWKT strip can leave {@code trimmed} empty after the SRID prefix,
            // at which point an inner "Unrecognized geometry format" throw has
            // lost the meaningful value — we restore it here. Local sub-context
            // the inner exception carried (e.g. "4-element GeoJSON coordinate:
            // [0,0,0,1]") is preserved in the appended reason.
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "Failed to convert geometry '" + input + "': " + e.getMessage());
        }
    }

    /**
     * Convert raw WKB bytes to WKT.
     *
     * <p>Reachable from two paths in PARSE mode:
     * <ul>
     *   <li>{@link #convertWkbHex(String)} for hex-encoded WKB strings.</li>
     *   <li>{@link #convert(Object)} when the sink receives a {@code ByteBuffer}
     *       (duplicated and copied into a byte array first).</li>
     * </ul>
     *
     * <p>The bytes go through JTS {@code WKBReader}, which preserves Z
     * coordinates; the 3D-aware writer ensures Z survives the trip to WKT.
     *
     * @throws MilvusConnectorException if the WKB carries an M coordinate flag
     *     (JTS cannot losslessly represent M, so VTS refuses rather than silently
     *     dropping it)
     */
    public String convertWkbBytes(byte[] wkb) {
        if (wkb == null || wkb.length == 0) {
            return null;
        }
        if (hasMCoordinateFlag(wkb)) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "WKB with M coordinate cannot be losslessly converted to WKT by VTS "
                            + "(JTS Coordinate does not represent M). "
                            + "Please convert to WKT format at the source.");
        }
        try {
            Geometry geom = new WKBReader(GEOMETRY_FACTORY).read(wkb);
            return writeWkt3D(geom);
        } catch (ParseException e) {
            // The most common reason this fails on real data is a PostGIS/Oracle
            // extended geometry type (CompoundCurve, CircularString, Triangle,
            // PolyhedralSurface, TIN, etc.) — JTS WKBReader only supports the
            // OGC SFS 1.1 core 7 types. The hint nudges users toward the WKT
            // path so the destination can decide whether to accept the type,
            // matching the same architectural stance as the WKT pass-through.
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "Failed to parse WKB bytes (" + e.getMessage() + "). "
                            + "If this is a PostGIS extended geometry type "
                            + "(CompoundCurve, CircularString, CurvePolygon, "
                            + "MultiCurve, MultiSurface, Triangle, TIN, "
                            + "PolyhedralSurface), JTS does not support parsing "
                            + "it from WKB. Send the geometry as a WKT string "
                            + "from the source (e.g., ST_AsText() in PostgreSQL) "
                            + "so the destination can decide whether to accept it.");
        }
    }

    private String doConvertToWkt(String trimmed) {
        // EWKT: "SRID=4326;POINT(1 1)" → strip SRID prefix, recurse on remainder.
        // regionMatches avoids allocating an upper-cased copy of every input record.
        if (trimmed.regionMatches(true, 0, "SRID=", 0, 5)) {
            int semicolonIdx = trimmed.indexOf(';');
            if (semicolonIdx >= 0) {
                return doConvertToWkt(trimmed.substring(semicolonIdx + 1).trim());
            }
        }

        // WKT: pass through unchanged. VTS does not validate WKT — Milvus owns that.
        // This includes all 3D / M / ZM variants: POINT Z (...), POINTZ(...),
        // POINT M (...), POINT ZM (...), inline POINT (1 2 3), GEOMETRYCOLLECTION ..., etc.
        if (isWkt(trimmed)) {
            return trimmed;
        }

        // JSON object: GeoJSON or ES geo_point {lat, lon}
        if (trimmed.startsWith("{")) {
            return convertJsonObject(trimmed);
        }

        // JSON array: ES geo_point [lon, lat] (or [lon, lat, alt] for 3D)
        if (trimmed.startsWith("[")) {
            return convertJsonArray(trimmed);
        }

        // Bare "lat,lon" / "lon,lat" string (ES geo_point convention by default)
        if (trimmed.indexOf(',') > 0) {
            String result = tryConvertCoordinateString(trimmed);
            if (result != null) {
                return result;
            }
        }

        // WKB/EWKB hex
        if (isLikelyWkbHex(trimmed)) {
            return convertWkbHex(trimmed);
        }

        // No format matched → error to prevent silent inconsistency
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Unrecognized geometry format");
    }

    // ---- Format detection ----

    /**
     * OGC SFS 1.1 core geometry types. VTS supports these in <b>all</b> input
     * paths: WKT pass-through, WKB / EWKB hex (via JTS), GeoJSON (via JTS),
     * and ES geo_point variants.
     *
     * <p>Order matters: longer keywords (e.g. MULTIPOINT) must precede their
     * substrings (POINT) so {@code regionMatches} matches the longest keyword
     * first.
     */
    private static final String[] WKT_CORE_KEYWORDS = {
            "GEOMETRYCOLLECTION", "MULTILINESTRING", "MULTIPOLYGON", "MULTIPOINT",
            "LINESTRING", "POLYGON", "POINT"
    };

    /**
     * OGC SFS 1.2 extended geometry types. VTS does <b>not</b> parse these —
     * JTS WKBReader does not support them, so they cannot flow through the
     * WKB or GeoJSON conversion paths.
     *
     * <p>However, when a source provides such a type as a WKT <b>string</b>,
     * VTS hands it to the destination unchanged so the destination can decide
     * whether to accept it. Today Milvus's go-geom rejects these types and
     * the user gets an error from Milvus; if a future Milvus version (or any
     * other sink) gains support, no VTS change is required.
     *
     * <p>Including these in {@link #isWkt(String)} is the same architectural
     * stance as 3D pass-through: VTS does not pre-empt the destination's
     * capabilities. The WKB ByteBuffer path still has to fail in
     * {@link #convertWkbBytes(byte[])} because JTS literally cannot parse
     * these binary forms — that is a "VTS cannot losslessly convert" failure
     * (per the pure-transport principle), not a destination pre-validation.
     */
    private static final String[] WKT_EXTENDED_KEYWORDS = {
            "POLYHEDRALSURFACE", "CIRCULARSTRING", "COMPOUNDCURVE",
            "CURVEPOLYGON", "MULTISURFACE", "MULTICURVE",
            "TRIANGLE", "TIN"
    };

    private static boolean isWkt(String s) {
        // regionMatches with ignoreCase=true: zero allocation, no toUpperCase().
        // The string is shorter than the keyword → returns false (no IOOBE).
        for (String keyword : WKT_CORE_KEYWORDS) {
            if (s.regionMatches(true, 0, keyword, 0, keyword.length())) {
                return true;
            }
        }
        for (String keyword : WKT_EXTENDED_KEYWORDS) {
            if (s.regionMatches(true, 0, keyword, 0, keyword.length())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Detect likely WKB/EWKB hex strings.
     * Checks: even length, minimum size, starts with byte order marker (00/01), all hex chars.
     */
    private static boolean isLikelyWkbHex(String s) {
        if (s.length() < 10 || s.length() % 2 != 0) return false;
        // WKB must start with byte order: 00 (big-endian) or 01 (little-endian)
        if (!(s.startsWith("00") || s.startsWith("01"))) return false;
        for (int i = 0; i < s.length(); i++) {
            if (Character.digit(s.charAt(i), 16) < 0) return false;
        }
        return true;
    }

    /**
     * Detect the M-coordinate flag in a WKB/EWKB header.
     *
     * <p>WKB geometryType layout:
     * <ul>
     *   <li>ISO WKB:  type values 1000-1999 = Z, 2000-2999 = M, 3000-3999 = ZM</li>
     *   <li>EWKB:     bit 0x40000000 = M flag (in addition to base type)</li>
     * </ul>
     */
    private static boolean hasMCoordinateFlag(byte[] wkb) {
        if (wkb.length < 5) return false;
        boolean littleEndian = wkb[0] == 1;
        // bytes 1..4 are the geometryType (uint32)
        long geomType;
        if (littleEndian) {
            geomType = (Byte.toUnsignedLong(wkb[1]))
                    | (Byte.toUnsignedLong(wkb[2]) << 8)
                    | (Byte.toUnsignedLong(wkb[3]) << 16)
                    | (Byte.toUnsignedLong(wkb[4]) << 24);
        } else {
            geomType = (Byte.toUnsignedLong(wkb[1]) << 24)
                    | (Byte.toUnsignedLong(wkb[2]) << 16)
                    | (Byte.toUnsignedLong(wkb[3]) << 8)
                    | (Byte.toUnsignedLong(wkb[4]));
        }
        // EWKB M flag
        if ((geomType & 0x40000000L) != 0) return true;
        // ISO WKB: 2000-2999 (M only) or 3000-3999 (ZM)
        long base = geomType & 0x0FFFFFFFL;
        return (base >= 2000 && base < 4000);
    }

    // ---- Format converters ----

    private static String convertJsonObject(String trimmed) {
        JsonObject obj = JsonParser.parseString(trimmed).getAsJsonObject();
        if (obj.has("type")) {
            return writeWkt3D(geoJsonToGeometry(obj));
        } else if (obj.has("lat") && obj.has("lon")) {
            return writeWkt3D(GEOMETRY_FACTORY.createPoint(
                    new Coordinate(obj.get("lon").getAsDouble(), obj.get("lat").getAsDouble())));
        }
        // The original input is attached by the outer convertToWkt catch.
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Unrecognized JSON geometry format");
    }

    private static String convertJsonArray(String trimmed) {
        JsonArray arr = JsonParser.parseString(trimmed).getAsJsonArray();
        if (arr.size() == 2) {
            return writeWkt3D(GEOMETRY_FACTORY.createPoint(
                    new Coordinate(arr.get(0).getAsDouble(), arr.get(1).getAsDouble())));
        }
        if (arr.size() == 3) {
            // [lon, lat, alt] — preserve Z
            return writeWkt3D(GEOMETRY_FACTORY.createPoint(new Coordinate(
                    arr.get(0).getAsDouble(),
                    arr.get(1).getAsDouble(),
                    arr.get(2).getAsDouble())));
        }
        // VTS capability boundary: the converter only knows how to produce 2D
        // and 3D Points. We deliberately phrase this as a VTS limitation, not
        // as "your data is wrong". The original input is attached by the
        // outer convertToWkt catch, so don't repeat it here.
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "VTS cannot convert " + arr.size() + "-element geo_point array (only 2D/3D supported)");
    }

    /**
     * Try to parse "x,y" string format. The interpretation of which number is lat
     * vs lon is controlled by this instance's {@link #stringCoordOrder}, which
     * defaults to {@link CoordinateOrder#LAT_LON} (Elasticsearch geo_point
     * convention).
     *
     * <p>Returns WKT if successful, null if the string is not a numeric pair.
     * 3D form "a,b,c" is supported when all parts are numeric.
     */
    private String tryConvertCoordinateString(String trimmed) {
        String[] parts = trimmed.split(",");
        if (parts.length < 2 || parts.length > 3) {
            return null;
        }
        double[] nums = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                nums[i] = Double.parseDouble(parts[i].trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        double lat;
        double lon;
        if (stringCoordOrder == CoordinateOrder.LAT_LON) {
            lat = nums[0];
            lon = nums[1];
        } else {
            lon = nums[0];
            lat = nums[1];
        }
        Coordinate coord = (parts.length == 3)
                ? new Coordinate(lon, lat, nums[2])
                : new Coordinate(lon, lat);
        return writeWkt3D(GEOMETRY_FACTORY.createPoint(coord));
    }

    private String convertWkbHex(String trimmed) {
        return convertWkbBytes(WKBReader.hexToBytes(trimmed));
    }

    // ---- GeoJSON → JTS Geometry ----
    //
    // Intentionally minimal. Malformed input (missing "type", missing
    // "coordinates", wrong field shape, etc.) is the source connector's
    // responsibility, not VTS's — VTS's contract is "correctly transform
    // well-formed input from format A to format B", nothing more. A NPE or
    // gson IllegalStateException from direct field access gets wrapped by
    // the outer catch in convertToWkt() and surfaced as "Failed to convert
    // geometry: <input> (<cause>)", which is enough for the user to see
    // which record broke. Adding per-field defensive checks would be
    // scope creep into data-quality assurance and a per-record performance
    // tax on a hot path.

    private static Geometry geoJsonToGeometry(JsonObject obj) {
        String type = obj.get("type").getAsString();
        switch (type) {
            case "Point":
                return GEOMETRY_FACTORY.createPoint(
                        toCoordinate(obj.getAsJsonArray("coordinates")));
            case "MultiPoint": {
                JsonArray coords = obj.getAsJsonArray("coordinates");
                Point[] points = new Point[coords.size()];
                for (int i = 0; i < coords.size(); i++) {
                    points[i] = GEOMETRY_FACTORY.createPoint(
                            toCoordinate(coords.get(i).getAsJsonArray()));
                }
                return GEOMETRY_FACTORY.createMultiPoint(points);
            }
            case "LineString":
                return GEOMETRY_FACTORY.createLineString(
                        toCoordinateArray(obj.getAsJsonArray("coordinates")));
            case "MultiLineString": {
                JsonArray lines = obj.getAsJsonArray("coordinates");
                LineString[] lineStrings = new LineString[lines.size()];
                for (int i = 0; i < lines.size(); i++) {
                    lineStrings[i] = GEOMETRY_FACTORY.createLineString(
                            toCoordinateArray(lines.get(i).getAsJsonArray()));
                }
                return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
            }
            case "Polygon":
                return createPolygon(obj.getAsJsonArray("coordinates"));
            case "MultiPolygon": {
                JsonArray polygons = obj.getAsJsonArray("coordinates");
                Polygon[] polys = new Polygon[polygons.size()];
                for (int i = 0; i < polygons.size(); i++) {
                    polys[i] = createPolygon(polygons.get(i).getAsJsonArray());
                }
                return GEOMETRY_FACTORY.createMultiPolygon(polys);
            }
            case "GeometryCollection": {
                JsonArray geometries = obj.getAsJsonArray("geometries");
                Geometry[] geoms = new Geometry[geometries.size()];
                for (int i = 0; i < geometries.size(); i++) {
                    geoms[i] = geoJsonToGeometry(geometries.get(i).getAsJsonObject());
                }
                return GEOMETRY_FACTORY.createGeometryCollection(geoms);
            }
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                        "Unsupported GeoJSON type: " + type);
        }
    }

    // ---- Coordinate helpers ----

    /**
     * Parse a GeoJSON coordinate array to JTS Coordinate. Supports 2D
     * [lon, lat] and 3D [lon, lat, alt]. Anything else is a VTS capability
     * boundary (JTS {@code Coordinate} has no slot for a 4th dimension).
     */
    private static Coordinate toCoordinate(JsonArray arr) {
        if (arr.size() == 2) {
            return new Coordinate(arr.get(0).getAsDouble(), arr.get(1).getAsDouble());
        }
        if (arr.size() == 3) {
            return new Coordinate(
                    arr.get(0).getAsDouble(),
                    arr.get(1).getAsDouble(),
                    arr.get(2).getAsDouble());
        }
        // VTS capability boundary: JTS Coordinate only represents 2D/3D. We
        // deliberately phrase this as a VTS limitation rather than "your data
        // is wrong".
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "VTS cannot convert " + arr.size() + "-element GeoJSON coordinate (only 2D/3D supported): " + arr);
    }

    private static Coordinate[] toCoordinateArray(JsonArray coords) {
        Coordinate[] result = new Coordinate[coords.size()];
        for (int i = 0; i < coords.size(); i++) {
            result[i] = toCoordinate(coords.get(i).getAsJsonArray());
        }
        return result;
    }

    private static Polygon createPolygon(JsonArray rings) {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(
                toCoordinateArray(rings.get(0).getAsJsonArray()));
        LinearRing[] holes = new LinearRing[rings.size() - 1];
        for (int i = 1; i < rings.size(); i++) {
            holes[i - 1] = GEOMETRY_FACTORY.createLinearRing(
                    toCoordinateArray(rings.get(i).getAsJsonArray()));
        }
        return GEOMETRY_FACTORY.createPolygon(shell, holes);
    }
}
