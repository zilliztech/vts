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
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
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

/**
 * Converts various geometry string formats to WKT (Well-Known Text) for Milvus.
 *
 * Supported input formats:
 * <ul>
 *   <li>WKT: "POINT (1 2)", "LINESTRING (0 0, 1 1)" etc.</li>
 *   <li>EWKT: "SRID=4326;POINT (1 2)" (SRID prefix stripped)</li>
 *   <li>GeoJSON: {"type":"Point","coordinates":[1,2]}</li>
 *   <li>WKB/EWKB hex: "0101000000..." (binary geometry in hex encoding)</li>
 *   <li>ES geo_point object: {"lat":31.23,"lon":121.47}</li>
 *   <li>ES geo_point string: "31.23,121.47" (lat,lon order)</li>
 *   <li>ES geo_point array: [121.47,31.23] (lon,lat order, GeoJSON convention)</li>
 * </ul>
 *
 * This converter is strict: unrecognized formats throw an exception
 * rather than silently passing through, to prevent data inconsistency during migration.
 * 3D coordinates (Z values) are also rejected.
 *
 * Uses JTS (Java Topology Suite) for WKB parsing and WKT formatting.
 */
public class GeometryConverter {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private GeometryConverter() {}

    /**
     * Convert a geometry string in any supported format to WKT.
     *
     * @param input geometry string in WKT, EWKT, GeoJSON, WKB hex, or ES geo_point format
     * @return WKT string, or null/empty if input is null/empty
     * @throws MilvusConnectorException if the format is unrecognized or contains 3D coordinates
     */
    public static String convertToWkt(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        String trimmed = input.trim();

        // EWKT: "SRID=4326;POINT(1 1)" → strip SRID prefix
        if (trimmed.toUpperCase().startsWith("SRID=")) {
            int semicolonIdx = trimmed.indexOf(';');
            if (semicolonIdx >= 0) {
                String wkt = trimmed.substring(semicolonIdx + 1).trim();
                rejectWkt3D(wkt);
                return wkt;
            }
        }

        // WKT: starts with geometry keyword → pass through
        if (isWkt(trimmed)) {
            rejectWkt3D(trimmed);
            return trimmed;
        }

        // JSON object: GeoJSON or ES geo_point {lat, lon}
        if (trimmed.startsWith("{")) {
            return convertJsonObject(trimmed);
        }

        // JSON array: ES geo_point [lon, lat] (GeoJSON order)
        if (trimmed.startsWith("[")) {
            return convertJsonArray(trimmed);
        }

        // ES geo_point string format: "lat,lon" (e.g. "31.2304,121.4737")
        if (trimmed.indexOf(',') > 0) {
            String result = tryConvertLatLonString(trimmed);
            if (result != null) {
                return result;
            }
        }

        // WKB/EWKB hex
        if (isLikelyWkbHex(trimmed)) {
            return convertWkbHex(trimmed);
        }

        // No format matched → error to prevent data inconsistency
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Unrecognized geometry format: " + trimmed);
    }

    // ---- Format detection ----

    private static final String[] WKT_KEYWORDS = {
            "GEOMETRYCOLLECTION", "MULTILINESTRING", "MULTIPOLYGON", "MULTIPOINT",
            "LINESTRING", "POLYGON", "POINT"
    };

    private static boolean isWkt(String s) {
        String upper = s.toUpperCase();
        for (String keyword : WKT_KEYWORDS) {
            if (upper.startsWith(keyword)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reject WKT with 3D (Z), M, or ZM coordinate dimensions.
     * PostGIS can produce: "POINT Z (1 2 3)", "POINTZ(1 2 3)", "POINT M (1 2 3)",
     * "POINT ZM (1 2 3 4)" — these would be silently truncated to 2D if passed through.
     */
    private static void rejectWkt3D(String wkt) {
        String upper = wkt.toUpperCase().trim();
        for (String keyword : WKT_KEYWORDS) {
            if (upper.startsWith(keyword)) {
                String rest = upper.substring(keyword.length()).trim();
                if (rest.startsWith("Z") || rest.startsWith("M")) {
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                            "3D/M geometry is not supported for migration: " + wkt);
                }
                return;
            }
        }
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

    // ---- Format converters ----

    private static String convertJsonObject(String trimmed) {
        JsonObject obj = JsonParser.parseString(trimmed).getAsJsonObject();
        if (obj.has("type")) {
            return toWkt(geoJsonToGeometry(obj));
        } else if (obj.has("lat") && obj.has("lon")) {
            return toWkt(GEOMETRY_FACTORY.createPoint(
                    new Coordinate(obj.get("lon").getAsDouble(), obj.get("lat").getAsDouble())));
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Unrecognized JSON geometry format: " + trimmed);
    }

    private static String convertJsonArray(String trimmed) {
        JsonArray arr = JsonParser.parseString(trimmed).getAsJsonArray();
        if (arr.size() == 2) {
            return toWkt(GEOMETRY_FACTORY.createPoint(
                    new Coordinate(arr.get(0).getAsDouble(), arr.get(1).getAsDouble())));
        }
        if (arr.size() > 2) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "3D coordinates are not supported for migration, got " + arr.size()
                            + "D coordinates: " + trimmed);
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                "Invalid geo_point array, expected [lon, lat] but got " + arr.size()
                        + " elements: " + trimmed);
    }

    /**
     * Try to parse "lat,lon" string format. Returns WKT if successful, null otherwise.
     * Also detects and rejects 3D "lat,lon,alt" format.
     */
    private static String tryConvertLatLonString(String trimmed) {
        String[] parts = trimmed.split(",");
        if (parts.length == 2) {
            try {
                double lat = Double.parseDouble(parts[0].trim());
                double lon = Double.parseDouble(parts[1].trim());
                return toWkt(GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat)));
            } catch (NumberFormatException e) {
                return null;
            }
        } else if (parts.length > 2) {
            boolean allNumbers = true;
            for (String part : parts) {
                try {
                    Double.parseDouble(part.trim());
                } catch (NumberFormatException e) {
                    allNumbers = false;
                    break;
                }
            }
            if (allNumbers) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                        "3D coordinates are not supported for migration, got " + parts.length
                                + "D coordinates: " + trimmed);
            }
        }
        return null;
    }

    private static String convertWkbHex(String trimmed) {
        try {
            Geometry geom = new WKBReader(GEOMETRY_FACTORY).read(WKBReader.hexToBytes(trimmed));
            rejectZCoordinate(geom);
            return toWkt(geom);
        } catch (ParseException e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "Failed to parse WKB hex: " + e.getMessage());
        }
    }

    // ---- WKT output ----

    private static String toWkt(Geometry geometry) {
        return new WKTWriter().write(geometry);
    }

    // ---- GeoJSON → JTS Geometry ----

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
     * Parse a GeoJSON coordinate array to JTS Coordinate.
     * Rejects 3D coordinates to prevent silent data loss during migration.
     */
    private static Coordinate toCoordinate(JsonArray arr) {
        if (arr.size() > 2) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                    "3D coordinates are not supported for migration, got " + arr.size()
                            + "D coordinates: " + arr);
        }
        return new Coordinate(arr.get(0).getAsDouble(), arr.get(1).getAsDouble());
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

    // ---- Validation ----

    /**
     * Reject 3D geometry (with Z coordinates) to prevent silent data loss during migration.
     */
    private static void rejectZCoordinate(Geometry geometry) {
        for (Coordinate coord : geometry.getCoordinates()) {
            if (!Double.isNaN(coord.getZ())) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE,
                        "3D geometry (Z coordinate) is not supported for migration. "
                                + "Input contains Z value: " + coord.getZ());
            }
        }
    }
}
