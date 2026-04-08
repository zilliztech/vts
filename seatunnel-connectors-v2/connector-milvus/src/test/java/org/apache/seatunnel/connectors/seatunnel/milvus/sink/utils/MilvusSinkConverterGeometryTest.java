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

import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GeometryConverter}.
 *
 * <p>Principle under test: VTS does pure data transport. WKT input is passed through
 * unchanged (Milvus owns WKT validation). Non-WKT input is converted to WKT losslessly,
 * preserving Z coordinates via 3D-aware writers. Conversion that cannot be lossless
 * (e.g. WKB with M) throws explicitly with the reason being a VTS limitation, not
 * a Milvus rejection.
 */
public class MilvusSinkConverterGeometryTest {

    // ---- WKT pass-through. These tests use PASSTHROUGH explicitly so they
    //      lock in the byte-for-byte behavior of the production default mode.
    //      They would also pass under PARSE because PARSE also pass-throughs
    //      WKT inputs unchanged — but using PASSTHROUGH here is the honest
    //      contract these tests are documenting.

    @Test
    void testWkt2DPassthrough() {
        // WKT is not parsed — Milvus is the source of truth on WKT syntax
        assertEquals("POINT (1 2)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT (1 2)"));
        assertEquals("POINT(1 2)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT(1 2)"));
        assertEquals("LINESTRING(0 0, 1 1)", GeometryConverter.PASSTHROUGH.convertToWkt("LINESTRING(0 0, 1 1)"));
        assertEquals("POLYGON((0 0, 1 0, 1 1, 0 0))",
                GeometryConverter.PASSTHROUGH.convertToWkt("POLYGON((0 0, 1 0, 1 1, 0 0))"));
        assertEquals("MULTIPOINT((0 0), (1 1))",
                GeometryConverter.PASSTHROUGH.convertToWkt("MULTIPOINT((0 0), (1 1))"));
        assertEquals("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))",
                GeometryConverter.PASSTHROUGH.convertToWkt("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))"));
    }

    @Test
    void testWktEmptyGeometriesPassthrough() {
        assertEquals("POINT EMPTY", GeometryConverter.PASSTHROUGH.convertToWkt("POINT EMPTY"));
        assertEquals("LINESTRING EMPTY", GeometryConverter.PASSTHROUGH.convertToWkt("LINESTRING EMPTY"));
        assertEquals("GEOMETRYCOLLECTION EMPTY", GeometryConverter.PASSTHROUGH.convertToWkt("GEOMETRYCOLLECTION EMPTY"));
    }

    @Test
    void testWkt3DZPassthrough() {
        // 3D WKT must pass through with Z preserved. Milvus 2.6 stores Z values
        // (whether spatial queries honor Z is the user's concern, not VTS's).
        assertEquals("POINT Z (1 2 3)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT Z (1 2 3)"));
        assertEquals("POINTZ(1 2 3)", GeometryConverter.PASSTHROUGH.convertToWkt("POINTZ(1 2 3)"));
        assertEquals("POINT (1 2 3)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT (1 2 3)"));
        assertEquals("LINESTRING Z (0 0 0, 1 1 1)",
                GeometryConverter.PASSTHROUGH.convertToWkt("LINESTRING Z (0 0 0, 1 1 1)"));
        assertEquals("POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))",
                GeometryConverter.PASSTHROUGH.convertToWkt("POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))"));
    }

    @Test
    void testWktMAndZMPassthrough() {
        // M and ZM forms also pass through unchanged
        assertEquals("POINT M (1 2 3)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT M (1 2 3)"));
        assertEquals("POINT ZM (1 2 3 4)", GeometryConverter.PASSTHROUGH.convertToWkt("POINT ZM (1 2 3 4)"));
        assertEquals("POINTM(1 2 3)", GeometryConverter.PASSTHROUGH.convertToWkt("POINTM(1 2 3)"));
        assertEquals("POINTZM(1 2 3 4)", GeometryConverter.PASSTHROUGH.convertToWkt("POINTZM(1 2 3 4)"));
    }

    @Test
    void testWkt3DMultiTypesPassthrough() {
        // 3D Multi* WKT must also pass through unchanged.
        assertEquals("MULTIPOINT Z ((0 0 0), (1 1 1))",
                GeometryConverter.PASSTHROUGH.convertToWkt("MULTIPOINT Z ((0 0 0), (1 1 1))"));
        assertEquals("MULTILINESTRING Z ((0 0 0, 1 1 1), (2 2 2, 3 3 3))",
                GeometryConverter.PASSTHROUGH.convertToWkt("MULTILINESTRING Z ((0 0 0, 1 1 1), (2 2 2, 3 3 3))"));
        assertEquals("MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))",
                GeometryConverter.PASSTHROUGH.convertToWkt("MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))"));
        assertEquals("GEOMETRYCOLLECTION Z (POINT Z (1 1 1), LINESTRING Z (0 0 0, 1 1 1))",
                GeometryConverter.PASSTHROUGH.convertToWkt(
                        "GEOMETRYCOLLECTION Z (POINT Z (1 1 1), LINESTRING Z (0 0 0, 1 1 1))"));
    }

    @Test
    void testWktNegativeCoordinatesPassthrough() {
        // Sign handling: negatives are common in real geo data (anything west of
        // Greenwich or south of equator). Pass-through must not strip signs.
        assertEquals("POINT (-74.006 40.7128)",
                GeometryConverter.PASSTHROUGH.convertToWkt("POINT (-74.006 40.7128)"));
        assertEquals("LINESTRING (-1 -1, -2 -2)",
                GeometryConverter.PASSTHROUGH.convertToWkt("LINESTRING (-1 -1, -2 -2)"));
        assertEquals("POINT Z (-74.006 40.7128 -10.5)",
                GeometryConverter.PASSTHROUGH.convertToWkt("POINT Z (-74.006 40.7128 -10.5)"));
    }

    @Test
    void testWktExtendedTypesPassthrough() {
        // OGC SFS 1.2 extended types: VTS passes them to the destination
        // unchanged. Whether the destination accepts them is its concern.
        assertEquals("CIRCULARSTRING (0 0, 1 1, 2 0)",
                GeometryConverter.PASSTHROUGH.convertToWkt("CIRCULARSTRING (0 0, 1 1, 2 0)"));
        assertEquals("COMPOUNDCURVE (CIRCULARSTRING (0 0, 1 1, 2 0), (2 0, 3 0))",
                GeometryConverter.PASSTHROUGH.convertToWkt(
                        "COMPOUNDCURVE (CIRCULARSTRING (0 0, 1 1, 2 0), (2 0, 3 0))"));
        assertEquals("CURVEPOLYGON (CIRCULARSTRING (0 0, 1 0, 1 1, 0 1, 0 0))",
                GeometryConverter.PASSTHROUGH.convertToWkt(
                        "CURVEPOLYGON (CIRCULARSTRING (0 0, 1 0, 1 1, 0 1, 0 0))"));
        assertEquals("MULTICURVE ((0 0, 1 1), CIRCULARSTRING (2 2, 3 3, 4 2))",
                GeometryConverter.PASSTHROUGH.convertToWkt(
                        "MULTICURVE ((0 0, 1 1), CIRCULARSTRING (2 2, 3 3, 4 2))"));
        assertEquals("MULTISURFACE (((0 0, 1 0, 1 1, 0 0)))",
                GeometryConverter.PASSTHROUGH.convertToWkt("MULTISURFACE (((0 0, 1 0, 1 1, 0 0)))"));
        assertEquals("TRIANGLE ((0 0, 1 0, 0 1, 0 0))",
                GeometryConverter.PASSTHROUGH.convertToWkt("TRIANGLE ((0 0, 1 0, 0 1, 0 0))"));
        assertEquals("TIN (((0 0 0, 0 1 0, 1 0 0, 0 0 0)))",
                GeometryConverter.PASSTHROUGH.convertToWkt("TIN (((0 0 0, 0 1 0, 1 0 0, 0 0 0)))"));
        assertEquals("POLYHEDRALSURFACE (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))",
                GeometryConverter.PASSTHROUGH.convertToWkt(
                        "POLYHEDRALSURFACE (((0 0 0, 1 0 0, 1 1 0, 0 0 0)))"));
    }

    @Test
    void testPassthroughDoesNotStripSrid() {
        // Critical contract test: PASSTHROUGH literally does nothing, so the
        // SRID prefix from PostGIS ST_AsEWKT is NOT stripped. Milvus would
        // reject this — that's why users with EWKT sources need parse mode.
        assertEquals("SRID=4326;POINT(1 2)",
                GeometryConverter.PASSTHROUGH.convertToWkt("SRID=4326;POINT(1 2)"));
        assertEquals("SRID=4326;CIRCULARSTRING (0 0, 1 1, 2 0)",
                GeometryConverter.PASSTHROUGH.convertToWkt("SRID=4326;CIRCULARSTRING (0 0, 1 1, 2 0)"));
    }

    @Test
    void testPassthroughDoesNotConvertGeoJson() {
        // PASSTHROUGH does not interpret GeoJSON. The string flows unchanged
        // to Milvus, which rejects it as invalid WKT. Users with GeoJSON
        // sources (e.g., Elasticsearch geo_shape) need parse mode.
        String geoJson = "{\"type\":\"Point\",\"coordinates\":[1,2]}";
        assertEquals(geoJson, GeometryConverter.PASSTHROUGH.convertToWkt(geoJson));
    }

    @Test
    void testPassthroughDoesNotConvertWkbHex() {
        // Same contract for WKB hex strings.
        String wkbHex = "0101000000000000000000F03F000000000000F03F";
        assertEquals(wkbHex, GeometryConverter.PASSTHROUGH.convertToWkt(wkbHex));
    }

    @Test
    void testPassthroughZeroProcessingOnArbitraryString() {
        // The defining contract of PASSTHROUGH: any string in, the same string out.
        // Even garbage. The destination is responsible for rejecting invalid input.
        assertEquals("not a geometry at all",
                GeometryConverter.PASSTHROUGH.convertToWkt("not a geometry at all"));
        assertEquals("   leading whitespace not even trimmed   ",
                GeometryConverter.PASSTHROUGH.convertToWkt("   leading whitespace not even trimmed   "));
    }

    @Test
    void testWktExtendedTypesEwktParseStripsSrid() {
        // SRID stripping requires PARSE mode (passthrough leaves SRID alone)
        assertEquals("CIRCULARSTRING (0 0, 1 1, 2 0)",
                GeometryConverter.PARSE.convertToWkt(
                        "SRID=4326;CIRCULARSTRING (0 0, 1 1, 2 0)"));
    }

    @Test
    void testWktExtendedTypesCaseInsensitive() {
        // Case-insensitive WKT detection (in PARSE mode); the original case is
        // preserved because the WKT branch is itself a pass-through within parse.
        assertEquals("circularstring (0 0, 1 1, 2 0)",
                GeometryConverter.PARSE.convertToWkt("circularstring (0 0, 1 1, 2 0)"));
    }

    // ---- EWKT (SRID stripping) ----

    @Test
    void testEwktStripsSrid() {
        assertEquals("POINT(121.4737 31.2304)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;POINT(121.4737 31.2304)"));
        assertEquals("POLYGON((0 0, 1 0, 1 1, 0 0))",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 0))"));
    }

    @Test
    void testEwkt3DStripsSridPreservesZ() {
        assertEquals("POINT Z (1 2 3)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;POINT Z (1 2 3)"));
        assertEquals("POINTZ(1 2 3)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;POINTZ(1 2 3)"));
    }

    @Test
    void testEwkt3DLineStringAndPolygon() {
        // Confirm SRID strip handles complex 3D shapes, not just POINT
        assertEquals("LINESTRING Z (0 0 0, 1 1 1)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;LINESTRING Z (0 0 0, 1 1 1)"));
        assertEquals("POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))"));
    }

    @Test
    void testEwktLowercaseSrid() {
        // PostGIS output is case-insensitive in practice; user data may be either case.
        // regionMatches(ignoreCase=true) should accept all variants without re-parsing.
        assertEquals("POINT(1 2)",
                GeometryConverter.PARSE.convertToWkt("srid=4326;POINT(1 2)"));
        assertEquals("POINT(1 2)",
                GeometryConverter.PARSE.convertToWkt("Srid=4326;POINT(1 2)"));
        assertEquals("POINT Z (1 2 3)",
                GeometryConverter.PARSE.convertToWkt("srid=4326;POINT Z (1 2 3)"));
    }

    @Test
    void testEwktWithLatLonString() {
        // SRID prefix + lat,lon string → strip SRID, then parse the lat,lon
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;31.2304,121.4737"));
    }

    @Test
    void testEwktWithGeoJson() {
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.PARSE.convertToWkt("SRID=4326;{\"type\":\"Point\",\"coordinates\":[121.4737,31.2304]}"));
    }

    @Test
    void testEwktEmptyContentThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt("SRID=4326;"));
    }

    // ---- GeoJSON ----

    @Test
    void testGeoJsonPoint() {
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.PARSE.convertToWkt("{\"type\":\"Point\",\"coordinates\":[121.4737,31.2304]}"));
    }

    @Test
    void testGeoJsonPoint3DPreservesZ() {
        // GeoJSON 3D point — Z must survive
        String result = GeometryConverter.PARSE.convertToWkt(
                "{\"type\":\"Point\",\"coordinates\":[121.47,31.23,10.5]}");
        assertTrue(result.contains("10.5"), "Z value 10.5 must be in output, got: " + result);
        assertTrue(result.toUpperCase().contains("Z"), "Output must indicate 3D, got: " + result);
    }

    @Test
    void testGeoJsonLineString() {
        assertEquals("LINESTRING (0 0, 1 1, 2 0)",
                GeometryConverter.PARSE.convertToWkt("{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1],[2,0]]}"));
    }

    @Test
    void testGeoJsonLineString3DPreservesZ() {
        String result = GeometryConverter.PARSE.convertToWkt(
                "{\"type\":\"LineString\",\"coordinates\":[[0,0,1],[1,1,2]]}");
        assertTrue(result.contains("0 0 1") && result.contains("1 1 2"),
                "Z values must be preserved in linestring, got: " + result);
    }

    @Test
    void testGeoJsonPolygon() {
        assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
                GeometryConverter.PARSE.convertToWkt("{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"));
    }

    @Test
    void testGeoJsonMultiPoint() {
        assertEquals("MULTIPOINT ((0 0), (1 1))",
                GeometryConverter.PARSE.convertToWkt("{\"type\":\"MultiPoint\",\"coordinates\":[[0,0],[1,1]]}"));
    }

    @Test
    void testGeoJsonMultiLineString() {
        String geoJson = "{\"type\":\"MultiLineString\",\"coordinates\":[[[0,0],[1,1]],[[2,2],[3,3]]]}";
        assertEquals("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
                GeometryConverter.PARSE.convertToWkt(geoJson));
    }

    @Test
    void testGeoJsonMultiPolygon() {
        String geoJson = "{\"type\":\"MultiPolygon\",\"coordinates\":["
                + "[[[0,0],[1,0],[1,1],[0,0]]],"
                + "[[[2,2],[3,2],[3,3],[2,2]]]"
                + "]}";
        assertEquals("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
                GeometryConverter.PARSE.convertToWkt(geoJson));
    }

    @Test
    void testGeoJsonGeometryCollection() {
        String geoJson = "{\"type\":\"GeometryCollection\",\"geometries\":["
                + "{\"type\":\"Point\",\"coordinates\":[1,1]},"
                + "{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}"
                + "]}";
        assertEquals("GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 1 1))",
                GeometryConverter.PARSE.convertToWkt(geoJson));
    }

    // ---- GeoJSON 3D for complex types: exercises createPolygon / createLinearRing
    //      / createMultiPolygon with non-NaN Z values throughout the geometry tree.

    @Test
    void testGeoJsonPolygon3DPreservesZ() {
        String geoJson = "{\"type\":\"Polygon\",\"coordinates\":["
                + "[[0,0,0],[1,0,1],[1,1,2],[0,1,1],[0,0,0]]"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"),
                "3D polygon must produce Z marker, got: " + result);
        // Spot-check a few Z values to confirm none were dropped
        assertTrue(result.contains("0 0 0") && result.contains("1 0 1") && result.contains("1 1 2"),
                "Z values must be preserved in polygon ring, got: " + result);
    }

    @Test
    void testGeoJsonPolygon3DWithHolePreservesZ() {
        // Outer ring + inner hole — both 3D. Exercises the holes loop in createPolygon.
        String geoJson = "{\"type\":\"Polygon\",\"coordinates\":["
                + "[[0,0,0],[10,0,1],[10,10,2],[0,10,1],[0,0,0]],"
                + "[[2,2,5],[8,2,5],[8,8,5],[2,8,5],[2,2,5]]"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"),
                "3D polygon-with-hole must produce Z marker, got: " + result);
        assertTrue(result.contains("10 0 1") && result.contains("2 2 5"),
                "Z values must survive in both outer ring and hole, got: " + result);
    }

    @Test
    void testGeoJsonMultiPoint3DPreservesZ() {
        String geoJson = "{\"type\":\"MultiPoint\",\"coordinates\":[[0,0,1],[1,1,2]]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"));
        assertTrue(result.contains("0 0 1") && result.contains("1 1 2"),
                "Z must survive on every member point, got: " + result);
    }

    @Test
    void testGeoJsonMultiLineString3DPreservesZ() {
        String geoJson = "{\"type\":\"MultiLineString\",\"coordinates\":["
                + "[[0,0,0],[1,1,1]],"
                + "[[2,2,2],[3,3,3]]"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"));
        assertTrue(result.contains("0 0 0") && result.contains("1 1 1")
                        && result.contains("2 2 2") && result.contains("3 3 3"),
                "Z must survive on every line, got: " + result);
    }

    @Test
    void testGeoJsonMultiPolygon3DPreservesZ() {
        String geoJson = "{\"type\":\"MultiPolygon\",\"coordinates\":["
                + "[[[0,0,0],[1,0,1],[1,1,2],[0,0,0]]],"
                + "[[[2,2,3],[3,2,4],[3,3,5],[2,2,3]]]"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"));
        assertTrue(result.contains("0 0 0") && result.contains("3 3 5"),
                "Z must survive in nested multipolygon rings, got: " + result);
    }

    @Test
    void testGeoJsonGeometryCollection3DPreservesZ() {
        String geoJson = "{\"type\":\"GeometryCollection\",\"geometries\":["
                + "{\"type\":\"Point\",\"coordinates\":[1,1,1]},"
                + "{\"type\":\"LineString\",\"coordinates\":[[0,0,0],[2,2,2]]}"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        assertTrue(result.toUpperCase().contains("Z"));
        assertTrue(result.contains("1 1 1") && result.contains("0 0 0") && result.contains("2 2 2"),
                "Z must survive on every collection member, got: " + result);
    }

    @Test
    void testGeoJsonMixedDimensionGeometryCollection() {
        // Edge case: PostGIS may emit a collection mixing 3D and 2D members.
        // VTS must not crash. The 2D member's Z is NaN; JTS WKTWriter(3) handles
        // this by either omitting Z for 2D members or padding with 0 — either is
        // acceptable as long as we don't lose the 3D values.
        String geoJson = "{\"type\":\"GeometryCollection\",\"geometries\":["
                + "{\"type\":\"Point\",\"coordinates\":[1,1,5]},"
                + "{\"type\":\"LineString\",\"coordinates\":[[0,0],[2,2]]}"
                + "]}";
        String result = GeometryConverter.PARSE.convertToWkt(geoJson);
        // The 3D point's Z must be preserved
        assertTrue(result.contains("5"),
                "Z=5 from the 3D Point member must survive, got: " + result);
        // Both members must appear
        assertTrue(result.toUpperCase().contains("POINT")
                        && result.toUpperCase().contains("LINESTRING"),
                "Both collection members must appear, got: " + result);
    }

    @Test
    void testGeoJson4DCoordinatesThrowsForLineString() {
        // 4D rejection works for non-Point types too — testGeoJson4DCoordinatesThrow
        // covers Point; this exercises the createLineString path.
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt(
                        "{\"type\":\"LineString\",\"coordinates\":[[0,0,0,1],[1,1,1,1]]}"));
        // Error must phrase as a VTS limitation, not a data-validation message
        assertTrue(ex.getMessage().contains("VTS cannot convert"),
                "Error must attribute 4D rejection to VTS's capability boundary, got: " + ex.getMessage());
    }

    @Test
    void testGeoJsonNegativeCoordinates() {
        // Real-world geo data has lots of negatives (Western/Southern hemisphere).
        assertEquals("POINT (-74.006 40.7128)",
                GeometryConverter.PARSE.convertToWkt(
                        "{\"type\":\"Point\",\"coordinates\":[-74.006,40.7128]}"));
        String linestring = GeometryConverter.PARSE.convertToWkt(
                "{\"type\":\"LineString\",\"coordinates\":[[-1,-1],[-2,-2]]}");
        assertEquals("LINESTRING (-1 -1, -2 -2)", linestring);
    }

    @Test
    void testGeoJsonWithCrsIgnored() {
        // PostGIS ST_AsGeoJSON may include CRS — should be ignored
        String geoJson = "{\"type\":\"Point\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[1,2]}";
        assertEquals("POINT (1 2)", GeometryConverter.PARSE.convertToWkt(geoJson));
    }

    @Test
    void testGeoJson4DCoordinatesThrow() {
        // GeoJSON spec doesn't define a 4th element, JTS Coordinate can't hold M
        // → VTS can't losslessly convert this
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt(
                        "{\"type\":\"Point\",\"coordinates\":[1,2,3,4]}"));
        // Error must phrase as a VTS limitation, not a data-validation message
        assertTrue(ex.getMessage().contains("VTS cannot convert"),
                "Error must attribute 4D rejection to VTS's capability boundary, got: " + ex.getMessage());
    }

    // ---- WKB hex ----

    @Test
    void testWkbHexPoint() {
        // POINT(1 1)
        String wkbHex = "0101000000000000000000F03F000000000000F03F";
        assertEquals("POINT (1 1)", GeometryConverter.PARSE.convertToWkt(wkbHex));
    }

    @Test
    void testEwkbHexPoint() {
        // POINT(1 1) with SRID=4326
        String ewkbHex = "0101000020E6100000000000000000F03F000000000000F03F";
        assertEquals("POINT (1 1)", GeometryConverter.PARSE.convertToWkt(ewkbHex));
    }

    @Test
    void testWkb3DHexPreservesZ() {
        // POINT Z (1 1 1) — little-endian, type = 0x80000001 (wkbPoint with Z flag)
        String wkb3D = "0101000080000000000000F03F000000000000F03F000000000000F03F";
        String result = GeometryConverter.PARSE.convertToWkt(wkb3D);
        assertTrue(result.contains("1 1 1"),
                "3D WKB must round-trip with Z preserved, got: " + result);
        assertTrue(result.toUpperCase().contains("Z"),
                "Output must mark 3D explicitly, got: " + result);
    }

    @Test
    void testEwkb3DHexPreservesZ() {
        // EWKB with SRID=4326 and Z flag: POINT Z (1 1 1)
        String ewkb3D = "01010000A0E6100000000000000000F03F000000000000F03F000000000000F03F";
        String result = GeometryConverter.PARSE.convertToWkt(ewkb3D);
        assertTrue(result.contains("1 1 1"),
                "3D EWKB must round-trip with Z preserved, got: " + result);
    }

    // ---- WKB hex 3D for non-Point types: this is the *real* code path that
    //      exercises WKBReader → WKTWriter(3). The Point-only coverage above is
    //      not enough — JTS handles complex geometries through different code
    //      paths (rings, nested geometries, etc.) where Z preservation could
    //      regress independently.

    @Test
    void testWkbHex3DLineStringPreservesZ() {
        // Generated via shapely:
        //   LineString([(0, 0, 0), (1, 1, 1), (2, 0, 2)])
        String wkbHex = "010200008003000000"
                + "000000000000000000000000000000000000000000000000"  // (0, 0, 0)
                + "000000000000F03F000000000000F03F000000000000F03F"  // (1, 1, 1)
                + "000000000000004000000000000000000000000000000040"; // (2, 0, 2)
        String result = GeometryConverter.PARSE.convertToWkt(wkbHex);
        assertTrue(result.toUpperCase().contains("Z"),
                "3D WKB linestring must produce Z marker, got: " + result);
        assertTrue(result.contains("0 0 0") && result.contains("1 1 1") && result.contains("2 0 2"),
                "All three Z values must survive, got: " + result);
    }

    @Test
    void testWkbHex3DPolygonPreservesZ() {
        // Generated via shapely:
        //   Polygon([(0, 0, 0), (1, 0, 1), (1, 1, 2), (0, 0, 0)])
        String wkbHex = "010300008001000000"  // Polygon Z, 1 ring
                + "04000000"                    // 4 points
                + "000000000000000000000000000000000000000000000000"  // (0, 0, 0)
                + "000000000000F03F0000000000000000000000000000F03F"  // (1, 0, 1)
                + "000000000000F03F000000000000F03F0000000000000040"  // (1, 1, 2)
                + "000000000000000000000000000000000000000000000000"; // (0, 0, 0)
        String result = GeometryConverter.PARSE.convertToWkt(wkbHex);
        assertTrue(result.toUpperCase().contains("Z"),
                "3D WKB polygon must produce Z marker, got: " + result);
        assertTrue(result.contains("0 0 0") && result.contains("1 0 1") && result.contains("1 1 2"),
                "All Z values from polygon ring must survive, got: " + result);
    }

    @Test
    void testWkbHex3DMultiPointPreservesZ() {
        // Generated via shapely:
        //   MultiPoint([(0, 0, 1), (1, 1, 2)])
        String wkbHex = "010400008002000000"  // MultiPoint Z, 2 points
                + "010100008000000000000000000000000000000000000000000000F03F"  // (0, 0, 1)
                + "0101000080000000000000F03F000000000000F03F0000000000000040"; // (1, 1, 2)
        String result = GeometryConverter.PARSE.convertToWkt(wkbHex);
        assertTrue(result.toUpperCase().contains("Z"),
                "3D WKB multipoint must produce Z marker, got: " + result);
        assertTrue(result.contains("0 0 1") && result.contains("1 1 2"),
                "Both member Z values must survive, got: " + result);
    }

    @Test
    void testWkbHexNegativeCoordinates() {
        // Generated via shapely: Point(-74.006, 40.7128)
        // Sign bit handling through the binary parse path.
        String wkbHex = "0101000000AAF1D24D628052C05E4BC8073D5B4440";
        String result = GeometryConverter.PARSE.convertToWkt(wkbHex);
        assertTrue(result.contains("-74.006"),
                "Negative longitude must survive WKB→WKT, got: " + result);
        assertTrue(result.contains("40.7128"),
                "Latitude must survive WKB→WKT, got: " + result);
    }

    @Test
    void testWkbHex3DNegativeCoordinatesPreservesSignAndZ() {
        // Generated via shapely: Point(-74.006, 40.7128, -10.5)
        // Negative Z + negative X — the most likely place for sign bugs.
        String wkbHex = "0101000080AAF1D24D628052C05E4BC8073D5B444000000000000025C0";
        String result = GeometryConverter.PARSE.convertToWkt(wkbHex);
        assertTrue(result.toUpperCase().contains("Z"));
        assertTrue(result.contains("-74.006") && result.contains("40.7128") && result.contains("-10.5"),
                "All three negative/positive components must survive, got: " + result);
    }

    @Test
    void testWkbBytesUnparseableTypeErrorMessageHasHint() {
        // ISO WKB with type = 9 (CompoundCurve, not supported by JTS).
        // We can't easily construct a valid CompoundCurve WKB, but we don't
        // need to: any unparseable WKB exercises the same catch branch and
        // we just verify the hint text is present.
        // Header: 01 (LE) 09000000 (type 9 = CompoundCurve)
        byte[] wkb = new byte[]{
                0x01, 0x09, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,  // numGeoms = 0 (intentionally truncated)
        };
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PASSTHROUGH.convertWkbBytes(wkb));
        // Hint must mention extended types and the recommended workaround
        assertTrue(ex.getMessage().contains("ST_AsText"),
                "Error must hint at ST_AsText workaround, got: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("postgis")
                        || ex.getMessage().contains("CompoundCurve")
                        || ex.getMessage().contains("CircularString"),
                "Error must mention extended geometry types, got: " + ex.getMessage());
    }

    @Test
    void testWkbHexLineString() {
        String wkbHex = "010200000003000000"
                + "00000000000000000000000000000000"
                + "000000000000F03F000000000000F03F"
                + "00000000000000400000000000000000";
        assertEquals("LINESTRING (0 0, 1 1, 2 0)", GeometryConverter.PARSE.convertToWkt(wkbHex));
    }

    @Test
    void testWkbHexWithMCoordinateThrowsAsVtsLimitation() {
        // ISO WKB with M flag: type = 2001 (POINT M) = 0xD1 0x07 = 7D1
        // little-endian: 01 D1070000 + 3 doubles
        String wkbWithM = "01D1070000"
                + "000000000000F03F"  // x = 1
                + "0000000000000040"  // y = 2
                + "0000000000000840"; // m = 3
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt(wkbWithM));
        // Error must attribute the limitation to VTS, not to Milvus
        assertTrue(ex.getMessage().contains("VTS"),
                "Error must attribute the limitation to VTS, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("M"),
                "Error must mention M coordinate, got: " + ex.getMessage());
    }

    @Test
    void testEwkbHexWithMFlagThrowsAsVtsLimitation() {
        // EWKB with M flag bit 0x40000000: type = 0x40000001
        String ewkbWithM = "0101000040"
                + "000000000000F03F"
                + "0000000000000040"
                + "0000000000000840";
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt(ewkbWithM));
        assertTrue(ex.getMessage().contains("VTS"));
        assertTrue(ex.getMessage().contains("M"));
    }

    // ---- ES geo_point variants ----

    @Test
    void testEsGeoPointObjectFormat() {
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.PARSE.convertToWkt("{\"lat\":31.2304,\"lon\":121.4737}"));
        assertEquals("POINT (0 0)",
                GeometryConverter.PARSE.convertToWkt("{\"lat\":0,\"lon\":0}"));
    }

    @Test
    void testEsGeoPointStringDefaultLatLon() {
        // Default order is LAT_LON (ES convention)
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.PARSE.convertToWkt("31.2304,121.4737"));
        assertEquals("POINT (0 0)", GeometryConverter.PARSE.convertToWkt("0,0"));
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.PARSE.convertToWkt("31.2304, 121.4737"));
    }

    @Test
    void testCoordinateStringLonLatOrder() {
        // Opt-in lon,lat order — same input bytes are interpreted differently.
        // Bare "x,y" parsing only happens in PARSE mode.
        GeometryConverter lonLat = new GeometryConverter(
                GeometryConverter.ConvertMode.PARSE,
                GeometryConverter.CoordinateOrder.LON_LAT);
        assertEquals("POINT (121.4737 31.2304)",
                lonLat.convertToWkt("121.4737,31.2304"));
    }

    @Test
    void testCoordinateStringSwapBetweenOrders() {
        // The same string yields swapped points depending on order — proves the
        // option actually controls semantic interpretation
        GeometryConverter latLon = new GeometryConverter(
                GeometryConverter.ConvertMode.PARSE,
                GeometryConverter.CoordinateOrder.LAT_LON);
        GeometryConverter lonLat = new GeometryConverter(
                GeometryConverter.ConvertMode.PARSE,
                GeometryConverter.CoordinateOrder.LON_LAT);
        String input = "10,20";
        assertEquals("POINT (20 10)", latLon.convertToWkt(input));
        assertEquals("POINT (10 20)", lonLat.convertToWkt(input));
    }

    @Test
    void testEsGeoPointStringScientificNotation() {
        assertEquals("POINT (1 0.00001)", GeometryConverter.PARSE.convertToWkt("1E-5,1"));
        assertEquals("POINT (121.47 31.23)", GeometryConverter.PARSE.convertToWkt("+31.23,+121.47"));
    }

    @Test
    void testEsGeoPointArrayFormat() {
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.PARSE.convertToWkt("[121.4737,31.2304]"));
        assertEquals("POINT (0 0)", GeometryConverter.PARSE.convertToWkt("[0,0]"));
    }

    @Test
    void testEsGeoPointArray3DPreservesZ() {
        // [lon, lat, alt] — Z survives
        String result = GeometryConverter.PARSE.convertToWkt("[121.47,31.23,10.5]");
        assertTrue(result.contains("10.5"),
                "Z value must be preserved from 3-element array, got: " + result);
    }

    @Test
    void testCoordinateString3DPreservesZ() {
        // "lat,lon,alt" — Z survives
        String result = GeometryConverter.PARSE.convertToWkt("31.23,121.47,10.5");
        assertTrue(result.contains("10.5"), "got: " + result);
    }

    @Test
    void testNullAndEmpty() {
        // null/empty short-circuit happens before mode dispatch — verify both modes
        assertNull(GeometryConverter.PASSTHROUGH.convertToWkt(null));
        assertEquals("", GeometryConverter.PASSTHROUGH.convertToWkt(""));
        assertNull(GeometryConverter.PARSE.convertToWkt(null));
        assertEquals("", GeometryConverter.PARSE.convertToWkt(""));
    }

    // ---- WKB bytes entry point (used by ByteBuffer sources) ----

    @Test
    void testConvertWkbBytes2D() {
        // POINT(1 1)
        byte[] wkb = hexToBytes("0101000000000000000000F03F000000000000F03F");
        assertEquals("POINT (1 1)", GeometryConverter.PASSTHROUGH.convertWkbBytes(wkb));
    }

    @Test
    void testConvertWkbBytes3DPreservesZ() {
        byte[] wkb = hexToBytes("0101000080000000000000F03F000000000000F03F000000000000F03F");
        String result = GeometryConverter.PASSTHROUGH.convertWkbBytes(wkb);
        assertTrue(result.contains("1 1 1"), "Z must survive: " + result);
    }

    @Test
    void testConvertWkbBytesWithMThrows() {
        byte[] wkb = hexToBytes("01D1070000000000000000F03F00000000000000400000000000000840");
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PASSTHROUGH.convertWkbBytes(wkb));
        assertTrue(ex.getMessage().contains("VTS"));
    }

    @Test
    void testConvertWkbBytesNullOrEmpty() {
        assertNull(GeometryConverter.PASSTHROUGH.convertWkbBytes(null));
        assertNull(GeometryConverter.PASSTHROUGH.convertWkbBytes(new byte[0]));
    }

    // ---- convert(Object) entry point: dispatches on value type in PARSE mode ----

    @Test
    void testConvertObject_Parse_StringDispatchesToConvertToWkt() {
        // Exercises convert(Object) String branch
        Object result = GeometryConverter.PARSE.convert("SRID=4326;POINT(1 2)");
        assertEquals("POINT(1 2)", result, "SRID should be stripped in PARSE mode");
    }

    @Test
    void testConvertObject_Parse_ByteBufferDispatchesToConvertWkbBytes() {
        // Exercises convert(Object) ByteBuffer branch — the raison d'être of
        // keeping ByteBuffer in the dispatch: any future source that emits
        // raw WKB bytes gets lossless conversion for free.
        byte[] wkb = hexToBytes("0101000000000000000000F03F000000000000F03F");
        Object result = GeometryConverter.PARSE.convert(java.nio.ByteBuffer.wrap(wkb));
        assertEquals("POINT (1 1)", result);
    }

    @Test
    void testConvertObject_Parse_ByteBuffer3DPreservesZ() {
        byte[] wkb = hexToBytes("0101000080000000000000F03F000000000000F03F000000000000F03F");
        Object result = GeometryConverter.PARSE.convert(java.nio.ByteBuffer.wrap(wkb));
        assertNotNull(result);
        assertTrue(result.toString().contains("1 1 1"),
                "3D WKB bytes via convert(Object) must preserve Z, got: " + result);
    }

    @Test
    void testConvertObject_Parse_ByteBufferDoesNotMutateCaller() {
        // duplicate() contract: caller's position and limit survive the call
        byte[] wkb = hexToBytes("0101000000000000000000F03F000000000000F03F");
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(wkb);
        int originalPos = buf.position();
        int originalLimit = buf.limit();
        GeometryConverter.PARSE.convert(buf);
        assertEquals(originalPos, buf.position(),
                "Input ByteBuffer position must not be advanced by convert(Object)");
        assertEquals(originalLimit, buf.limit(),
                "Input ByteBuffer limit must not change");
    }

    @Test
    void testConvertObject_Parse_ByteBufferSlicePreservesCorrectBytes() {
        // Real-world JDBC slice scenario: backing array has garbage prefix,
        // position points past the garbage, limit truncates the real payload.
        byte[] wkb = hexToBytes("0101000000000000000000F03F000000000000F03F");
        byte[] backing = new byte[wkb.length + 8];
        for (int i = 0; i < backing.length; i++) backing[i] = (byte) 0xFF;
        System.arraycopy(wkb, 0, backing, 4, wkb.length);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(backing);
        buf.position(4);
        buf.limit(4 + wkb.length);
        Object result = GeometryConverter.PARSE.convert(buf);
        assertEquals("POINT (1 1)", result,
                "convert(Object) must read from position..limit, not index 0");
    }

    @Test
    void testConvertObject_Passthrough_ReturnsByteBufferUnchanged() {
        // PASSTHROUGH does not type-dispatch — it returns whatever.
        byte[] wkb = hexToBytes("0101000000000000000000F03F000000000000F03F");
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(wkb);
        Object result = GeometryConverter.PASSTHROUGH.convert(buf);
        assertSame(buf, result,
                "PASSTHROUGH must return the exact same reference, no conversion");
    }

    @Test
    void testConvertObject_Passthrough_ReturnsAnyValueAsIs() {
        assertEquals("anything at all",
                GeometryConverter.PASSTHROUGH.convert("anything at all"));
        assertNull(GeometryConverter.PASSTHROUGH.convert(null));
        Object weird = new Object();
        assertSame(weird, GeometryConverter.PASSTHROUGH.convert(weird));
    }

    @Test
    void testConvertObject_Parse_NullStaysNull() {
        assertNull(GeometryConverter.PARSE.convert(null));
    }

    @Test
    void testConvertObject_Parse_UnsupportedTypeThrows() {
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convert(42));
        assertTrue(ex.getMessage().contains("parse mode"));
        assertTrue(ex.getMessage().contains("Integer"));
    }

    // ---- Unsupported / unrecognized formats ----

    @Test
    void testUnrecognizedJsonThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt("{\"name\":\"foo\"}"));
    }

    @Test
    void testGeohashThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt("drm3btev3e86"));
    }

    @Test
    void testRandomStringThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt("not a geometry"));
    }

    @Test
    void testNumericHexStringNotMistakenForWkb() {
        // Hex string not starting with 00/01 should not be treated as WKB
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.PARSE.convertToWkt("ABCDEF123456789012"));
    }

    // ---- Helpers ----

    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            out[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return out;
    }
}
