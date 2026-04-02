package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MilvusSinkConverterGeometryTest {

    // ---- Supported formats: correct conversion ----

    @Test
    void testWktPassThrough() {
        assertEquals("POINT(1 2)", GeometryConverter.convertToWkt("POINT(1 2)"));
        assertEquals("LINESTRING(0 0, 1 1)", GeometryConverter.convertToWkt("LINESTRING(0 0, 1 1)"));
        assertEquals("POLYGON((0 0, 1 0, 1 1, 0 0))", GeometryConverter.convertToWkt("POLYGON((0 0, 1 0, 1 1, 0 0))"));
        assertEquals("MULTIPOINT((0 0), (1 1))", GeometryConverter.convertToWkt("MULTIPOINT((0 0), (1 1))"));
        assertEquals("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))",
                GeometryConverter.convertToWkt("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))"));
    }

    @Test
    void testWktEmptyGeometries() {
        // EMPTY geometries are valid WKT — pass through, let Milvus decide
        assertEquals("POINT EMPTY", GeometryConverter.convertToWkt("POINT EMPTY"));
        assertEquals("LINESTRING EMPTY", GeometryConverter.convertToWkt("LINESTRING EMPTY"));
        assertEquals("GEOMETRYCOLLECTION EMPTY", GeometryConverter.convertToWkt("GEOMETRYCOLLECTION EMPTY"));
    }

    @Test
    void testEwktStripsSrid() {
        assertEquals("POINT(121.4737 31.2304)",
                GeometryConverter.convertToWkt("SRID=4326;POINT(121.4737 31.2304)"));
        assertEquals("POLYGON((0 0, 1 0, 1 1, 0 0))",
                GeometryConverter.convertToWkt("SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 0))"));
    }

    @Test
    void testGeoJsonPoint() {
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.convertToWkt("{\"type\":\"Point\",\"coordinates\":[121.4737,31.2304]}"));
    }

    @Test
    void testGeoJsonLineString() {
        assertEquals("LINESTRING (0 0, 1 1, 2 0)",
                GeometryConverter.convertToWkt("{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1],[2,0]]}"));
    }

    @Test
    void testGeoJsonPolygon() {
        assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
                GeometryConverter.convertToWkt("{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"));
    }

    @Test
    void testGeoJsonMultiPoint() {
        assertEquals("MULTIPOINT ((0 0), (1 1))",
                GeometryConverter.convertToWkt("{\"type\":\"MultiPoint\",\"coordinates\":[[0,0],[1,1]]}"));
    }

    @Test
    void testGeoJsonGeometryCollection() {
        String geoJson = "{\"type\":\"GeometryCollection\",\"geometries\":["
                + "{\"type\":\"Point\",\"coordinates\":[1,1]},"
                + "{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}"
                + "]}";
        assertEquals("GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 1 1))",
                GeometryConverter.convertToWkt(geoJson));
    }

    @Test
    void testGeoJsonWithCrsIgnored() {
        // PostGIS ST_AsGeoJSON may include CRS — should be ignored
        String geoJson = "{\"type\":\"Point\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[1,2]}";
        assertEquals("POINT (1 2)", GeometryConverter.convertToWkt(geoJson));
    }

    @Test
    void testWkbHexPoint() {
        String wkbHex = "0101000000000000000000F03F000000000000F03F";
        assertEquals("POINT (1 1)", GeometryConverter.convertToWkt(wkbHex));
    }

    @Test
    void testEwkbHexPoint() {
        String ewkbHex = "0101000020E6100000000000000000F03F000000000000F03F";
        assertEquals("POINT (1 1)", GeometryConverter.convertToWkt(ewkbHex));
    }

    @Test
    void testWkbHexLineString() {
        String wkbHex = "010200000003000000"
                + "00000000000000000000000000000000"
                + "000000000000F03F000000000000F03F"
                + "00000000000000400000000000000000";
        assertEquals("LINESTRING (0 0, 1 1, 2 0)", GeometryConverter.convertToWkt(wkbHex));
    }

    @Test
    void testEsGeoPointObjectFormat() {
        assertEquals("POINT (121.4737 31.2304)",
                GeometryConverter.convertToWkt("{\"lat\":31.2304,\"lon\":121.4737}"));
        assertEquals("POINT (0 0)",
                GeometryConverter.convertToWkt("{\"lat\":0,\"lon\":0}"));
    }

    @Test
    void testEsGeoPointStringFormat() {
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.convertToWkt("31.2304,121.4737"));
        assertEquals("POINT (0 0)", GeometryConverter.convertToWkt("0,0"));
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.convertToWkt("31.2304, 121.4737"));
    }

    @Test
    void testEsGeoPointStringScientificNotation() {
        assertEquals("POINT (1 0.00001)", GeometryConverter.convertToWkt("1E-5,1"));
        assertEquals("POINT (121.47 31.23)", GeometryConverter.convertToWkt("+31.23,+121.47"));
    }

    @Test
    void testEsGeoPointArrayFormat() {
        assertEquals("POINT (121.4737 31.2304)", GeometryConverter.convertToWkt("[121.4737,31.2304]"));
        assertEquals("POINT (0 0)", GeometryConverter.convertToWkt("[0,0]"));
    }

    @Test
    void testNullAndEmpty() {
        assertEquals(null, GeometryConverter.convertToWkt(null));
        assertEquals("", GeometryConverter.convertToWkt(""));
    }

    // ---- 3D/M/ZM: must throw in ALL format paths ----

    @Test
    void testWkt3DZThrows() {
        // PostGIS: POINT Z (1 2 3)
        assertThrowsWith3D("POINT Z (1 2 3)");
        assertThrowsWith3D("LINESTRING Z (0 0 0, 1 1 1)");
        assertThrowsWith3D("POLYGON Z ((0 0 0, 1 0 0, 1 1 0, 0 0 0))");
        assertThrowsWith3D("MULTIPOINT Z ((0 0 0), (1 1 1))");
    }

    @Test
    void testWkt3DMThrows() {
        // PostGIS: POINT M (1 2 3)
        assertThrowsWith3D("POINT M (1 2 3)");
        assertThrowsWith3D("LINESTRING M (0 0 0, 1 1 1)");
    }

    @Test
    void testWkt3DZMThrows() {
        // PostGIS: POINT ZM (1 2 3 4)
        assertThrowsWith3D("POINT ZM (1 2 3 4)");
        assertThrowsWith3D("POLYGON ZM ((0 0 0 0, 1 0 0 0, 1 1 0 0, 0 0 0 0))");
    }

    @Test
    void testWkt3DNoSpaceThrows() {
        // Older PostGIS format without space: POINTZ(1 2 3)
        assertThrowsWith3D("POINTZ(1 2 3)");
        assertThrowsWith3D("POINTM(1 2 3)");
        assertThrowsWith3D("POINTZM(1 2 3 4)");
        assertThrowsWith3D("LINESTRINGZ(0 0 0, 1 1 1)");
    }

    @Test
    void testEwkt3DThrows() {
        // EWKT with 3D
        assertThrowsWith3D("SRID=4326;POINT Z (1 2 3)");
        assertThrowsWith3D("SRID=4326;POINTZ(1 2 3)");
        assertThrowsWith3D("SRID=4326;POLYGON ZM ((0 0 0 0, 1 0 0 0, 1 1 0 0, 0 0 0 0))");
    }

    @Test
    void testGeoJson3DThrows() {
        assertThrowsWith3D("{\"type\":\"Point\",\"coordinates\":[121.47,31.23,10.5]}");
        assertThrowsWith3D("{\"type\":\"LineString\",\"coordinates\":[[0,0,0],[1,1,1]]}");
    }

    @Test
    void testEsGeoPointArray3DThrows() {
        assertThrowsWith3D("[121.47,31.23,10.5]");
    }

    @Test
    void testEsGeoPointString3DThrows() {
        assertThrowsWith3D("31.23,121.47,10.5");
    }

    // ---- Unsupported formats: must throw, never silently pass ----

    @Test
    void testUnrecognizedJsonThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("{\"name\":\"foo\"}"));
    }

    @Test
    void testEsEnvelopeGeoJsonThrows() {
        // ES-specific envelope type
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("{\"type\":\"envelope\",\"coordinates\":[[100,1],[101,0]]}"));
    }

    @Test
    void testEsCircleGeoJsonThrows() {
        // ES-specific circle type
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("{\"type\":\"circle\",\"radius\":\"40m\",\"coordinates\":[30,10]}"));
    }

    @Test
    void testGeoJsonFeatureThrows() {
        // GeoJSON Feature is not a geometry
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,2]}}"));
    }

    @Test
    void testEsBboxWktThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("BBOX (100, 102, 2, 0)"));
    }

    @Test
    void testEsCircleWktThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("CIRCLE (30 10 40)"));
    }

    @Test
    void testPostgisCurveTypesThrow() {
        // PostGIS curve geometry types — not supported
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("CIRCULARSTRING(0 0, 1 1, 2 0)"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 2 0), (2 0, 3 0))"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("CURVEPOLYGON(CIRCULARSTRING(0 0, 1 0, 1 1, 0 1, 0 0))"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("MULTICURVE(CIRCULARSTRING(0 0, 1 1, 2 0))"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(0 0, 1 0, 1 1, 0 1, 0 0)))"));
    }

    @Test
    void testPostgisSurfaceTypesThrow() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("TRIANGLE((0 0, 1 0, 0 1, 0 0))"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("TIN(((0 0 0, 0 1 0, 1 0 0, 0 0 0)))"));
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("POLYHEDRALSURFACE(((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)))"));
    }

    @Test
    void testGeohashThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("drm3btev3e86"));
    }

    @Test
    void testRandomStringThrows() {
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("not a geometry"));
    }

    @Test
    void testNumericHexStringNotMistakenForWkb() {
        // Hex string not starting with 00/01 should not be treated as WKB
        assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt("ABCDEF123456789012"));
    }

    // ---- Helper ----

    private void assertThrowsWith3D(String input) {
        MilvusConnectorException ex = assertThrows(MilvusConnectorException.class,
                () -> GeometryConverter.convertToWkt(input),
                "Expected 3D rejection for: " + input);
        assertTrue(ex.getMessage().contains("3D") || ex.getMessage().contains("3d"),
                "Error message should mention 3D for: " + input + ", got: " + ex.getMessage());
    }
}
