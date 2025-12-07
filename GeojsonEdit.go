package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// RemoveLinePolygonBoundaryOverlapFromGeoJSON 使用geojson feature处理线与面的边界重叠
// lineFeature: 输入的线geojson要素
// polygonFeature: 输入的面geojson要素
// 返回：处理后最长线段的geojson要素
func RemoveLinePolygonBoundaryOverlapFromGeoJSON(lineFeature, polygonFeature *geojson.Feature, tolerance float64) (*geojson.Feature, error) {
	if lineFeature == nil || polygonFeature == nil {
		return nil, fmt.Errorf("输入的geojson要素为空")
	}

	if lineFeature.Geometry == nil || polygonFeature.Geometry == nil {
		return nil, fmt.Errorf("geojson要素的几何体为空")
	}

	// 将geojson几何体转换为GDAL几何体
	lineGeom, err := orbGeometryToOGRGeometry(lineFeature.Geometry)
	if err != nil {
		return nil, fmt.Errorf("转换线geojson几何体失败: %v", err)
	}
	if lineGeom == nil {
		return nil, fmt.Errorf("线geojson几何体转换结果为空")
	}
	defer C.OGR_G_DestroyGeometry(lineGeom)

	polygonGeom, err := orbGeometryToOGRGeometry(polygonFeature.Geometry)
	if err != nil {
		return nil, fmt.Errorf("转换面geojson几何体失败: %v", err)
	}
	if polygonGeom == nil {
		return nil, fmt.Errorf("面geojson几何体转换结果为空")
	}
	defer C.OGR_G_DestroyGeometry(polygonGeom)

	// 处理重叠部分
	resultGeom := RemoveLinePolygonBoundaryOverlapGeometryAndReturnLongest(lineGeom, polygonGeom, tolerance)
	if resultGeom == nil {
		return nil, fmt.Errorf("处理结果为空")
	}
	defer C.OGR_G_DestroyGeometry(resultGeom)

	// 将GDAL几何体转换回geojson几何体
	resultOrbGeom, err := ogrGeometryToOrbGeometry(resultGeom)
	if err != nil {
		return nil, fmt.Errorf("转换结果几何体失败: %v", err)
	}

	// 创建结果feature，保留原始属性和ID
	resultFeature := &geojson.Feature{
		ID:         lineFeature.ID,
		Type:       "Feature",
		Geometry:   resultOrbGeom,
		Properties: lineFeature.Properties,
	}

	// 如果有长度属性，添加处理后的长度
	if resultFeature.Properties == nil {
		resultFeature.Properties = make(geojson.Properties)
	}

	length := float64(C.OGR_G_Length(resultGeom))
	resultFeature.Properties["length"] = length
	resultFeature.Properties["processed_at"] = "RemoveLinePolygonBoundaryOverlap"

	return resultFeature, nil
}

// orbGeometryToOGRGeometry 将orb.Geometry转换为GDAL OGRGeometry
func orbGeometryToOGRGeometry(geometry orb.Geometry) (C.OGRGeometryH, error) {
	if geometry == nil {
		return nil, fmt.Errorf("geometry为空")
	}

	switch geom := geometry.(type) {
	case orb.Point:
		return createOGRPoint(geom), nil

	case orb.LineString:
		return createOGRLineString(geom), nil

	case orb.Ring:
		return createOGRLinearRing(geom), nil

	case orb.Polygon:
		return createOGRPolygon(geom), nil

	case orb.MultiPoint:
		return createOGRMultiPoint(geom), nil

	case orb.MultiLineString:
		return createOGRMultiLineString(geom), nil

	case orb.MultiPolygon:
		return createOGRMultiPolygon(geom), nil

	case orb.Collection:
		return createOGRGeometryCollection(geom), nil

	default:
		return nil, fmt.Errorf("不支持的几何类型: %T", geometry)
	}
}

// ogrGeometryToOrbGeometry 将GDAL OGRGeometry转换为orb.Geometry
func ogrGeometryToOrbGeometry(geometry C.OGRGeometryH) (orb.Geometry, error) {
	if geometry == nil {
		return nil, fmt.Errorf("OGRGeometry为空")
	}

	geomType := C.OGR_G_GetGeometryType(geometry)

	switch geomType {
	case C.wkbPoint:
		return ogrPointToOrbPoint(geometry), nil

	case C.wkbLineString, C.wkbLineString25D:
		return ogrLineStringToOrbLineString(geometry), nil

	case C.wkbPolygon, C.wkbPolygon25D:
		return ogrPolygonToOrbPolygon(geometry), nil

	case C.wkbMultiPoint:
		return ogrMultiPointToOrbMultiPoint(geometry), nil

	case C.wkbMultiLineString, C.wkbMultiLineString25D:
		return ogrMultiLineStringToOrbMultiLineString(geometry), nil

	case C.wkbMultiPolygon, C.wkbMultiPolygon25D:
		return ogrMultiPolygonToOrbMultiPolygon(geometry), nil

	case C.wkbGeometryCollection:
		return ogrGeometryCollectionToOrbCollection(geometry), nil

	default:
		return nil, fmt.Errorf("不支持的OGR几何类型: %v", geomType)
	}
}

// ============================================================================
// 辅助函数：orb -> OGR
// ============================================================================

func createOGRPoint(p orb.Point) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbPoint)
	if geom == nil {
		return nil
	}
	C.OGR_G_SetPoint_2D(geom, 0, C.double(p.X()), C.double(p.Y()))
	return geom
}

func createOGRLineString(line orb.LineString) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbLineString)
	if geom == nil {
		return nil
	}

	for i, p := range line {
		C.OGR_G_SetPoint_2D(geom, C.int(i), C.double(p.X()), C.double(p.Y()))
	}

	return geom
}

func createOGRLinearRing(ring orb.Ring) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbLinearRing)
	if geom == nil {
		return nil
	}

	for i, p := range ring {
		C.OGR_G_SetPoint_2D(geom, C.int(i), C.double(p.X()), C.double(p.Y()))
	}

	return geom
}

func createOGRPolygon(poly orb.Polygon) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbPolygon)
	if geom == nil {
		return nil
	}

	for _, ring := range poly {
		ringGeom := createOGRLinearRing(ring)
		if ringGeom != nil {
			C.OGR_G_AddGeometryDirectly(geom, ringGeom)
		}
	}

	return geom
}

func createOGRMultiPoint(mp orb.MultiPoint) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbMultiPoint)
	if geom == nil {
		return nil
	}

	for _, p := range mp {
		pointGeom := createOGRPoint(p)
		if pointGeom != nil {
			C.OGR_G_AddGeometryDirectly(geom, pointGeom)
		}
	}

	return geom
}

func createOGRMultiLineString(mls orb.MultiLineString) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbMultiLineString)
	if geom == nil {
		return nil
	}

	for _, line := range mls {
		lineGeom := createOGRLineString(line)
		if lineGeom != nil {
			C.OGR_G_AddGeometryDirectly(geom, lineGeom)
		}
	}

	return geom
}

func createOGRMultiPolygon(mp orb.MultiPolygon) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
	if geom == nil {
		return nil
	}

	for _, poly := range mp {
		polyGeom := createOGRPolygon(poly)
		if polyGeom != nil {
			C.OGR_G_AddGeometryDirectly(geom, polyGeom)
		}
	}

	return geom
}

func createOGRGeometryCollection(collection orb.Collection) C.OGRGeometryH {
	geom := C.OGR_G_CreateGeometry(C.wkbGeometryCollection)
	if geom == nil {
		return nil
	}

	for _, g := range collection {
		subGeom, err := orbGeometryToOGRGeometry(g)
		if err == nil && subGeom != nil {
			C.OGR_G_AddGeometryDirectly(geom, subGeom)
		}
	}

	return geom
}

// ============================================================================
// 辅助函数：OGR -> orb
// ============================================================================

func ogrPointToOrbPoint(geometry C.OGRGeometryH) orb.Point {
	x := float64(C.OGR_G_GetX(geometry, 0))
	y := float64(C.OGR_G_GetY(geometry, 0))
	return orb.Point{x, y}
}

func ogrLineStringToOrbLineString(geometry C.OGRGeometryH) orb.LineString {
	pointCount := int(C.OGR_G_GetPointCount(geometry))
	line := make(orb.LineString, pointCount)

	for i := 0; i < pointCount; i++ {
		x := float64(C.OGR_G_GetX(geometry, C.int(i)))
		y := float64(C.OGR_G_GetY(geometry, C.int(i)))
		line[i] = orb.Point{x, y}
	}

	return line
}

func ogrLinearRingToOrbRing(geometry C.OGRGeometryH) orb.Ring {
	pointCount := int(C.OGR_G_GetPointCount(geometry))
	ring := make(orb.Ring, pointCount)

	for i := 0; i < pointCount; i++ {
		x := float64(C.OGR_G_GetX(geometry, C.int(i)))
		y := float64(C.OGR_G_GetY(geometry, C.int(i)))
		ring[i] = orb.Point{x, y}
	}

	return ring
}

func ogrPolygonToOrbPolygon(geometry C.OGRGeometryH) orb.Polygon {
	ringCount := int(C.OGR_G_GetGeometryCount(geometry))
	poly := make(orb.Polygon, ringCount)

	for i := 0; i < ringCount; i++ {
		ringGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
		if ringGeom != nil {
			poly[i] = ogrLinearRingToOrbRing(ringGeom)
		}
	}

	return poly
}

func ogrMultiPointToOrbMultiPoint(geometry C.OGRGeometryH) orb.MultiPoint {
	pointCount := int(C.OGR_G_GetGeometryCount(geometry))
	mp := make(orb.MultiPoint, pointCount)

	for i := 0; i < pointCount; i++ {
		pointGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
		if pointGeom != nil {
			mp[i] = ogrPointToOrbPoint(pointGeom)
		}
	}

	return mp
}

func ogrMultiLineStringToOrbMultiLineString(geometry C.OGRGeometryH) orb.MultiLineString {
	lineCount := int(C.OGR_G_GetGeometryCount(geometry))
	mls := make(orb.MultiLineString, lineCount)

	for i := 0; i < lineCount; i++ {
		lineGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
		if lineGeom != nil {
			mls[i] = ogrLineStringToOrbLineString(lineGeom)
		}
	}

	return mls
}

func ogrMultiPolygonToOrbMultiPolygon(geometry C.OGRGeometryH) orb.MultiPolygon {
	polyCount := int(C.OGR_G_GetGeometryCount(geometry))
	mp := make(orb.MultiPolygon, polyCount)

	for i := 0; i < polyCount; i++ {
		polyGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
		if polyGeom != nil {
			mp[i] = ogrPolygonToOrbPolygon(polyGeom)
		}
	}

	return mp
}

func ogrGeometryCollectionToOrbCollection(geometry C.OGRGeometryH) orb.Collection {
	geomCount := int(C.OGR_G_GetGeometryCount(geometry))
	collection := make(orb.Collection, 0, geomCount)

	for i := 0; i < geomCount; i++ {
		subGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
		if subGeom != nil {
			if orbGeom, err := ogrGeometryToOrbGeometry(subGeom); err == nil {
				collection = append(collection, orbGeom)
			}
		}
	}

	return collection
}
