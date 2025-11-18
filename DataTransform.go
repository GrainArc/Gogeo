package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"
)

type GeoJsonLayers struct {
	Layer     *geojson.FeatureCollection
	LayerName string
	GeoType   string
}

func GDBToGeoJSON(gdbPath string) ([]GeoJsonLayers, error) {
	// 初始化GDAL
	InitializeGDAL()

	var layers []GeoJsonLayers

	// 打开GDB文件
	cGdbPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGdbPath))

	hDataSource := C.OGROpen(cGdbPath, 0, nil)
	if hDataSource == nil {
		return layers, fmt.Errorf("无法打开GDB文件: %s", gdbPath)
	}
	defer C.OGRReleaseDataSource(hDataSource)

	// 创建目标坐标系 EPSG:4490
	hTargetSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hTargetSRS)

	if C.OSRImportFromEPSG(hTargetSRS, 4490) != C.OGRERR_NONE {
		return layers, fmt.Errorf("无法创建EPSG:4490坐标系")
	}

	// 获取图层数量
	layerCount := C.OGR_DS_GetLayerCount(hDataSource)

	// 遍历所有图层
	for i := 0; i < int(layerCount); i++ {
		hLayer := C.OGR_DS_GetLayer(hDataSource, C.int(i))
		if hLayer == nil {
			continue
		}

		// 获取图层名称
		layerName := C.OGR_L_GetName(hLayer)
		layerNameStr := C.GoString(layerName)
		fmt.Printf("处理图层: %s\n", layerNameStr)

		// 获取图层几何类型
		hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
		geomType := C.OGR_FD_GetGeomType(hLayerDefn)
		geoTypeStr := getGeometryTypeName(geomType)

		// 转换图层为GeoJSON（包含坐标系转换）
		featureCollection, err := layerToGeoJSONWithTransform(hLayer, hTargetSRS)
		if err != nil {
			log.Printf("转换图层 %s 失败: %v", layerNameStr, err)
			continue
		}

		layers = append(layers, GeoJsonLayers{
			LayerName: layerNameStr,
			Layer:     featureCollection,
			GeoType:   geoTypeStr,
		})
	}

	return layers, nil
}

// layerToGeoJSONWithTransform 将单个图层转换为GeoJSON FeatureCollection（包含坐标系转换）
func layerToGeoJSONWithTransform(hLayer C.OGRLayerH, hTargetSRS C.OGRSpatialReferenceH) (*geojson.FeatureCollection, error) {
	// 重置读取位置
	C.OGR_L_ResetReading(hLayer)

	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return nil, fmt.Errorf("无法获取图层定义")
	}

	// 获取源坐标系
	hSourceSRS := C.OGR_L_GetSpatialRef(hLayer)
	var hTransform C.OGRCoordinateTransformationH

	// 如果源坐标系存在且与目标坐标系不同，创建坐标转换
	if hSourceSRS != nil {
		// 检查是否需要转换
		if C.OSRIsSame(hSourceSRS, hTargetSRS) == 0 {
			hTransform = C.OCTNewCoordinateTransformation(hSourceSRS, hTargetSRS)
			if hTransform != nil {
				defer C.OCTDestroyCoordinateTransformation(hTransform)
				fmt.Printf("创建坐标转换: 从源坐标系到EPSG:4490\n")
			} else {
				log.Printf("警告: 无法创建坐标转换，将使用原始坐标\n")
			}
		} else {
			fmt.Printf("图层已经是EPSG:4490坐标系，无需转换\n")
		}
	} else {
		log.Printf("警告: 图层没有坐标系信息，假设为EPSG:4490\n")
	}

	// 创建FeatureCollection
	fc := geojson.NewFeatureCollection()

	// 遍历所有要素
	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		// 转换要素为GeoJSON Feature（包含坐标转换）
		feature, err := featureToGeoJSONWithTransform(hFeature, hLayerDefn, hTransform)
		if err != nil {
			log.Printf("转换要素失败: %v", err)
			C.OGR_F_Destroy(hFeature)
			continue
		}

		fc.Append(feature)
		C.OGR_F_Destroy(hFeature)
	}

	return fc, nil
}

// featureToGeoJSONWithTransform 将单个要素转换为GeoJSON Feature（包含坐标转换）
func featureToGeoJSONWithTransform(hFeature C.OGRFeatureH, hLayerDefn C.OGRFeatureDefnH, hTransform C.OGRCoordinateTransformationH) (*geojson.Feature, error) {
	// 获取几何信息
	hGeometry := C.OGR_F_GetGeometryRef(hFeature)
	var geometry orb.Geometry
	var err error

	if hGeometry != nil {
		// 如果需要坐标转换
		if hTransform != nil {
			// 克隆几何对象以避免修改原始数据
			hTransformedGeom := C.OGR_G_Clone(hGeometry)
			defer C.OGR_G_DestroyGeometry(hTransformedGeom)

			// 执行坐标转换
			if C.OGR_G_Transform(hTransformedGeom, hTransform) != C.OGRERR_NONE {
				return nil, fmt.Errorf("坐标转换失败")
			}

			geometry, err = convertGeometry(hTransformedGeom)
		} else {
			geometry, err = convertGeometry(hGeometry)
		}

		if err != nil {
			return nil, fmt.Errorf("转换几何信息失败: %v", err)
		}
	}

	// 创建Feature
	feature := geojson.NewFeature(geometry)

	// 获取属性信息
	fieldCount := C.OGR_FD_GetFieldCount(hLayerDefn)
	for i := 0; i < int(fieldCount); i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		if hFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(hFieldDefn))

		// 检查字段是否为空
		if C.OGR_F_IsFieldSet(hFeature, C.int(i)) == 0 {
			feature.Properties[fieldName] = nil
			continue
		}

		fieldType := C.OGR_Fld_GetType(hFieldDefn)
		value := getFieldValue(hFeature, C.int(i), fieldType)
		feature.Properties[fieldName] = value
	}

	return feature, nil
}

// convertGeometry 转换GDAL几何对象为orb几何对象
func convertGeometry(hGeometry C.OGRGeometryH) (orb.Geometry, error) {
	geometryType := C.OGR_G_GetGeometryType(hGeometry)

	switch geometryType {
	case C.wkbPoint, C.wkbPoint25D:
		return convertPoint(hGeometry), nil
	case C.wkbLineString, C.wkbLineString25D:
		return convertLineString(hGeometry), nil
	case C.wkbPolygon, C.wkbPolygon25D:
		return convertPolygon(hGeometry), nil
	case C.wkbMultiPoint, C.wkbMultiPoint25D:
		return convertMultiPoint(hGeometry), nil
	case C.wkbMultiLineString, C.wkbMultiLineString25D:
		return convertMultiLineString(hGeometry), nil
	case C.wkbMultiPolygon, C.wkbMultiPolygon25D:
		return convertMultiPolygon(hGeometry), nil
	default:
		return nil, fmt.Errorf("不支持的几何类型: %d", int(geometryType))
	}
}

// convertPoint 转换点几何
func convertPoint(hGeometry C.OGRGeometryH) orb.Point {
	x := float64(C.OGR_G_GetX(hGeometry, 0))
	y := float64(C.OGR_G_GetY(hGeometry, 0))
	return orb.Point{x, y}
}

// convertLineString 转换线几何
func convertLineString(hGeometry C.OGRGeometryH) orb.LineString {
	pointCount := int(C.OGR_G_GetPointCount(hGeometry))
	lineString := make(orb.LineString, pointCount)

	for i := 0; i < pointCount; i++ {
		x := float64(C.OGR_G_GetX(hGeometry, C.int(i)))
		y := float64(C.OGR_G_GetY(hGeometry, C.int(i)))
		lineString[i] = orb.Point{x, y}
	}

	return lineString
}

// convertPolygon 转换面几何
func convertPolygon(hGeometry C.OGRGeometryH) orb.Polygon {
	ringCount := int(C.OGR_G_GetGeometryCount(hGeometry))
	polygon := make(orb.Polygon, ringCount)

	for i := 0; i < ringCount; i++ {
		hRing := C.OGR_G_GetGeometryRef(hGeometry, C.int(i))
		pointCount := int(C.OGR_G_GetPointCount(hRing))
		ring := make(orb.Ring, pointCount)

		for j := 0; j < pointCount; j++ {
			x := float64(C.OGR_G_GetX(hRing, C.int(j)))
			y := float64(C.OGR_G_GetY(hRing, C.int(j)))
			ring[j] = orb.Point{x, y}
		}

		polygon[i] = ring
	}

	return polygon
}

// convertMultiPoint 转换多点几何
func convertMultiPoint(hGeometry C.OGRGeometryH) orb.MultiPoint {
	geomCount := int(C.OGR_G_GetGeometryCount(hGeometry))
	multiPoint := make(orb.MultiPoint, geomCount)

	for i := 0; i < geomCount; i++ {
		hPoint := C.OGR_G_GetGeometryRef(hGeometry, C.int(i))
		multiPoint[i] = convertPoint(hPoint)
	}

	return multiPoint
}

// convertMultiLineString 转换多线几何
func convertMultiLineString(hGeometry C.OGRGeometryH) orb.MultiLineString {
	geomCount := int(C.OGR_G_GetGeometryCount(hGeometry))
	multiLineString := make(orb.MultiLineString, geomCount)

	for i := 0; i < geomCount; i++ {
		hLineString := C.OGR_G_GetGeometryRef(hGeometry, C.int(i))
		multiLineString[i] = convertLineString(hLineString)
	}

	return multiLineString
}

// convertMultiPolygon 转换多面几何
func convertMultiPolygon(hGeometry C.OGRGeometryH) orb.MultiPolygon {
	geomCount := int(C.OGR_G_GetGeometryCount(hGeometry))
	multiPolygon := make(orb.MultiPolygon, geomCount)

	for i := 0; i < geomCount; i++ {
		hPolygon := C.OGR_G_GetGeometryRef(hGeometry, C.int(i))
		multiPolygon[i] = convertPolygon(hPolygon)
	}

	return multiPolygon
}

// getFieldValue 获取字段值
func getFieldValue(hFeature C.OGRFeatureH, fieldIndex C.int, fieldType C.OGRFieldType) interface{} {
	switch fieldType {
	case C.OFTString:
		value := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(value)
	case C.OFTInteger:
		value := C.OGR_F_GetFieldAsInteger(hFeature, fieldIndex)
		return int(value)
	case C.OFTInteger64:
		value := C.OGR_F_GetFieldAsInteger64(hFeature, fieldIndex)
		return int64(value)
	case C.OFTReal:
		value := C.OGR_F_GetFieldAsDouble(hFeature, fieldIndex)
		return float64(value)
	case C.OFTDate, C.OFTTime, C.OFTDateTime:
		value := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(value)
	default:
		value := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(value)
	}
}

// 获取字段类型名称
func getFieldTypeName(fieldType C.OGRFieldType) string {
	switch fieldType {
	case C.OFTInteger:
		return "Integer"
	case C.OFTInteger64:
		return "Integer64"
	case C.OFTReal:
		return "Real"
	case C.OFTString:
		return "String"
	case C.OFTDate:
		return "Date"
	case C.OFTTime:
		return "Time"
	case C.OFTDateTime:
		return "DateTime"
	case C.OFTBinary:
		return "Binary"
	case C.OFTIntegerList:
		return "IntegerList"
	case C.OFTRealList:
		return "RealList"
	case C.OFTStringList:
		return "StringList"
	default:
		return "Unknown"
	}
}

// 获取几何类型名称

func getGeometryTypeName(geoType C.OGRwkbGeometryType) string {
	geomTypeInt := int(geoType)

	// 提取基础类型（去除维度标志）
	baseType := geomTypeInt % 1000

	// 检测维度标志
	hasZ := false
	hasM := false

	if geomTypeInt >= 3000 && geomTypeInt < 4000 {
		// ZM 类型 (3000 系列)
		hasZ = true
		hasM = true
	} else if geomTypeInt >= 2000 && geomTypeInt < 3000 {
		// M 类型 (2000 系列)
		hasM = true
	} else if geomTypeInt >= 1000 && geomTypeInt < 2000 {
		// Z 类型 (1000 系列)
		hasZ = true
	} else if (geomTypeInt & 0x80000000) != 0 {
		// 25D 类型（旧格式，带 0x80000000 标志）
		hasZ = true
		baseType = geomTypeInt & 0x7FFFFFFF
	}

	// 根据基础类型返回几何类型名称
	var geomName string
	switch baseType {
	case 1: // wkbPoint
		geomName = "POINT"
	case 2: // wkbLineString
		geomName = "MULTILINESTRING"
	case 3: // wkbPolygon
		geomName = "MULTIPOLYGON"
	case 4: // wkbMultiPoint
		geomName = "MULTIPOINT"
	case 5: // wkbMultiLineString
		geomName = "MULTILINESTRING"
	case 6: // wkbMultiPolygon
		geomName = "MULTIPOLYGON"
	case 7: // wkbGeometryCollection
		geomName = "GEOMETRYCOLLECTION"
	default:
		return "GEOMETRY"
	}

	// 添加维度后缀
	if hasZ && hasM {
		return geomName
	} else if hasZ {
		return geomName
	} else if hasM {
		return geomName
	}

	return geomName
}

// GDBLayerInfo 直接从GDB获取的图层信息
type GDBLayerInfo struct {
	LayerName   string
	GeoType     string
	FieldInfos  []FieldInfo
	FeatureData []FeatureData
}

// FieldInfo 字段信息
type FieldInfo struct {
	Name   string
	Type   string
	DBType string // 数据库对应类型
}

// FeatureData 要素数据
type FeatureData struct {
	Properties map[string]interface{}
	WKBHex     string // 几何数据的WKB十六进制表示
}

// GDBToPostGIS 直接将GDB转换为PostGIS可用的数据结构
func GDBToPostGIS(gdbPath string) ([]GDBLayerInfo, error) {
	InitializeGDAL()

	var layers []GDBLayerInfo

	// 打开GDB文件
	cGdbPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGdbPath))

	hDataSource := C.OGROpen(cGdbPath, 0, nil)
	if hDataSource == nil {
		return layers, fmt.Errorf("无法打开GDB文件: %s", gdbPath)
	}
	defer C.OGRReleaseDataSource(hDataSource)

	// 创建目标坐标系 EPSG:4490
	hTargetSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hTargetSRS)

	if C.OSRImportFromEPSG(hTargetSRS, 4490) != C.OGRERR_NONE {
		return layers, fmt.Errorf("无法创建EPSG:4490坐标系")
	}

	// 获取图层数量
	layerCount := C.OGR_DS_GetLayerCount(hDataSource)

	// 遍历所有图层
	for i := 0; i < int(layerCount); i++ {
		hLayer := C.OGR_DS_GetLayer(hDataSource, C.int(i))
		if hLayer == nil {
			continue
		}

		// 获取图层信息
		layerInfo, err := processLayerDirect(hLayer, hTargetSRS)
		if err != nil {
			log.Printf("处理图层失败: %v", err)
			continue
		}

		layers = append(layers, layerInfo)
	}

	return layers, nil
}

// processLayerDirect 直接处理图层，不经过GeoJSON
func processLayerDirect(hLayer C.OGRLayerH, hTargetSRS C.OGRSpatialReferenceH) (GDBLayerInfo, error) {
	// 获取图层名称
	layerName := C.OGR_L_GetName(hLayer)
	layerNameStr := C.GoString(layerName)
	fmt.Printf("处理图层: %s\n", layerNameStr)

	// 获取图层几何类型
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	geomType := C.OGR_FD_GetGeomType(hLayerDefn)
	geoTypeStr := getGeometryTypeName(geomType)

	// 获取字段信息
	fieldInfos := getFieldInfos(hLayerDefn)

	// 设置坐标转换
	hSourceSRS := C.OGR_L_GetSpatialRef(hLayer)
	var hTransform C.OGRCoordinateTransformationH

	if hSourceSRS != nil && C.OSRIsSame(hSourceSRS, hTargetSRS) == 0 {
		hTransform = C.OCTNewCoordinateTransformation(hSourceSRS, hTargetSRS)
		if hTransform != nil {
			defer C.OCTDestroyCoordinateTransformation(hTransform)
			fmt.Printf("创建坐标转换: 从源坐标系到EPSG:4490\n")
		}
	}

	// 重置读取位置
	C.OGR_L_ResetReading(hLayer)

	// 读取要素数据
	var featureData []FeatureData
	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		feature, err := processFeatureDirect(hFeature, hLayerDefn, hTransform, fieldInfos)
		if err != nil {
			log.Printf("处理要素失败: %v", err)
			C.OGR_F_Destroy(hFeature)
			continue
		}

		featureData = append(featureData, feature)
		C.OGR_F_Destroy(hFeature)
	}

	return GDBLayerInfo{
		LayerName:   layerNameStr,
		GeoType:     geoTypeStr,
		FieldInfos:  fieldInfos,
		FeatureData: featureData,
	}, nil
}

// getFieldInfos 获取字段信息

func getFieldInfos(hLayerDefn C.OGRFeatureDefnH) []FieldInfo {
	fieldCount := int(C.OGR_FD_GetFieldCount(hLayerDefn))
	fieldInfos := make([]FieldInfo, 0, fieldCount)

	for i := 0; i < fieldCount; i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		if hFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(hFieldDefn))
		fieldType := C.OGR_Fld_GetType(hFieldDefn)

		fieldInfo := FieldInfo{
			Name:   fieldName,
			Type:   getFieldTypeName(fieldType),
			DBType: mapFieldTypeToPostGIS(hFieldDefn), // 传入字段定义而不是类型
		}

		fieldInfos = append(fieldInfos, fieldInfo)
	}

	return fieldInfos
}

// processFeatureDirect 直接处理要素
func processFeatureDirect(hFeature C.OGRFeatureH, hLayerDefn C.OGRFeatureDefnH,
	hTransform C.OGRCoordinateTransformationH, fieldInfos []FieldInfo) (FeatureData, error) {

	feature := FeatureData{
		Properties: make(map[string]interface{}),
	}

	// 处理几何数据
	hGeometry := C.OGR_F_GetGeometryRef(hFeature)
	if hGeometry != nil {
		// 坐标转换
		if hTransform != nil {
			hTransformedGeom := C.OGR_G_Clone(hGeometry)
			defer C.OGR_G_DestroyGeometry(hTransformedGeom)

			if C.OGR_G_Transform(hTransformedGeom, hTransform) != C.OGRERR_NONE {
				return feature, fmt.Errorf("坐标转换失败")
			}
			hGeometry = hTransformedGeom
		}

		// 转换为WKB
		wkbHex, err := geometryToWKBHex(hGeometry)
		if err != nil {
			return feature, fmt.Errorf("几何转换失败: %v", err)
		}
		feature.WKBHex = wkbHex
	}

	// 处理属性数据
	for i, fieldInfo := range fieldInfos {
		if C.OGR_F_IsFieldSet(hFeature, C.int(i)) == 0 {
			feature.Properties[fieldInfo.Name] = nil
			continue
		}

		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		fieldType := C.OGR_Fld_GetType(hFieldDefn)
		value := getFieldValue(hFeature, C.int(i), fieldType)
		feature.Properties[fieldInfo.Name] = value
	}

	return feature, nil
}

// geometryToWKBHex 将几何转换为WKB十六进制字符串
func geometryToWKBHex(hGeometry C.OGRGeometryH) (string, error) {
	// 获取WKB大小
	wkbSize := C.OGR_G_WkbSize(hGeometry)
	if wkbSize <= 0 {
		return "", fmt.Errorf("无效的几何对象")
	}

	// 分配内存
	wkbData := C.malloc(C.size_t(wkbSize))
	if wkbData == nil {
		return "", fmt.Errorf("内存分配失败")
	}
	defer C.free(wkbData)

	// 导出为WKB
	err := C.OGR_G_ExportToWkb(hGeometry, C.wkbNDR, (*C.uchar)(wkbData))
	if err != C.OGRERR_NONE {
		return "", fmt.Errorf("导出WKB失败")
	}

	// 转换为十六进制字符串
	wkbBytes := C.GoBytes(wkbData, wkbSize)
	hexStr := fmt.Sprintf("%x", wkbBytes)

	return hexStr, nil
}

// mapFieldTypeToPostGIS 将GDAL字段类型映射为PostGIS类型
func mapFieldTypeToPostGIS(hFieldDefn C.OGRFieldDefnH) string {
	fieldType := C.OGR_Fld_GetType(hFieldDefn)
	switch fieldType {
	case C.OFTInteger:
		return "INTEGER"
	case C.OFTInteger64:
		return "BIGINT"
	case C.OFTReal:
		return "DOUBLE PRECISION"
	case C.OFTString:
		// 获取字符串字段的实际宽度
		width := int(C.OGR_Fld_GetWidth(hFieldDefn))
		if width > 0 && width <= 10485760 {
			// 为了安全起见，增加一些缓冲空间
			safeWidth := width
			if safeWidth > 10485760 {
				return "TEXT"
			}
			if safeWidth <= 50 {
				safeWidth = width + 50
			} else {
				safeWidth = width + 20
			}
			return fmt.Sprintf("VARCHAR(%d)", safeWidth)
		} else if width > 10485760 {
			return "TEXT"
		}
		return "TEXT" // 默认使用TEXT而不是固定长度的VARCHAR
	case C.OFTDate:
		return "DATE"
	case C.OFTTime:
		return "TIME"
	case C.OFTDateTime:
		return "TIMESTAMP"
	default:
		return "TEXT" // 默认使用TEXT而不是VARCHAR(254)
	}
}

// PostGISRecord 表示从PostGIS查询得到的记录
type PostGISRecord struct {
	Properties map[string]interface{}
	WKBHex     string // 几何数据的WKB十六进制表示
}

// PostGISLayer 表示PostGIS图层数据
type PostGISLayer struct {
	LayerName string
	Records   []PostGISRecord
	Fields    []PostGISFieldInfo
}

// PostGISFieldInfo 字段信息
type PostGISFieldInfo struct {
	Name         string
	Type         string
	Width        int
	Precision    int
	IsNullable   bool
	DefaultValue *string
	IsPrimaryKey bool
	IsGeometry   bool
	GeometryType string // POINT, LINESTRING, POLYGON等
	SRID         int
}
type TableStructureInfo struct {
	TableName string
	Schema    string
	Fields    []PostGISFieldInfo
}

// 将PostGIS数据直接转换为Shapefile
func ConvertPostGISToShapefile(data []map[string]interface{}, outputPath string) error {
	if len(data) == 0 {
		return fmt.Errorf("没有数据需要转换")
	}

	// 初始化GDAL
	InitializeGDAL()

	// 分析数据结构，按几何类型分组
	layers := analyzeAndGroupData(data)

	// 为每种几何类型创建单独的Shapefile
	for geomType, layer := range layers {
		if len(layer.Records) == 0 {
			continue
		}

		// 生成输出文件名
		fileName := outputPath

		err := createShapefileFromLayer(layer, fileName, geomType)

		if err != nil {
			log.Printf("创建%s类型的Shapefile失败: %v", geomType, err)
			continue
		}

	}

	return nil
}

// queryTableFields 查询表字段信息
func queryTableFields(DB *gorm.DB, schema, table string) ([]PostGISFieldInfo, error) {

	db, _ := DB.DB()
	query := `
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
            CASE WHEN g.f_geometry_column IS NOT NULL THEN true ELSE false END as is_geometry,
            COALESCE(g.type, '') as geometry_type,
            COALESCE(g.srid, 0) as srid
        FROM 
            information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku 
                ON tc.constraint_name = ku.constraint_name
                AND tc.table_schema = ku.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) pk ON c.column_name = pk.column_name
        LEFT JOIN geometry_columns g 
            ON g.f_table_schema = c.table_schema 
            AND g.f_table_name = c.table_name 
            AND g.f_geometry_column = c.column_name
        WHERE 
            c.table_schema = $1
            AND c.table_name = $2
        ORDER BY c.ordinal_position
    `

	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []PostGISFieldInfo

	for rows.Next() {
		var field PostGISFieldInfo
		var maxLength, precision, scale sql.NullInt64
		var defaultValue sql.NullString
		var isNullable string

		err := rows.Scan(
			&field.Name,
			&field.Type,
			&maxLength,
			&precision,
			&scale,
			&isNullable,
			&defaultValue,
			&field.IsPrimaryKey,
			&field.IsGeometry,
			&field.GeometryType,
			&field.SRID,
		)
		if err != nil {
			return nil, err
		}

		// 处理字段属性
		field.IsNullable = (isNullable == "YES")
		if defaultValue.Valid {
			field.DefaultValue = &defaultValue.String
		}

		// 转换PostgreSQL数据类型到OGR字段类型
		field.Type, field.Width, field.Precision = convertPostgreSQLType(
			field.Type, maxLength, precision, scale)

		fields = append(fields, field)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("表 %s.%s 不存在或没有字段", schema, table)
	}

	return fields, nil
}

func GetTableStructure(DB *gorm.DB, tableName string) (*TableStructureInfo, error) {
	// 解析表名，支持schema.table格式
	schema := "public"
	table := tableName

	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		schema = parts[0]
		table = parts[1]
	}

	// 查询表结构
	fields, err := queryTableFields(DB, schema, table)
	if err != nil {
		return nil, fmt.Errorf("查询表结构失败: %v", err)
	}

	return &TableStructureInfo{
		TableName: table,
		Schema:    schema,
		Fields:    fields,
	}, nil
}

func ConvertPostGISToShapefileWithStructure(DB *gorm.DB, data []map[string]interface{}, outputPath string, tableName string) error {

	if len(data) == 0 {
		return fmt.Errorf("没有数据需要转换")
	}

	// 查询表结构
	tableStructure, err := GetTableStructure(DB, tableName)
	if err != nil {
		return fmt.Errorf("获取表结构失败: %v", err)
	}

	// 初始化GDAL
	InitializeGDAL()

	// 使用表结构分析和分组数据
	layers := analyzeAndGroupDataWithStructure(data, tableStructure)

	// 为每种几何类型创建单独的Shapefile
	for geomType, layer := range layers {
		if len(layer.Records) == 0 {
			continue
		}

		// 生成输出文件名
		fileName := outputPath

		err := createShapefileFromLayer(layer, fileName, geomType)
		if err != nil {
			log.Printf("创建%s类型的Shapefile失败: %v", geomType, err)
			continue
		}

		fmt.Printf("成功创建%s类型的Shapefile: %s\n", geomType, fileName)
	}

	return nil
}

// analyzeAndGroupDataWithStructure 使用表结构信息分析和分组数据
func analyzeAndGroupDataWithStructure(data []map[string]interface{},
	tableStructure *TableStructureInfo) map[string]PostGISLayer {

	layers := make(map[string]PostGISLayer)

	// 找到几何字段
	var geometryField *PostGISFieldInfo
	var attributeFields []PostGISFieldInfo

	for _, field := range tableStructure.Fields {
		if field.IsGeometry {
			geometryField = &field
		} else {
			// 过滤掉不需要的系统字段
			if !isSystemField(field.Name) {
				attributeFields = append(attributeFields, field)
			}
		}
	}

	if geometryField == nil {

		return layers
	}

	// 遍历所有记录，按几何类型分组
	for _, record := range data {
		if geomValue, ok := record[geometryField.Name]; ok {
			var geomStr string

			// 处理不同类型的几何数据
			switch v := geomValue.(type) {
			case string:
				geomStr = v
			case []byte:
				geomStr = string(v)
			default:
				log.Printf("警告: 不支持的几何数据类型: %T", v)
				continue
			}

			if geomStr == "" {
				continue
			}

			// 解析几何类型
			geomType := getGeometryTypeFromWKB(geomStr)
			if geomType == "" {
				log.Printf("警告: 无法识别几何类型，跳过记录")
				continue
			}

			// 创建PostGIS记录
			postgisRecord := PostGISRecord{
				Properties: make(map[string]interface{}),
				WKBHex:     geomStr,
			}

			// 根据表结构复制属性数据
			for _, field := range attributeFields {
				if value, exists := record[field.Name]; exists {
					// 根据字段类型进行数据转换和验证
					convertedValue := convertFieldValue(value, field)
					postgisRecord.Properties[field.Name] = convertedValue
				}
			}

			// 添加到对应的图层
			if layer, exists := layers[geomType]; exists {
				layer.Records = append(layer.Records, postgisRecord)
				layers[geomType] = layer
			} else {
				layers[geomType] = PostGISLayer{
					LayerName: fmt.Sprintf("%s_%s", tableStructure.TableName, geomType),
					Records:   []PostGISRecord{postgisRecord},
					Fields:    attributeFields,
				}
			}
		}
	}

	return layers
}

// isSystemField 判断是否为系统字段
func isSystemField(fieldName string) bool {
	systemFields := []string{
		"oid", "tableoid", "xmin", "cmin", "xmax", "cmax", "ctid",
		"ogc_fid", "gid", // 常见的PostGIS系统字段
	}

	fieldLower := strings.ToLower(fieldName)
	for _, sysField := range systemFields {
		if fieldLower == sysField {
			return true
		}
	}
	return false
}

// convertFieldValue 根据字段类型转换值
func convertFieldValue(value interface{}, field PostGISFieldInfo) interface{} {
	if value == nil {
		return nil
	}

	switch field.Type {
	case "Integer":
		switch v := value.(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
	case "Integer64":
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
	case "Real":
		switch v := value.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int32:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	case "String":
		str := fmt.Sprintf("%v", value)
		// 根据字段宽度截断字符串
		if field.Width > 0 && len(str) > field.Width {
			return str[:field.Width]
		}
		return str
	}

	// 默认转换为字符串
	return fmt.Sprintf("%v", value)
}

// convertPostgreSQLType 转换PostgreSQL数据类型到OGR类型
func convertPostgreSQLType(pgType string, maxLength, precision, scale sql.NullInt64) (string, int, int) {
	switch strings.ToLower(pgType) {
	case "integer", "int4", "serial":
		return "Integer", 10, 0
	case "bigint", "int8", "bigserial":
		return "Integer64", 20, 0
	case "smallint", "int2":
		return "Integer", 6, 0
	case "real", "float4":
		return "Real", 15, 7
	case "double precision", "float8":
		return "Real", 24, 15
	case "numeric", "decimal":
		width := 15
		prec := 6
		if precision.Valid {
			width = int(precision.Int64)
		}
		if scale.Valid {
			prec = int(scale.Int64)
		}
		return "Real", width, prec
	case "boolean":
		return "Integer", 1, 0
	case "date":
		return "Date", 10, 0
	case "time", "time without time zone":
		return "Time", 8, 0
	case "timestamp", "timestamp without time zone", "timestamp with time zone":
		return "DateTime", 19, 0
	case "character varying", "varchar", "text", "character", "char":
		width := 254 // Shapefile默认最大长度
		if maxLength.Valid && maxLength.Int64 > 0 {
			if maxLength.Int64 < 254 {
				width = int(maxLength.Int64)
			}
		}
		return "String", width, 0
	case "geometry", "geography":
		return "Geometry", 0, 0
	default:
		// 未知类型默认为字符串
		width := 100
		if maxLength.Valid && maxLength.Int64 > 0 && maxLength.Int64 < 254 {
			width = int(maxLength.Int64)
		}
		return "String", width, 0
	}
}

// analyzeAndGroupData 分析数据并按几何类型分组
func analyzeAndGroupData(data []map[string]interface{}) map[string]PostGISLayer {
	layers := make(map[string]PostGISLayer)

	// 从第一条记录分析字段结构
	firstRecord := data[0]
	var fields []PostGISFieldInfo

	for key, value := range firstRecord {
		if key != "geom" { // 排除几何字段
			field := PostGISFieldInfo{
				Name: key,
			}

			// 根据值类型推断字段类型
			switch v := value.(type) {
			case int, int32, int64:
				field.Type = "Integer"
				field.Width = 10
			case float32, float64:
				field.Type = "Real"
				field.Width = 15
				field.Precision = 6
			case string:
				field.Type = "String"
				field.Width = len(v)
				if field.Width < 50 {
					field.Width = 100 // 默认字符串长度
				}
			default:
				field.Type = "String"
				field.Width = 120
			}

			fields = append(fields, field)
		}
	}

	// 遍历所有记录，按几何类型分组
	for _, record := range data {
		if geomStr, ok := record["geom"].(string); ok && geomStr != "" {
			// 解析几何类型

			geomType := getGeometryTypeFromWKB(geomStr)
			if geomType == "" {
				continue
			}

			// 创建PostGIS记录
			postgisRecord := PostGISRecord{
				Properties: make(map[string]interface{}),
				WKBHex:     geomStr,
			}

			// 复制属性数据
			for key, value := range record {
				if key != "geom" {
					postgisRecord.Properties[key] = value
				}
			}

			// 添加到对应的图层
			if layer, exists := layers[geomType]; exists {
				layer.Records = append(layer.Records, postgisRecord)
				layers[geomType] = layer
			} else {
				layers[geomType] = PostGISLayer{
					LayerName: geomType,
					Records:   []PostGISRecord{postgisRecord},
					Fields:    fields,
				}
			}
		}
	}

	return layers
}

func getGeometryTypeFromWKB(wkbHex string) string {
	if len(wkbHex) < 18 { // 至少需要9个字节（18个十六进制字符）
		return ""
	}

	// 转换前9个字节来分析（包含可能的SRID）
	wkbData := make([]byte, 9)
	for i := 0; i < 9 && i*2+1 < len(wkbHex); i++ {
		hex := wkbHex[i*2 : i*2+2]
		var b byte
		fmt.Sscanf(hex, "%02x", &b)
		wkbData[i] = b
	}

	// 读取字节序
	byteOrder := wkbData[0]

	// 读取几何类型
	var geomType uint32
	if byteOrder == 1 { // 小端序
		geomType = uint32(wkbData[1]) | uint32(wkbData[2])<<8 | uint32(wkbData[3])<<16 | uint32(wkbData[4])<<24
	} else { // 大端序
		geomType = uint32(wkbData[1])<<24 | uint32(wkbData[2])<<16 | uint32(wkbData[3])<<8 | uint32(wkbData[4])
	}

	// 移除所有标志位，只保留基本几何类型
	cleanGeomType := geomType & 0x000000FF // 只保留最低8位

	// 判断几何类型
	switch cleanGeomType {
	case 1: // Point
		return "point"
	case 2: // LineString
		return "line"
	case 3: // Polygon
		return "polygon"
	case 4: // MultiPoint
		return "point"
	case 5: // MultiLineString
		return "line"
	case 6: // MultiPolygon
		return "polygon"
	default:
		log.Printf("未知几何类型: %d (原始: 0x%08X)", cleanGeomType, geomType)
		return "unknown"
	}
}

// 从图层数据创建Shapefile
func createShapefileFromLayer(layer PostGISLayer, outputPath, geomType string) error {
	// 创建Shapefile驱动
	cDriverName := C.CString("ESRI Shapefile")
	defer C.free(unsafe.Pointer(cDriverName))

	hDriver := C.OGRGetDriverByName(cDriverName)
	if hDriver == nil {
		return fmt.Errorf("无法获取Shapefile驱动")
	}

	// 创建数据源 - 添加GBK编码选项
	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))
	// 设置Shapefile编码为GBK/GB2312（中文Windows系统）
	C.CPLSetConfigOption(C.CString("SHAPE_ENCODING"), C.CString("GBK"))
	defer C.CPLSetConfigOption(C.CString("SHAPE_ENCODING"), nil)
	// 创建选项数组，指定GBK编码
	cEncodingOption := C.CString("ENCODING=GBK")
	defer C.free(unsafe.Pointer(cEncodingOption))

	// 创建选项指针数组

	hDataSource := C.OGR_Dr_CreateDataSource(hDriver, cOutputPath, nil)

	if hDataSource == nil {
		return fmt.Errorf("无法创建数据源: %s", outputPath)
	}
	defer C.OGRReleaseDataSource(hDataSource)

	// 创建坐标系
	hSRS, _ := CreateEPSG4490WithCorrectAxis()
	if hSRS == nil {
		return fmt.Errorf("无法创建坐标系")
	}

	// 确定OGR几何类型
	var ogrGeomType C.OGRwkbGeometryType
	switch geomType {
	case "point":
		ogrGeomType = C.wkbPoint
	case "line":
		ogrGeomType = C.wkbLineString
	case "polygon":
		ogrGeomType = C.wkbPolygon
	default:
		ogrGeomType = C.wkbUnknown
	}

	// 创建图层
	cLayerName := C.CString(layer.LayerName)
	defer C.free(unsafe.Pointer(cLayerName))

	hLayer := C.OGR_DS_CreateLayer(hDataSource, cLayerName, hSRS.cPtr, ogrGeomType, nil)
	if hLayer == nil {
		return fmt.Errorf("无法创建图层")
	}

	// 创建字段
	for _, field := range layer.Fields {
		err := createField(hLayer, field)
		if err != nil {
			log.Printf("创建字段%s失败: %v", field.Name, err)
		}
	}

	// 添加要素
	for _, record := range layer.Records {
		err := addFeatureToLayer(hLayer, record)
		if err != nil {
			log.Printf("添加要素失败: %v", err)
		}
	}
	cpg := strings.Replace(outputPath, ".shp", ".cpg", -1)
	createCpgFile(cpg)
	return nil
}
func createCpgFile(filename string) error {
	// 创建一个.cpg文件
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("无法创建文件: %v", err)
	}
	defer file.Close()

	// 写入内容"GBK"
	_, err = file.WriteString("GBK")
	if err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}

	return nil
}

// createField 创建字段
func createField(hLayer C.OGRLayerH, field PostGISFieldInfo) error {
	cFieldName := C.CString(field.Name)
	defer C.free(unsafe.Pointer(cFieldName))

	hFieldDefn := C.OGR_Fld_Create(cFieldName, getOGRFieldType(field.Type))
	defer C.OGR_Fld_Destroy(hFieldDefn)

	// 设置字段宽度和精度
	if field.Width > 0 {
		C.OGR_Fld_SetWidth(hFieldDefn, C.int(field.Width))
	}
	if field.Precision > 0 {
		C.OGR_Fld_SetPrecision(hFieldDefn, C.int(field.Precision))
	}

	// 添加字段到图层
	if C.OGR_L_CreateField(hLayer, hFieldDefn, 1) != C.OGRERR_NONE {
		return fmt.Errorf("无法创建字段: %s", field.Name)
	}

	return nil
}

// getOGRFieldType 获取OGR字段类型
func getOGRFieldType(fieldType string) C.OGRFieldType {
	switch fieldType {
	case "Integer":
		return C.OFTInteger
	case "Real":
		return C.OFTReal
	case "String":
		return C.OFTString
	default:
		return C.OFTString
	}
}

// addFeatureToLayer 向图层添加要素
func addFeatureToLayer(hLayer C.OGRLayerH, record PostGISRecord) error {
	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)

	// 创建要素
	hFeature := C.OGR_F_Create(hLayerDefn)
	if hFeature == nil {
		return fmt.Errorf("无法创建要素")
	}
	defer C.OGR_F_Destroy(hFeature)

	// 设置几何
	if record.WKBHex != "" {

		hGeometry, _ := CreateGeometryFromWKBHex(record.WKBHex)
		if hGeometry != nil {
			C.OGR_F_SetGeometry(hFeature, hGeometry.cPtr)
			C.OGR_G_DestroyGeometry(hGeometry.cPtr)
		}
	}

	// 设置属性
	fieldCount := int(C.OGR_FD_GetFieldCount(hLayerDefn))
	for i := 0; i < fieldCount; i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(hFieldDefn))

		if value, exists := record.Properties[fieldName]; exists && value != nil {
			setFieldValue(hFeature, i, value)
		}
	}

	// 添加要素到图层
	if C.OGR_L_CreateFeature(hLayer, hFeature) != C.OGRERR_NONE {
		return fmt.Errorf("无法添加要素到图层")
	}

	return nil
}

// setFieldValue 设置字段值
func setFieldValue(hFeature C.OGRFeatureH, fieldIndex int, value interface{}) {
	switch v := value.(type) {
	case int:
		C.OGR_F_SetFieldInteger(hFeature, C.int(fieldIndex), C.int(v))
	case int32:
		C.OGR_F_SetFieldInteger(hFeature, C.int(fieldIndex), C.int(v))
	case int64:
		C.OGR_F_SetFieldInteger64(hFeature, C.int(fieldIndex), C.longlong(v))
	case float32:
		C.OGR_F_SetFieldDouble(hFeature, C.int(fieldIndex), C.double(v))
	case float64:
		C.OGR_F_SetFieldDouble(hFeature, C.int(fieldIndex), C.double(v))
	case string:
		cValue := C.CString(v)
		defer C.free(unsafe.Pointer(cValue))
		C.OGR_F_SetFieldString(hFeature, C.int(fieldIndex), cValue)
	default:
		// 默认转换为字符串
		strValue := fmt.Sprintf("%v", v)
		cValue := C.CString(strValue)
		defer C.free(unsafe.Pointer(cValue))
		C.OGR_F_SetFieldString(hFeature, C.int(fieldIndex), cValue)
	}
}

// SHPLayerInfo SHP图层信息结构
type SHPLayerInfo struct {
	LayerName   string
	GeoType     string
	FieldInfos  []FieldInfo
	FeatureData []FeatureData
}

// SHPToPostGIS 将SHP文件转换为PostGIS格式
func SHPToPostGIS(shpPath string) (SHPLayerInfo, error) {
	InitializeGDAL()

	var layer SHPLayerInfo

	// 检测编码
	encoding := detectSHPEncoding(shpPath)
	fmt.Printf(encoding)
	// 设置GDAL编码选项
	cEncodingKey := C.CString("SHAPE_ENCODING")
	cEncodingValue := C.CString(encoding)
	cFilenameKey := C.CString("GDAL_FILENAME_IS_GBK")
	cFilenameValue := C.CString("NO")

	C.CPLSetConfigOption(cEncodingKey, cEncodingValue)
	C.CPLSetConfigOption(cFilenameKey, cFilenameValue)

	defer func() {
		C.CPLSetConfigOption(cEncodingKey, nil)
		C.CPLSetConfigOption(cFilenameKey, nil)
		C.free(unsafe.Pointer(cEncodingKey))
		C.free(unsafe.Pointer(cEncodingValue))
		C.free(unsafe.Pointer(cFilenameKey))
		C.free(unsafe.Pointer(cFilenameValue))
	}()

	// 打开SHP文件
	cShpPath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cShpPath))

	hDataSource := C.OGROpen(cShpPath, 0, nil)
	if hDataSource == nil {
		return layer, fmt.Errorf("无法打开SHP文件: %s", shpPath)
	}
	defer C.OGRReleaseDataSource(hDataSource)

	// 创建目标坐标系 EPSG:4490
	hTargetSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hTargetSRS)

	if C.OSRImportFromEPSG(hTargetSRS, 4490) != C.OGRERR_NONE {
		return layer, fmt.Errorf("无法创建EPSG:4490坐标系")
	}

	// 获取图层数量（SHP通常只有一个图层）
	layerCount := C.OGR_DS_GetLayerCount(hDataSource)
	if layerCount == 0 {
		return layer, fmt.Errorf("SHP文件中没有找到图层")
	}

	// 获取第一个图层（SHP文件通常只有一个图层）
	hLayer := C.OGR_DS_GetLayer(hDataSource, 0)
	if hLayer == nil {
		return layer, fmt.Errorf("无法获取SHP图层")
	}

	// 处理图层信息
	layerInfo, err := processSHPLayerDirect(hLayer, hTargetSRS, shpPath)
	if err != nil {
		return layer, fmt.Errorf("处理SHP图层失败: %v", err)
	}

	return layerInfo, nil
}

func detectSHPEncoding(shpPath string) string {
	// 检查是否存在.cpg文件（编码文件）
	cpgPath := strings.TrimSuffix(shpPath, filepath.Ext(shpPath)) + ".cpg"
	if _, err := os.Stat(cpgPath); err == nil {
		// 读取.cpg文件内容
		if content, err := os.ReadFile(cpgPath); err == nil {
			encoding := strings.TrimSpace(string(content))
			return encoding
		}
	}

	// 默认返回GBK（中国常用编码）
	return "GBK"
}

// processSHPLayerDirect 直接处理SHP图层
func processSHPLayerDirect(hLayer C.OGRLayerH, hTargetSRS C.OGRSpatialReferenceH, shpPath string) (SHPLayerInfo, error) {
	var layerInfo SHPLayerInfo

	// 获取图层名称（从文件路径提取）
	layerName := extractLayerNameFromPath(shpPath)
	layerInfo.LayerName = layerName

	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return layerInfo, fmt.Errorf("无法获取图层定义")
	}

	// 获取几何类型
	geoType := C.OGR_FD_GetGeomType(hLayerDefn)
	layerInfo.GeoType = getGeometryTypeName(geoType)

	// 获取字段信息
	fieldInfos, err := getSHPFieldInfos(hLayerDefn)
	if err != nil {
		return layerInfo, fmt.Errorf("获取字段信息失败: %v", err)
	}
	layerInfo.FieldInfos = fieldInfos

	// 获取源坐标系
	hSourceSRS := C.OGR_L_GetSpatialRef(hLayer)

	// 创建坐标转换
	var hTransform C.OGRCoordinateTransformationH
	if hSourceSRS != nil {
		hTransform = C.OCTNewCoordinateTransformation(hSourceSRS, hTargetSRS)
		if hTransform != nil {
			defer C.OCTDestroyCoordinateTransformation(hTransform)
		}
	}

	// 重置读取位置
	C.OGR_L_ResetReading(hLayer)

	// 读取要素数据
	var featureData []FeatureData
	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		feature, err := processSHPFeature(hFeature, fieldInfos, hTransform)
		if err != nil {
			C.OGR_F_Destroy(hFeature)
			log.Printf("处理要素失败: %v", err)
			continue
		}

		featureData = append(featureData, feature)
		C.OGR_F_Destroy(hFeature)
	}

	layerInfo.FeatureData = featureData
	return layerInfo, nil
}

// extractLayerNameFromPath 从文件路径提取图层名称
func extractLayerNameFromPath(filePath string) string {
	// 获取文件名（不包含路径）
	fileName := filepath.Base(filePath)

	// 移除扩展名
	ext := filepath.Ext(fileName)
	if ext != "" {
		fileName = fileName[:len(fileName)-len(ext)]
	}

	return fileName
}

// getSHPFieldInfos 获取SHP字段信息
func getSHPFieldInfos(hLayerDefn C.OGRFeatureDefnH) ([]FieldInfo, error) {
	var fieldInfos []FieldInfo

	fieldCount := C.OGR_FD_GetFieldCount(hLayerDefn)

	for i := 0; i < int(fieldCount); i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		if hFieldDefn == nil {
			continue
		}

		// 获取字段名称
		fieldNamePtr := C.OGR_Fld_GetNameRef(hFieldDefn)
		fieldName := C.GoString(fieldNamePtr)

		// 获取字段类型
		fieldType := C.OGR_Fld_GetType(hFieldDefn)
		dbType := convertOGRFieldTypeToDBType(fieldType)

		// 获取字段长度和精度
		width := C.OGR_Fld_GetWidth(hFieldDefn)
		precision := C.OGR_Fld_GetPrecision(hFieldDefn)

		// 根据类型和长度调整数据库类型
		if dbType == "VARCHAR" && width > 0 {
			dbType = fmt.Sprintf("VARCHAR(%d)", width)
		} else if dbType == "NUMERIC" && precision > 0 {
			dbType = fmt.Sprintf("NUMERIC(%d,%d)", width, precision)
		}

		fieldInfo := FieldInfo{
			Name:   fieldName,
			DBType: dbType,
		}

		fieldInfos = append(fieldInfos, fieldInfo)
	}

	return fieldInfos, nil
}

// processSHPFeature 处理SHP要素
func processSHPFeature(hFeature C.OGRFeatureH, fieldInfos []FieldInfo, hTransform C.OGRCoordinateTransformationH) (FeatureData, error) {
	var feature FeatureData
	feature.Properties = make(map[string]interface{})

	// 处理属性数据
	for _, fieldInfo := range fieldInfos {
		fieldName := fieldInfo.Name
		cFieldName := C.CString(fieldName)
		fieldIndex := C.OGR_F_GetFieldIndex(hFeature, cFieldName)
		C.free(unsafe.Pointer(cFieldName))

		if fieldIndex < 0 {
			continue
		}

		// 检查字段是否为空
		if C.OGR_F_IsFieldNull(hFeature, fieldIndex) == 1 {
			feature.Properties[fieldName] = nil
			continue
		}

		// 根据字段类型获取值
		fieldDefn := C.OGR_F_GetFieldDefnRef(hFeature, fieldIndex)
		fieldType := C.OGR_Fld_GetType(fieldDefn)

		var value interface{}
		switch fieldType {
		case C.OFTInteger:
			value = int(C.OGR_F_GetFieldAsInteger(hFeature, fieldIndex))
		case C.OFTInteger64:
			value = int64(C.OGR_F_GetFieldAsInteger64(hFeature, fieldIndex))
		case C.OFTReal:
			value = float64(C.OGR_F_GetFieldAsDouble(hFeature, fieldIndex))
		case C.OFTString:
			strPtr := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
			value = C.GoString(strPtr)
		default:
			strPtr := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
			value = C.GoString(strPtr)
		}

		feature.Properties[fieldName] = value
	}

	// 处理几何数据
	hGeometry := C.OGR_F_GetGeometryRef(hFeature)
	if hGeometry != nil {
		// 检查原始几何体是否有效
		if C.OGR_G_IsValid(hGeometry) == 0 {
			hFixedGeom := C.OGR_G_MakeValid(hGeometry)
			if hFixedGeom != nil && C.OGR_G_IsValid(hFixedGeom) == 1 {
				hGeometry = hFixedGeom
				defer C.OGR_G_DestroyGeometry(hFixedGeom)
			} else {
				return feature, fmt.Errorf("原始几何体无效")
			}

		}

		// 克隆几何对象以避免修改原始数据
		hGeomClone := C.OGR_G_Clone(hGeometry)
		if hGeomClone == nil {
			return feature, fmt.Errorf("几何体克隆失败")
		}
		defer C.OGR_G_DestroyGeometry(hGeomClone)

		// 坐标转换
		if hTransform != nil {
			if C.OGR_G_Transform(hGeomClone, hTransform) != C.OGRERR_NONE {
				return feature, fmt.Errorf("坐标转换失败")
			}
		}

		// 再次检查转换后的几何体是否有效
		if C.OGR_G_IsValid(hGeomClone) == 0 {
			return feature, fmt.Errorf("转换后几何体无效")
		}

		// 获取WKB大小并动态分配内存
		wkbSize := C.OGR_G_WkbSize(hGeomClone)
		if wkbSize <= 0 {
			return feature, fmt.Errorf("无法获取WKB大小")
		}

		// 动态分配内存，添加一些额外空间以防万一
		bufferSize := wkbSize + 1024
		wkbPtr := C.malloc(C.size_t(bufferSize))
		if wkbPtr == nil {
			return feature, fmt.Errorf("内存分配失败")
		}
		defer C.free(wkbPtr)

		// 清零内存
		C.memset(wkbPtr, 0, C.size_t(bufferSize))

		// 转换为WKB格式
		result := C.OGR_G_ExportToWkb(hGeomClone, C.wkbNDR, (*C.uchar)(wkbPtr))
		if result == C.OGRERR_NONE {
			// 转换为十六进制字符串
			wkbBytes := C.GoBytes(wkbPtr, wkbSize)
			feature.WKBHex = hex.EncodeToString(wkbBytes)
		} else {
			return feature, fmt.Errorf("WKB导出失败，错误码: %d", int(result))
		}
	}

	return feature, nil
}

// convertOGRFieldTypeToDBType 转换OGR字段类型为数据库类型
func convertOGRFieldTypeToDBType(fieldType C.OGRFieldType) string {
	switch fieldType {
	case C.OFTInteger:
		return "INTEGER"
	case C.OFTInteger64:
		return "BIGINT"
	case C.OFTReal:
		return "DOUBLE PRECISION"
	case C.OFTString:
		return "VARCHAR"
	case C.OFTDate:
		return "DATE"
	case C.OFTTime:
		return "TIME"
	case C.OFTDateTime:
		return "TIMESTAMP"
	case C.OFTBinary:
		return "BYTEA"
	default:
		return "TEXT"
	}
}

func ConvertGeoJSONToGDALLayer(fc *geojson.FeatureCollection, layerName string) (*GDALLayer, error) {
	if fc == nil {
		return nil, fmt.Errorf("输入的 FeatureCollection 不能为空")
	}

	// 1. 获取内存驱动 (GDAL Memory Driver)
	cDriverName := C.CString("Memory")
	defer C.free(unsafe.Pointer(cDriverName))
	hDriver := C.OGRGetDriverByName(cDriverName)
	if hDriver == nil {
		return nil, fmt.Errorf("无法获取内存驱动")
	}

	// 2. 创建内存数据源
	hDataSource := C.OGR_Dr_CreateDataSource(hDriver, nil, nil) // 第二个参数为 nil，表示创建内存数据源
	if hDataSource == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 3. 创建坐标系 (这里以 EPSG:4326 为例，可以根据需要修改)
	hSRS := C.OSRNewSpatialReference(nil)
	if hSRS == nil {
		C.OGRReleaseDataSource(hDataSource)
		return nil, fmt.Errorf("无法创建空间参考系统")
	}
	defer C.OSRDestroySpatialReference(hSRS)

	if C.OSRImportFromEPSG(hSRS, 4326) != C.OGRERR_NONE { // 假设 GeoJSON 是 WGS84
		C.OGRReleaseDataSource(hDataSource)
		return nil, fmt.Errorf("无法创建 EPSG:4326 坐标系")
	}

	// 4. 确定图层几何类型 (这里简化处理，取第一个要素的类型，或默认为 Unknown)
	var ogrGeomType C.OGRwkbGeometryType = C.wkbUnknown
	if len(fc.Features) > 0 && fc.Features[0].Geometry != nil {
		geomType := fc.Features[0].Geometry.GeoJSONType() // 使用库提供的方法
		ogrGeomType = getOGRGeometryTypeFromOrb(geomType)
		if ogrGeomType == C.wkbUnknown {
			log.Printf("警告: 无法识别第一个要素的几何类型 '%s'，图层将使用 wkbUnknown 类型", geomType)
		}
	}

	// 5. 创建图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))
	hLayer := C.OGR_DS_CreateLayer(hDataSource, cLayerName, hSRS, ogrGeomType, nil)
	if hLayer == nil {
		C.OGRReleaseDataSource(hDataSource)
		return nil, fmt.Errorf("无法创建图层: %s", layerName)
	}

	// 6. (可选) 根据第一个要素的属性创建字段 (这里可以更智能地分析所有要素来确定字段)
	if len(fc.Features) > 0 && len(fc.Features[0].Properties) > 0 {
		err := createFieldsFromProperties(hLayer, fc.Features[0].Properties)
		if err != nil {
			C.OGRReleaseDataSource(hDataSource)
			return nil, fmt.Errorf("创建字段失败: %v", err)
		}
	}

	// 7. 遍历 GeoJSON 特征，创建 GDAL 要素并添加到图层
	for _, feature := range fc.Features {
		err := addGeoJSONFeatureToGDALLayer(hLayer, feature)
		if err != nil {
			log.Printf("添加要素失败: %v", err)
			// 可以选择继续处理其他要素或返回错误
			// 这里选择继续
		}
	}

	// 8. 创建并返回 GDALLayer 结构体
	return &GDALLayer{
		layer:   hLayer,
		dataset: hDataSource,
		driver:  hDriver,
	}, nil
}

// getOGRGeometryTypeFromOrb 根据 orb.Geometry GeoJSONType 获取对应的 OGRwkbGeometryType
func getOGRGeometryTypeFromOrb(orbType string) C.OGRwkbGeometryType {
	switch orbType {
	case "Point":
		return C.wkbPoint
	case "LineString":
		return C.wkbLineString
	case "Polygon":
		return C.wkbPolygon
	case "MultiPoint":
		return C.wkbMultiPoint
	case "MultiLineString":
		return C.wkbMultiLineString
	case "MultiPolygon":
		return C.wkbMultiPolygon
	case "GeometryCollection":
		return C.wkbGeometryCollection
	default:
		return C.wkbUnknown
	}
}

// createFieldsFromProperties 根据 GeoJSON 属性创建 GDAL 字段
// 注意：这里进行了简单的类型推断，可能需要更复杂的逻辑
func createFieldsFromProperties(hLayer C.OGRLayerH, properties geojson.Properties) error {
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return fmt.Errorf("无法获取图层定义")
	}

	for key, value := range properties {
		var ogrFieldType C.OGRFieldType

		// 简单类型推断
		switch v := value.(type) {
		case string:
			ogrFieldType = C.OFTString
		case float64: // JSON number
			// 检查是否为整数
			if v == float64(int64(v)) {
				ogrFieldType = C.OFTInteger64 // 使用 Integer64 以防数值过大
			} else {
				ogrFieldType = C.OFTReal
			}
		case bool: // JSON boolean
			// GDAL 没有原生布尔类型，通常用整数表示 (0/1)
			ogrFieldType = C.OFTInteger
		case nil: // JSON null
			ogrFieldType = C.OFTString // 默认用字符串，后续赋值为 NULL
		default:
			// 其他类型（如 map, array）通常转为字符串或 JSON 字符串
			ogrFieldType = C.OFTString
		}

		cFieldName := C.CString(key)
		hFieldDefn := C.OGR_Fld_Create(cFieldName, ogrFieldType)
		C.free(unsafe.Pointer(cFieldName))

		if hFieldDefn == nil {
			log.Printf("无法创建字段定义: %s", key)
			continue // 尝试创建下一个字段
		}

		// 设置字段长度 (对于字符串类型)
		if ogrFieldType == C.OFTString {
			// 可以根据属性值的最大长度设置，或者使用默认值
			C.OGR_Fld_SetWidth(hFieldDefn, 254) // 设置默认长度
		}

		// 添加字段到图层
		if C.OGR_L_CreateField(hLayer, hFieldDefn, 1) != C.OGRERR_NONE {
			C.OGR_Fld_Destroy(hFieldDefn)
			log.Printf("无法创建字段: %s", key)
			continue // 尝试创建下一个字段
		}
		C.OGR_Fld_Destroy(hFieldDefn) // 创建后销毁字段定义
	}
	return nil
}

// addGeoJSONFeatureToGDALLayer 将单个 geojson.Feature 添加到 GDAL Layer
func addGeoJSONFeatureToGDALLayer(hLayer C.OGRLayerH, feature *geojson.Feature) error {
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return fmt.Errorf("无法获取图层定义")
	}

	// 1. 创建 GDAL 要素
	hFeature := C.OGR_F_Create(hLayerDefn)
	if hFeature == nil {
		return fmt.Errorf("无法创建 GDAL 要素")
	}
	defer C.OGR_F_Destroy(hFeature) // 确保在函数结束时销毁

	// 2. 设置几何
	if feature.Geometry != nil {
		hGeometry, err := orbGeometryToOGR(feature.Geometry)
		if err != nil {
			return fmt.Errorf("转换几何失败: %v", err)
		}
		if hGeometry != nil {
			C.OGR_F_SetGeometry(hFeature, hGeometry)
			C.OGR_G_DestroyGeometry(hGeometry) // SetGeometry 会增加引用计数，这里销毁原始指针
		}
	}

	// 3. 设置属性
	for key, value := range feature.Properties {
		cFieldName := C.CString(key)
		fieldIndex := C.OGR_F_GetFieldIndex(hFeature, cFieldName)
		C.free(unsafe.Pointer(cFieldName))

		if fieldIndex < 0 {
			log.Printf("警告: 图层中不存在字段 '%s'，跳过该属性", key)
			continue
		}

		err := setGDALFieldValue(hFeature, int(fieldIndex), value)
		if err != nil {
			log.Printf("设置字段 '%s' 值失败: %v", key, err)
			// 继续处理其他字段
		}
	}

	// 4. 将要素添加到图层
	if C.OGR_L_CreateFeature(hLayer, hFeature) != C.OGRERR_NONE {
		return fmt.Errorf("无法将要素添加到图层")
	}

	return nil
}

// orbGeometryToOGR 将 orb.Geometry 转换为 OGRGeometryH
func orbGeometryToOGR(geom orb.Geometry) (C.OGRGeometryH, error) {
	if geom == nil {
		return nil, nil // 允许空几何
	}

	var hGeom C.OGRGeometryH
	switch g := geom.(type) {
	case orb.Point:
		hGeom = C.OGR_G_CreateGeometry(C.wkbPoint)
		if hGeom != nil {
			C.OGR_G_SetPoint_2D(hGeom, 0, C.double(g[0]), C.double(g[1]))
		}
	case orb.LineString:
		hGeom = C.OGR_G_CreateGeometry(C.wkbLineString)
		if hGeom != nil {
			for _, p := range g {
				C.OGR_G_AddPoint_2D(hGeom, C.double(p[0]), C.double(p[1]))
			}
		}
	case orb.Polygon:
		hGeom = C.OGR_G_CreateGeometry(C.wkbPolygon)
		if hGeom != nil {
			for _, ring := range g {
				hRing := C.OGR_G_CreateGeometry(C.wkbLinearRing)
				if hRing != nil {
					for _, p := range ring {
						C.OGR_G_AddPoint_2D(hRing, C.double(p[0]), C.double(p[1]))
					}
					// 确保环闭合
					if len(ring) > 0 {
						C.OGR_G_AddPoint_2D(hRing, C.double(ring[0][0]), C.double(ring[0][1]))
					}
					// 添加环到多边形
					if C.OGR_G_AddGeometryDirectly(hGeom, hRing) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(hRing) // 如果添加失败，销毁环
					}
				}
			}
		}
	case orb.MultiPoint:
		hGeom = C.OGR_G_CreateGeometry(C.wkbMultiPoint)
		if hGeom != nil {
			for _, p := range g {
				hPoint := C.OGR_G_CreateGeometry(C.wkbPoint)
				if hPoint != nil {
					C.OGR_G_SetPoint_2D(hPoint, 0, C.double(p[0]), C.double(p[1]))
					if C.OGR_G_AddGeometryDirectly(hGeom, hPoint) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(hPoint)
					}
				}
			}
		}
	case orb.MultiLineString:
		hGeom = C.OGR_G_CreateGeometry(C.wkbMultiLineString)
		if hGeom != nil {
			for _, ls := range g {
				hLine := C.OGR_G_CreateGeometry(C.wkbLineString)
				if hLine != nil {
					for _, p := range ls {
						C.OGR_G_AddPoint_2D(hLine, C.double(p[0]), C.double(p[1]))
					}
					if C.OGR_G_AddGeometryDirectly(hGeom, hLine) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(hLine)
					}
				}
			}
		}
	case orb.MultiPolygon:
		hGeom = C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
		if hGeom != nil {
			for _, poly := range g {
				hPoly := C.OGR_G_CreateGeometry(C.wkbPolygon)
				if hPoly != nil {
					for _, ring := range poly {
						hRing := C.OGR_G_CreateGeometry(C.wkbLinearRing)
						if hRing != nil {
							for _, p := range ring {
								C.OGR_G_AddPoint_2D(hRing, C.double(p[0]), C.double(p[1]))
							}
							// 确保环闭合
							if len(ring) > 0 {
								C.OGR_G_AddPoint_2D(hRing, C.double(ring[0][0]), C.double(ring[0][1]))
							}
							if C.OGR_G_AddGeometryDirectly(hPoly, hRing) != C.OGRERR_NONE {
								C.OGR_G_DestroyGeometry(hRing)
							}
						}
					}
					if C.OGR_G_AddGeometryDirectly(hGeom, hPoly) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(hPoly)
					}
				}
			}
		}
	case orb.Collection: // Handle GeometryCollection
		hGeom = C.OGR_G_CreateGeometry(C.wkbGeometryCollection)
		if hGeom != nil {
			for _, subGeom := range g {
				hSubGeom, err := orbGeometryToOGR(subGeom)
				if err != nil {
					log.Printf("转换 GeometryCollection 中的子几何失败: %v", err)
					continue
				}
				if hSubGeom != nil {
					if C.OGR_G_AddGeometryDirectly(hGeom, hSubGeom) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(hSubGeom)
					}
				}
			}
		}
	default:
		return nil, fmt.Errorf("不支持的几何类型: %T", g)
	}

	if hGeom == nil {
		return nil, fmt.Errorf("创建 OGR 几何对象失败")
	}

	return hGeom, nil
}

// setGDALFieldValue 设置 GDAL 要素的字段值
func setGDALFieldValue(hFeature C.OGRFeatureH, fieldIndex int, value interface{}) error {
	cFieldIndex := C.int(fieldIndex)

	// 获取字段类型以进行类型安全的设置
	hFieldDefn := C.OGR_F_GetFieldDefnRef(hFeature, cFieldIndex)
	if hFieldDefn == nil {
		return fmt.Errorf("无法获取字段定义")
	}
	ogrFieldType := C.OGR_Fld_GetType(hFieldDefn)

	switch v := value.(type) {
	case string:
		if ogrFieldType == C.OFTString {
			cValue := C.CString(v)
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, cFieldIndex, cValue)
		} else {
			// 如果类型不匹配，可以尝试转换或设置为字符串
			cValue := C.CString(v)
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, cFieldIndex, cValue)
		}
	case float64: // JSON number
		if v == float64(int64(v)) && ogrFieldType == C.OFTInteger64 {
			C.OGR_F_SetFieldInteger64(hFeature, cFieldIndex, C.longlong(v))
		} else if ogrFieldType == C.OFTReal {
			C.OGR_F_SetFieldDouble(hFeature, cFieldIndex, C.double(v))
		} else if ogrFieldType == C.OFTInteger {
			C.OGR_F_SetFieldInteger(hFeature, cFieldIndex, C.int(v))
		} else {
			// 类型不匹配，转为字符串
			cValue := C.CString(fmt.Sprintf("%v", v))
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, cFieldIndex, cValue)
		}
	case bool: // JSON boolean
		if ogrFieldType == C.OFTInteger { // 使用整数 0/1 表示布尔值
			if v {
				C.OGR_F_SetFieldInteger(hFeature, cFieldIndex, 1)
			} else {
				C.OGR_F_SetFieldInteger(hFeature, cFieldIndex, 0)
			}
		} else {
			// 类型不匹配，转为字符串
			cValue := C.CString(fmt.Sprintf("%v", v))
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, cFieldIndex, cValue)
		}
	case nil: // JSON null
		C.OGR_F_SetFieldNull(hFeature, cFieldIndex)
	default: // 其他类型
		cValue := C.CString(fmt.Sprintf("%v", v))
		defer C.free(unsafe.Pointer(cValue))
		C.OGR_F_SetFieldString(hFeature, cFieldIndex, cValue)
	}
	return nil
}
