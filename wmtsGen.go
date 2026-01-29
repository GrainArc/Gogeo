package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"unsafe"
)

// VectorTileConfig 矢量瓦片配置
type VectorTileConfig struct {
	TileSize int               // 瓦片大小，默认256
	Opacity  float64           // 透明度 0-1
	ColorMap []VectorColorRule // 颜色映射规则
}

// VectorColorRule 矢量颜色规则
type VectorColorRule struct {
	AttributeName  string            // 属性字段名，"默认"表示单一颜色
	AttributeValue string            // 属性值，"默认"表示所有值
	Color          string            // 颜色值（支持 hex、rgb、rgba）
	ColorValues    map[string]string // 属性值到颜色的映射
}

// VectorTileBounds 瓦片边界
type VectorTileBounds struct {
	MinLon float64
	MinLat float64
	MaxLon float64
	MaxLat float64
}

// VectorTileGenerator 矢量瓦片生成器
type VectorTileGenerator struct {
	config VectorTileConfig
}

// NewVectorTileGenerator 创建矢量瓦片生成器
func NewVectorTileGenerator(config VectorTileConfig) *VectorTileGenerator {
	if config.TileSize <= 0 {
		config.TileSize = 256
	}
	if config.Opacity <= 0 {
		config.Opacity = 1.0
	}
	if config.Opacity > 1.0 {
		config.Opacity = 1.0
	}

	return &VectorTileGenerator{
		config: config,
	}
}

func (gen *VectorTileGenerator) CreateVectorLayerFromWKB(features []VectorFeature, srid int, geomType C.OGRwkbGeometryType) (*GDALLayer, error) {

	if len(features) == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	// 使用MEM驱动创建数据源
	driverName := C.CString("MEM")
	defer C.free(unsafe.Pointer(driverName))

	driver := C.OGRGetDriverByName(driverName)
	if driver == nil {
		return nil, fmt.Errorf("无法获取MEM驱动")
	}

	dsName := C.CString("")
	defer C.free(unsafe.Pointer(dsName))

	ds := C.OGR_Dr_CreateDataSource(driver, dsName, nil)
	if ds == nil {
		return nil, fmt.Errorf("创建内存数据源失败")
	}

	// 创建空间参考
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建空间参考失败")
	}
	defer C.OSRDestroySpatialReference(srs)

	result := C.OSRImportFromEPSG(srs, C.int(srid))
	if result != C.OGRERR_NONE {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("设置EPSG失败: %d", int(result))
	}

	// 确保使用Multi类型
	var multiGeomType C.OGRwkbGeometryType
	if isMultiGeometryType(geomType) {
		multiGeomType = geomType
	} else {
		// 转换为对应的Multi类型
		switch geomType {
		case C.wkbPoint:
			multiGeomType = C.wkbMultiPoint
		case C.wkbLineString:
			multiGeomType = C.wkbMultiLineString
		case C.wkbPolygon:
			multiGeomType = C.wkbMultiPolygon
		default:
			multiGeomType = C.wkbMultiPolygon
		}
	}

	// 创建图层
	layerName := C.CString("vector_layer")
	defer C.free(unsafe.Pointer(layerName))

	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, multiGeomType, nil)
	if layer == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建图层失败")
	}

	// 创建GDALLayer包装器
	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: ds,
		driver:  driver,
	}

	// 添加属性字段（从第一个要素推断）
	if err := gen.createFieldsFromFeatures(gdalLayer, features); err != nil {
		gdalLayer.Close()
		return nil, fmt.Errorf("创建字段失败: %v", err)
	}

	// 添加要素
	successCount, failCount := gen.addFeaturesToLayer(gdalLayer, features, srs, multiGeomType)

	if successCount == 0 {
		gdalLayer.Close()
		return nil, fmt.Errorf("所有要素添加失败，成功: %d, 失败: %d", successCount, failCount)
	}

	fmt.Printf("要素添加完成，成功: %d, 失败: %d\n", successCount, failCount)
	return gdalLayer, nil
}

// createFieldsFromFeatures 从要素创建字段
func (gen *VectorTileGenerator) createFieldsFromFeatures(gdalLayer *GDALLayer, features []VectorFeature) error {
	if len(features) == 0 || len(features[0].Attributes) == 0 {
		return nil
	}

	// 从第一个要素推断字段类型
	for attrName, attrValue := range features[0].Attributes {
		fieldType := gen.inferFieldType(attrValue)

		fieldName := C.CString(attrName)
		fieldDefn := C.OGR_Fld_Create(fieldName, fieldType)

		if fieldDefn == nil {
			C.free(unsafe.Pointer(fieldName))
			continue
		}

		// 设置字段长度（对于字符串类型）
		if fieldType == C.OFTString {
			C.OGR_Fld_SetWidth(fieldDefn, 254)
		}

		createResult := C.OGR_L_CreateField(gdalLayer.layer, fieldDefn, 1)
		C.OGR_Fld_Destroy(fieldDefn)
		C.free(unsafe.Pointer(fieldName))

		if createResult != C.OGRERR_NONE {
			fmt.Printf("警告: 创建字段 %s 失败，错误码: %d\n", attrName, int(createResult))
		}
	}

	return nil
}

// inferFieldType 推断字段类型
func (gen *VectorTileGenerator) inferFieldType(value string) C.OGRFieldType {
	// 尝试解析为整数
	if _, err := strconv.ParseInt(value, 10, 32); err == nil {
		return C.OFTInteger
	}

	// 尝试解析为浮点数
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return C.OFTReal
	}

	// 默认为字符串
	return C.OFTString
}

// addFeaturesToLayer 添加要素到图层
func (gen *VectorTileGenerator) addFeaturesToLayer(gdalLayer *GDALLayer, features []VectorFeature, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) (int, int) {
	featureDefn := C.OGR_L_GetLayerDefn(gdalLayer.layer)
	if featureDefn == nil {
		return 0, len(features)
	}

	successCount := 0
	failCount := 0

	for i, feat := range features {
		feature := C.OGR_F_Create(featureDefn)
		if feature == nil {
			failCount++
			continue
		}

		// 设置几何字段
		if len(feat.WKB) > 0 {
			if gen.setFeatureGeometry(feature, feat.WKB, srs, multiGeomType) {
				successCount++
			} else {
				failCount++
				if i < 5 {
					fmt.Printf("警告: 要素 %d 几何设置失败\n", i)
				}
			}
		}

		// 设置属性
		gen.setFeatureAttributes(feature, feat.Attributes)

		// 添加要素到图层
		createResult := C.OGR_L_CreateFeature(gdalLayer.layer, feature)
		C.OGR_F_Destroy(feature)

		if createResult != C.OGRERR_NONE {
			if successCount > 0 {
				successCount-- // 几何成功但添加失败
			}
			failCount++
		}
	}

	return successCount, failCount
}

// setFeatureGeometry 设置要素几何
func (gen *VectorTileGenerator) setFeatureGeometry(feature C.OGRFeatureH, wkb []byte, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) bool {
	var geom C.OGRGeometryH
	cWkb := (*C.uchar)(C.CBytes(wkb))
	defer C.free(unsafe.Pointer(cWkb))

	wkbSize := C.int(len(wkb))

	// 从WKB创建几何体
	result := C.OGR_G_CreateFromWkb(
		unsafe.Pointer(cWkb),
		srs,
		&geom,
		wkbSize,
	)

	if result != C.OGRERR_NONE || geom == nil {
		return false
	}
	defer C.OGR_G_DestroyGeometry(geom)

	// 检查几何体是否为空
	if C.OGR_G_IsEmpty(geom) != 0 {
		return false
	}

	// 检查几何类型并转换为Multi类型（如果需要）
	currentGeomType := C.OGR_G_GetGeometryType(geom)
	var finalGeom C.OGRGeometryH

	if isMultiGeometryType(currentGeomType) {
		// 已经是Multi类型，克隆一份
		finalGeom = C.OGR_G_Clone(geom)
	} else {
		// 转换为Multi类型
		finalGeom = convertToMultiGeometry(geom, multiGeomType)
	}

	if finalGeom == nil {
		return false
	}
	defer func() {
		if finalGeom != geom {
			C.OGR_G_DestroyGeometry(finalGeom)
		}
	}()

	// 设置几何到要素
	setResult := C.OGR_F_SetGeometry(feature, finalGeom)
	return setResult == C.OGRERR_NONE
}

// setFeatureAttributes 设置要素属性
func (gen *VectorTileGenerator) setFeatureAttributes(feature C.OGRFeatureH, attributes map[string]string) {
	for attrName, attrValue := range attributes {
		cAttrName := C.CString(attrName)
		fieldIndex := C.OGR_F_GetFieldIndex(feature, cAttrName)

		if fieldIndex >= 0 {
			// 根据字段类型设置值
			fieldDefn := C.OGR_F_GetFieldDefnRef(feature, fieldIndex)
			if fieldDefn != nil {
				fieldType := C.OGR_Fld_GetType(fieldDefn)
				gen.setFieldValue(feature, fieldIndex, attrValue, fieldType)
			}
		}

		C.free(unsafe.Pointer(cAttrName))
	}
}

// setFieldValue 根据字段类型设置值
func (gen *VectorTileGenerator) setFieldValue(feature C.OGRFeatureH, fieldIndex C.int, value string, fieldType C.OGRFieldType) {
	switch fieldType {
	case C.OFTInteger:
		if intVal, err := strconv.ParseInt(value, 10, 32); err == nil {
			C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(intVal))
		}
	case C.OFTInteger64:
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(intVal))
		}
	case C.OFTReal:
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(floatVal))
		}
	default:
		// 默认作为字符串处理
		cValue := C.CString(value)
		C.OGR_F_SetFieldString(feature, fieldIndex, cValue)
		C.free(unsafe.Pointer(cValue))
	}
}

// VectorFeature 矢量要素
type VectorFeature struct {
	WKB        []byte            // WKB几何数据
	Attributes map[string]string // 属性字段
}

func (gen *VectorTileGenerator) RasterizeVectorLayer(GDALLayer *GDALLayer, bounds VectorTileBounds) ([]byte, error) {
	tileSize := gen.config.TileSize

	// 调用C函数创建栅格数据集
	rasterDS := C.createRasterDataset(
		C.int(tileSize),
		C.int(tileSize),
		4, // RGBA
		C.double(bounds.MinLon),
		C.double(bounds.MinLat),
		C.double(bounds.MaxLon),
		C.double(bounds.MaxLat),
		4326, // EPSG:4326
	)
	if rasterDS == nil {
		return nil, fmt.Errorf("创建栅格数据集失败")
	}
	defer C.GDALClose(rasterDS)

	// 根据颜色配置栅格化
	if err := gen.rasterizeWithColorsC(rasterDS, GDALLayer); err != nil {
		return nil, err
	}

	// 转换为PNG
	return gen.rasterToPNGC(rasterDS)
}

// 使用C函数进行栅格化
func (gen *VectorTileGenerator) rasterizeWithColorsC(rasterDS C.GDALDatasetH, GDALLayer *GDALLayer) error {
	// 重置图层
	C.OGR_L_ResetReading(GDALLayer.layer)
	defer C.OGR_L_ResetReading(GDALLayer.layer)
	defer C.OGR_L_SetAttributeFilter(GDALLayer.layer, nil)

	// 如果没有颜色配置，使用默认灰色
	if len(gen.config.ColorMap) == 0 {
		result := C.rasterizeLayerWithColor(
			rasterDS,
			GDALLayer.layer,
			128, 128, 128,
			C.int(gen.config.Opacity*255),
		)
		if result != 0 {
			return fmt.Errorf("栅格化失败，错误码: %d", int(result))
		}
		return nil
	}

	rule := gen.config.ColorMap[0]

	// 单一颜色模式
	if rule.AttributeName == "默认" && rule.AttributeValue == "默认" {
		rgb := ParseColor(rule.Color)
		result := C.rasterizeLayerWithColor(
			rasterDS,
			GDALLayer.layer,
			C.int(rgb.R), C.int(rgb.G), C.int(rgb.B),
			C.int(gen.config.Opacity*255),
		)
		if result != 0 {
			return fmt.Errorf("栅格化失败，错误码: %d", int(result))
		}
		return nil
	}

	// 按属性值分类渲染
	if len(rule.ColorValues) > 0 {
		return gen.rasterizeByAttributeC(rasterDS, GDALLayer, rule.AttributeName, rule.ColorValues)
	}

	// 默认使用规则中的颜色
	rgb := ParseColor(rule.Color)
	result := C.rasterizeLayerWithColor(
		rasterDS,
		GDALLayer.layer,
		C.int(rgb.R), C.int(rgb.G), C.int(rgb.B),
		C.int(gen.config.Opacity*255),
	)
	if result != 0 {
		return fmt.Errorf("栅格化失败，错误码: %d", int(result))
	}
	return nil
}

// 按属性值栅格化（C版本）
func (gen *VectorTileGenerator) rasterizeByAttributeC(rasterDS C.GDALDatasetH, GDALLayer *GDALLayer, attrName string, colorValues map[string]string) error {
	gdalMutex.Lock()
	defer gdalMutex.Unlock()

	for attrValue, colorStr := range colorValues {
		rgb := ParseColor(colorStr)
		alpha := int(gen.config.Opacity * 255)

		cAttrName := C.CString(attrName)
		cAttrValue := C.CString(attrValue)

		result := C.rasterizeLayerByAttribute(
			rasterDS,
			GDALLayer.layer,
			cAttrName,
			cAttrValue,
			C.int(rgb.R), C.int(rgb.G), C.int(rgb.B), C.int(alpha),
		)

		C.free(unsafe.Pointer(cAttrName))
		C.free(unsafe.Pointer(cAttrValue))

		if result != 0 {
			return fmt.Errorf("按属性栅格化失败，属性=%s, 值=%s, 错误码=%d", attrName, attrValue, int(result))
		}
	}

	// 清除过滤器
	C.OGR_L_SetAttributeFilter(GDALLayer.layer, nil)
	C.OGR_L_ResetReading(GDALLayer.layer)

	return nil
}

// 使用C函数将栅格转换为PNG
func (gen *VectorTileGenerator) rasterToPNGC(rasterDS C.GDALDatasetH) ([]byte, error) {
	if rasterDS == nil {
		return nil, fmt.Errorf("无效的栅格数据集")
	}

	// 调用C函数转换为PNG
	imageBuffer := C.rasterDatasetToPNG(rasterDS)
	if imageBuffer == nil {
		return nil, fmt.Errorf("PNG转换失败")
	}
	defer C.freeImageBuffer(imageBuffer)

	// 复制数据到Go切片
	if imageBuffer.size <= 0 || imageBuffer.data == nil {
		return nil, fmt.Errorf("PNG数据为空")
	}

	pngData := C.GoBytes(unsafe.Pointer(imageBuffer.data), C.int(imageBuffer.size))
	return pngData, nil
}

// RGBA 颜色结构
type RGBA struct {
	R int
	G int
	B int
	A int
}

// ParseColor 解析颜色字符串
func ParseColor(color string) RGBA {
	color = strings.TrimSpace(color)
	colorLower := strings.ToLower(color)

	if strings.HasPrefix(colorLower, "#") {
		return parseHexColor(color)
	}

	if strings.HasPrefix(colorLower, "rgba") {
		return parseRGBAColor(color)
	}

	if strings.HasPrefix(colorLower, "rgb") {
		return parseRGBColor(color)
	}

	return RGBA{R: 128, G: 128, B: 128, A: 255}
}

// parseHexColor 解析十六进制颜色
func parseHexColor(hex string) RGBA {
	hex = strings.TrimPrefix(hex, "#")
	hex = strings.ToLower(hex)

	if len(hex) == 3 {
		hex = string([]byte{hex[0], hex[0], hex[1], hex[1], hex[2], hex[2]})
	}

	if len(hex) != 6 {
		return RGBA{R: 128, G: 128, B: 128, A: 255}
	}

	var r, g, b int
	fmt.Sscanf(hex, "%02x%02x%02x", &r, &g, &b)
	return RGBA{R: r, G: g, B: b, A: 255}
}

// parseRGBColor 解析 rgb(r, g, b) 格式
func parseRGBColor(color string) RGBA {
	re := regexp.MustCompile(`(?i)rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)`)
	matches := re.FindStringSubmatch(color)

	if len(matches) != 4 {
		return RGBA{R: 128, G: 128, B: 128, A: 255}
	}

	r, _ := strconv.Atoi(matches[1])
	g, _ := strconv.Atoi(matches[2])
	b, _ := strconv.Atoi(matches[3])

	return RGBA{
		R: clamp(r, 0, 255),
		G: clamp(g, 0, 255),
		B: clamp(b, 0, 255),
		A: 255,
	}
}

// parseRGBAColor 解析 rgba(r, g, b, a) 格式
func parseRGBAColor(color string) RGBA {
	re := regexp.MustCompile(`(?i)rgba\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)`)
	matches := re.FindStringSubmatch(color)

	if len(matches) != 5 {
		return RGBA{R: 128, G: 128, B: 128, A: 255}
	}

	r, _ := strconv.Atoi(matches[1])
	g, _ := strconv.Atoi(matches[2])
	b, _ := strconv.Atoi(matches[3])
	a, _ := strconv.ParseFloat(matches[4], 64)

	alphaInt := int(a)
	if a <= 1.0 {
		alphaInt = int(a * 255)
	}

	return RGBA{
		R: clamp(r, 0, 255),
		G: clamp(g, 0, 255),
		B: clamp(b, 0, 255),
		A: clamp(alphaInt, 0, 255),
	}
}

// clamp 限制值在指定范围内
func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// CalculateTileBounds 计算瓦片边界（Web Mercator）
func CalculateTileBounds(x, y, z int) VectorTileBounds {
	n := math.Pow(2, float64(z))

	minLon := float64(x)/n*360.0 - 180.0
	maxLon := float64(x+1)/n*360.0 - 180.0

	minLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y+1)/n)))
	maxLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y)/n)))

	minLat := minLatRad * 180.0 / math.Pi
	maxLat := maxLatRad * 180.0 / math.Pi

	return VectorTileBounds{
		MinLon: minLon,
		MinLat: minLat,
		MaxLon: maxLon,
		MaxLat: maxLat,
	}
}
