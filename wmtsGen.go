// wmtsGen.go - 修改后的核心代码
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
	TileSize int
	Opacity  float64
	ColorMap []VectorColorRule
}

// VectorColorRule 矢量颜色规则
type VectorColorRule struct {
	AttributeName  string
	AttributeValue string
	Color          string
	ColorValues    map[string]string
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

// VectorFeature 矢量要素
type VectorFeature struct {
	WKB        []byte
	Attributes map[string]string
}

// NewVectorTileGenerator 创建矢量瓦片生成器
func NewVectorTileGenerator(config VectorTileConfig) *VectorTileGenerator {
	if config.TileSize <= 0 {
		config.TileSize = 256
	}
	if config.Opacity <= 0 || config.Opacity > 1.0 {
		config.Opacity = 1.0
	}
	return &VectorTileGenerator{config: config}
}

// CreateVectorLayerFromWKB 从WKB创建矢量图层（线程安全）
func (gen *VectorTileGenerator) CreateVectorLayerFromWKB(features []VectorFeature, srid int, geomType C.OGRwkbGeometryType) (*GDALLayer, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	// 保护GDAL驱动获取和数据源创建
	gdalMutex.Lock()
	defer gdalMutex.Unlock()

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

	if result := C.OSRImportFromEPSG(srs, C.int(srid)); result != C.OGRERR_NONE {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("设置EPSG失败: %d", int(result))
	}

	// 确保使用Multi类型
	multiGeomType := ensureMultiGeometryType(geomType)

	layerName := C.CString("vector_layer")
	defer C.free(unsafe.Pointer(layerName))

	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, multiGeomType, nil)
	if layer == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建图层失败")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: ds,
		driver:  driver,
	}

	// 创建字段和添加要素
	if err := gen.createFieldsFromFeatures(gdalLayer, features); err != nil {
		gdalLayer.Close()
		return nil, fmt.Errorf("创建字段失败: %v", err)
	}

	successCount, failCount := gen.addFeaturesToLayer(gdalLayer, features, srs, multiGeomType)
	if successCount == 0 {
		gdalLayer.Close()
		return nil, fmt.Errorf("所有要素添加失败，成功: %d, 失败: %d", successCount, failCount)
	}

	return gdalLayer, nil
}

// ensureMultiGeometryType 确保返回Multi几何类型
func ensureMultiGeometryType(geomType C.OGRwkbGeometryType) C.OGRwkbGeometryType {
	if isMultiGeometryType(geomType) {
		return geomType
	}
	switch geomType {
	case C.wkbPoint:
		return C.wkbMultiPoint
	case C.wkbLineString:
		return C.wkbMultiLineString
	case C.wkbPolygon:
		return C.wkbMultiPolygon
	default:
		return C.wkbMultiPolygon
	}
}

// RasterizeVectorLayer 栅格化矢量图层（线程安全）
func (gen *VectorTileGenerator) RasterizeVectorLayer(gdalLayer *GDALLayer, bounds VectorTileBounds) ([]byte, error) {
	// 使用图层级别的锁，确保同一图层不会被并发操作
	gdalLayer.mu.Lock()
	defer gdalLayer.mu.Unlock()

	tileSize := gen.config.TileSize

	// 创建栅格数据集
	rasterDS := C.createRasterDataset(
		C.int(tileSize),
		C.int(tileSize),
		4,
		C.double(bounds.MinLon),
		C.double(bounds.MinLat),
		C.double(bounds.MaxLon),
		C.double(bounds.MaxLat),
		4326,
	)
	if rasterDS == nil {
		return nil, fmt.Errorf("创建栅格数据集失败")
	}
	defer C.GDALClose(rasterDS)

	// 栅格化
	if err := gen.rasterizeWithColorsC(rasterDS, gdalLayer); err != nil {
		return nil, err
	}

	// 转换为PNG
	return gen.rasterToPNGC(rasterDS)
}

// rasterizeWithColorsC 使用C函数进行栅格化
func (gen *VectorTileGenerator) rasterizeWithColorsC(rasterDS C.GDALDatasetH, gdalLayer *GDALLayer) error {
	C.OGR_L_ResetReading(gdalLayer.layer)

	if len(gen.config.ColorMap) == 0 {
		return gen.rasterizeSingleColor(rasterDS, gdalLayer.layer, 128, 128, 128)
	}

	rule := gen.config.ColorMap[0]

	if rule.AttributeName == "默认" && rule.AttributeValue == "默认" {
		rgb := ParseColor(rule.Color)
		return gen.rasterizeSingleColor(rasterDS, gdalLayer.layer, rgb.R, rgb.G, rgb.B)
	}

	if len(rule.ColorValues) > 0 {
		return gen.rasterizeByAttributeC(rasterDS, gdalLayer, rule.AttributeName, rule.ColorValues)
	}

	rgb := ParseColor(rule.Color)
	return gen.rasterizeSingleColor(rasterDS, gdalLayer.layer, rgb.R, rgb.G, rgb.B)
}

// rasterizeSingleColor 单色栅格化
func (gen *VectorTileGenerator) rasterizeSingleColor(rasterDS C.GDALDatasetH, layer C.OGRLayerH, r, g, b int) error {
	result := C.rasterizeLayerWithColor(
		rasterDS,
		layer,
		C.int(r), C.int(g), C.int(b),
		C.int(gen.config.Opacity*255),
	)
	if result != 0 {
		return fmt.Errorf("栅格化失败，错误码: %d", int(result))
	}
	return nil
}

// rasterizeByAttributeC 按属性值栅格化（关键修复点）
func (gen *VectorTileGenerator) rasterizeByAttributeC(rasterDS C.GDALDatasetH, gdalLayer *GDALLayer, attrName string, colorValues map[string]string) error {
	// 注意：此时已经持有 gdalLayer.mu 锁，无需再加锁

	for attrValue, colorStr := range colorValues {
		rgb := ParseColor(colorStr)
		alpha := int(gen.config.Opacity * 255)

		cAttrName := C.CString(attrName)
		cAttrValue := C.CString(attrValue)

		result := C.rasterizeLayerByAttribute(
			rasterDS,
			gdalLayer.layer,
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

	C.OGR_L_SetAttributeFilter(gdalLayer.layer, nil)
	C.OGR_L_ResetReading(gdalLayer.layer)

	return nil
}

// rasterToPNGC 转换为PNG
func (gen *VectorTileGenerator) rasterToPNGC(rasterDS C.GDALDatasetH) ([]byte, error) {
	if rasterDS == nil {
		return nil, fmt.Errorf("无效的栅格数据集")
	}

	imageBuffer := C.rasterDatasetToPNG(rasterDS)
	if imageBuffer == nil {
		return nil, fmt.Errorf("PNG转换失败")
	}
	defer C.freeImageBuffer(imageBuffer)

	if imageBuffer.size <= 0 || imageBuffer.data == nil {
		return nil, fmt.Errorf("PNG数据为空")
	}

	return C.GoBytes(unsafe.Pointer(imageBuffer.data), C.int(imageBuffer.size)), nil
}

// ========== 辅助函数 ==========

func (gen *VectorTileGenerator) createFieldsFromFeatures(gdalLayer *GDALLayer, features []VectorFeature) error {
	if len(features) == 0 || len(features[0].Attributes) == 0 {
		return nil
	}

	for attrName, attrValue := range features[0].Attributes {
		fieldType := gen.inferFieldType(attrValue)
		fieldName := C.CString(attrName)
		fieldDefn := C.OGR_Fld_Create(fieldName, fieldType)

		if fieldDefn != nil {
			if fieldType == C.OFTString {
				C.OGR_Fld_SetWidth(fieldDefn, 254)
			}
			C.OGR_L_CreateField(gdalLayer.layer, fieldDefn, 1)
			C.OGR_Fld_Destroy(fieldDefn)
		}
		C.free(unsafe.Pointer(fieldName))
	}
	return nil
}

func (gen *VectorTileGenerator) inferFieldType(value string) C.OGRFieldType {
	if _, err := strconv.ParseInt(value, 10, 32); err == nil {
		return C.OFTInteger
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return C.OFTReal
	}
	return C.OFTString
}

func (gen *VectorTileGenerator) addFeaturesToLayer(gdalLayer *GDALLayer, features []VectorFeature, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) (int, int) {
	featureDefn := C.OGR_L_GetLayerDefn(gdalLayer.layer)
	if featureDefn == nil {
		return 0, len(features)
	}

	successCount, failCount := 0, 0

	for _, feat := range features {
		feature := C.OGR_F_Create(featureDefn)
		if feature == nil {
			failCount++
			continue
		}

		if len(feat.WKB) > 0 && gen.setFeatureGeometry(feature, feat.WKB, srs, multiGeomType) {
			gen.setFeatureAttributes(feature, feat.Attributes)
			if C.OGR_L_CreateFeature(gdalLayer.layer, feature) == C.OGRERR_NONE {
				successCount++
			} else {
				failCount++
			}
		} else {
			failCount++
		}

		C.OGR_F_Destroy(feature)
	}

	return successCount, failCount
}

func (gen *VectorTileGenerator) setFeatureGeometry(feature C.OGRFeatureH, wkb []byte, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) bool {
	var geom C.OGRGeometryH
	cWkb := (*C.uchar)(C.CBytes(wkb))
	defer C.free(unsafe.Pointer(cWkb))

	if C.OGR_G_CreateFromWkb(unsafe.Pointer(cWkb), srs, &geom, C.int(len(wkb))) != C.OGRERR_NONE || geom == nil {
		return false
	}
	defer C.OGR_G_DestroyGeometry(geom)

	if C.OGR_G_IsEmpty(geom) != 0 {
		return false
	}

	var finalGeom C.OGRGeometryH
	if isMultiGeometryType(C.OGR_G_GetGeometryType(geom)) {
		finalGeom = C.OGR_G_Clone(geom)
	} else {
		finalGeom = convertToMultiGeometry(geom, multiGeomType)
	}

	if finalGeom == nil {
		return false
	}

	result := C.OGR_F_SetGeometry(feature, finalGeom)
	C.OGR_G_DestroyGeometry(finalGeom)
	return result == C.OGRERR_NONE
}

func (gen *VectorTileGenerator) setFeatureAttributes(feature C.OGRFeatureH, attributes map[string]string) {
	for attrName, attrValue := range attributes {
		cAttrName := C.CString(attrName)
		fieldIndex := C.OGR_F_GetFieldIndex(feature, cAttrName)

		if fieldIndex >= 0 {
			fieldDefn := C.OGR_F_GetFieldDefnRef(feature, fieldIndex)
			if fieldDefn != nil {
				gen.setFieldValue(feature, fieldIndex, attrValue, C.OGR_Fld_GetType(fieldDefn))
			}
		}
		C.free(unsafe.Pointer(cAttrName))
	}
}

func (gen *VectorTileGenerator) setFieldValue(feature C.OGRFeatureH, fieldIndex C.int, value string, fieldType C.OGRFieldType) {
	switch fieldType {
	case C.OFTInteger:
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(v))
		}
	case C.OFTInteger64:
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(v))
		}
	case C.OFTReal:
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(v))
		}
	default:
		cValue := C.CString(value)
		C.OGR_F_SetFieldString(feature, fieldIndex, cValue)
		C.free(unsafe.Pointer(cValue))
	}
}

// ========== 颜色解析函数 ==========

type RGBA struct {
	R, G, B, A int
}

func ParseColor(color string) RGBA {
	color = strings.TrimSpace(color)
	colorLower := strings.ToLower(color)

	switch {
	case strings.HasPrefix(colorLower, "#"):
		return parseHexColor(color)
	case strings.HasPrefix(colorLower, "rgba"):
		return parseRGBAColor(color)
	case strings.HasPrefix(colorLower, "rgb"):
		return parseRGBColor(color)
	default:
		return RGBA{R: 128, G: 128, B: 128, A: 255}
	}
}

func parseHexColor(hex string) RGBA {
	hex = strings.TrimPrefix(strings.ToLower(hex), "#")
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

func parseRGBColor(color string) RGBA {
	re := regexp.MustCompile(`(?i)rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)`)
	if matches := re.FindStringSubmatch(color); len(matches) == 4 {
		r, _ := strconv.Atoi(matches[1])
		g, _ := strconv.Atoi(matches[2])
		b, _ := strconv.Atoi(matches[3])
		return RGBA{R: clamp(r, 0, 255), G: clamp(g, 0, 255), B: clamp(b, 0, 255), A: 255}
	}
	return RGBA{R: 128, G: 128, B: 128, A: 255}
}

func parseRGBAColor(color string) RGBA {
	re := regexp.MustCompile(`(?i)rgba\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)`)
	if matches := re.FindStringSubmatch(color); len(matches) == 5 {
		r, _ := strconv.Atoi(matches[1])
		g, _ := strconv.Atoi(matches[2])
		b, _ := strconv.Atoi(matches[3])
		a, _ := strconv.ParseFloat(matches[4], 64)
		alphaInt := int(a)
		if a <= 1.0 {
			alphaInt = int(a * 255)
		}
		return RGBA{R: clamp(r, 0, 255), G: clamp(g, 0, 255), B: clamp(b, 0, 255), A: clamp(alphaInt, 0, 255)}
	}
	return RGBA{R: 128, G: 128, B: 128, A: 255}
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func CalculateTileBounds(x, y, z int) VectorTileBounds {
	n := math.Pow(2, float64(z))
	minLon := float64(x)/n*360.0 - 180.0
	maxLon := float64(x+1)/n*360.0 - 180.0
	minLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y+1)/n)))
	maxLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y)/n)))
	return VectorTileBounds{
		MinLon: minLon,
		MinLat: minLatRad * 180.0 / math.Pi,
		MaxLon: maxLon,
		MaxLat: maxLatRad * 180.0 / math.Pi,
	}
}
