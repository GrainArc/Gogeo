package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"bytes"
	"fmt"
	"image"
	"image/png"
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

	// 注册GDAL驱动
	C.GDALAllRegister()
	C.OGRRegisterAll()

	return &VectorTileGenerator{
		config: config,
	}
}

// CreateVectorLayerFromWKB 从WKB数据创建矢量图层
func (gen *VectorTileGenerator) CreateVectorLayerFromWKB(features []VectorFeature, srid int) (*GDALLayer, error) {
	// 创建内存数据源
	driver := C.OGRGetDriverByName(C.CString("Memory"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	ds := C.OGR_Dr_CreateDataSource(driver, C.CString(""), nil)
	if ds == nil {
		return nil, fmt.Errorf("创建内存数据源失败")
	}

	// 创建空间参考
	srs := C.OSRNewSpatialReference(nil)
	C.OSRImportFromEPSG(srs, C.int(srid))
	defer C.OSRDestroySpatialReference(srs)

	// 创建图层
	layerName := C.CString("vector_layer")
	defer C.free(unsafe.Pointer(layerName))

	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, C.wkbUnknown, nil)
	if layer == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建图层失败")
	}

	// 添加属性字段（从第一个要素推断）
	if len(features) > 0 && len(features[0].Attributes) > 0 {
		for attrName := range features[0].Attributes {
			fieldName := C.CString(attrName)
			fieldDefn := C.OGR_Fld_Create(fieldName, C.OFTString)
			C.OGR_L_CreateField(layer, fieldDefn, 1)
			C.OGR_Fld_Destroy(fieldDefn)
			C.free(unsafe.Pointer(fieldName))
		}
	}

	featureDefn := C.OGR_L_GetLayerDefn(layer)

	// 添加要素
	for _, feat := range features {
		feature := C.OGR_F_Create(featureDefn)

		// 设置几何
		if len(feat.WKB) > 0 {
			geom := C.OGR_G_CreateGeometry(C.wkbUnknown)
			if geom != nil {
				cWkb := C.CBytes(feat.WKB)
				result := C.OGR_G_ImportFromWkb(geom, cWkb, C.int(len(feat.WKB)))
				C.free(cWkb)

				if result == C.OGRERR_NONE {
					C.OGR_F_SetGeometry(feature, geom)
				}
				C.OGR_G_DestroyGeometry(geom)
			}
		}

		// 设置属性
		for attrName, attrValue := range feat.Attributes {
			fieldIndex := C.OGR_F_GetFieldIndex(feature, C.CString(attrName))
			if fieldIndex >= 0 {
				cValue := C.CString(attrValue)
				C.OGR_F_SetFieldString(feature, fieldIndex, cValue)
				C.free(unsafe.Pointer(cValue))
			}
		}

		C.OGR_L_CreateFeature(layer, feature)
		C.OGR_F_Destroy(feature)
	}

	return &GDALLayer{
		layer:   layer,
		dataset: ds,
		driver:  driver,
	}, nil
}

// VectorFeature 矢量要素
type VectorFeature struct {
	WKB        []byte            // WKB几何数据
	Attributes map[string]string // 属性字段
}

// RasterizeGDALLayer 栅格化矢量图层为PNG
func (gen *VectorTileGenerator) RasterizeVectorLayer(GDALLayer *GDALLayer, bounds VectorTileBounds) ([]byte, error) {
	tileSize := gen.config.TileSize

	// 1. 创建内存栅格数据集
	memDriver := C.GDALGetDriverByName(C.CString("MEM"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取MEM驱动")
	}

	// 计算分辨率
	pixelWidth := (bounds.MaxLon - bounds.MinLon) / float64(tileSize)
	pixelHeight := (bounds.MaxLat - bounds.MinLat) / float64(tileSize)

	// 创建4波段RGBA栅格
	rasterDS := C.GDALCreate(memDriver, C.CString(""), C.int(tileSize), C.int(tileSize), 4, C.GDT_Byte, nil)
	if rasterDS == nil {
		return nil, fmt.Errorf("创建栅格数据集失败")
	}
	defer C.GDALClose(rasterDS)

	// 设置地理变换参数
	geoTransform := []C.double{
		C.double(bounds.MinLon),
		C.double(pixelWidth),
		0,
		C.double(bounds.MaxLat),
		0,
		C.double(-pixelHeight),
	}
	C.GDALSetGeoTransform(rasterDS, (*C.double)(unsafe.Pointer(&geoTransform[0])))

	// 设置投影
	srs := C.OSRNewSpatialReference(nil)
	C.OSRImportFromEPSG(srs, 4326)
	var wkt *C.char
	C.OSRExportToWkt(srs, &wkt)
	C.GDALSetProjection(rasterDS, wkt)
	C.OSRDestroySpatialReference(srs)
	C.CPLFree(unsafe.Pointer(wkt))

	// 2. 根据颜色配置进行栅格化
	if err := gen.rasterizeWithColors(rasterDS, GDALLayer); err != nil {
		return nil, err
	}

	// 3. 读取栅格数据并转换为PNG
	return gen.rasterToPNG(rasterDS, tileSize)
}

// rasterizeWithColors 根据颜色配置栅格化
func (gen *VectorTileGenerator) rasterizeWithColors(rasterDS C.GDALDatasetH, GDALLayer *GDALLayer) error {
	if len(gen.config.ColorMap) == 0 {
		// 默认灰色
		return gen.rasterizeSingleColor(rasterDS, GDALLayer, RGBA{128, 128, 128, int(gen.config.Opacity * 255)})
	}

	rule := gen.config.ColorMap[0]

	// 检查是否为默认单一颜色
	if rule.AttributeName == "默认" && rule.AttributeValue == "默认" {
		rgb := ParseColor(rule.Color)
		rgb.A = int(gen.config.Opacity * 255)
		return gen.rasterizeSingleColor(rasterDS, GDALLayer, rgb)
	}

	// 按属性值分组栅格化
	if len(rule.ColorValues) > 0 {
		return gen.rasterizeByAttribute(rasterDS, GDALLayer, rule.AttributeName, rule.ColorValues)
	}

	// 单一颜色
	rgb := ParseColor(rule.Color)
	rgb.A = int(gen.config.Opacity * 255)
	return gen.rasterizeSingleColor(rasterDS, GDALLayer, rgb)
}

// rasterizeSingleColor 单一颜色栅格化
func (gen *VectorTileGenerator) rasterizeSingleColor(rasterDS C.GDALDatasetH, GDALLayer *GDALLayer, color RGBA) error {
	burnValues := []C.double{C.double(color.R), C.double(color.G), C.double(color.B), C.double(color.A)}
	bands := []C.int{1, 2, 3, 4}

	options := C.CSLSetNameValue(nil, C.CString("ALL_TOUCHED"), C.CString("TRUE"))
	defer C.CSLDestroy(options)

	err := C.GDALRasterizeLayers(
		rasterDS,
		4,
		(*C.int)(unsafe.Pointer(&bands[0])),
		1,
		&GDALLayer.layer,
		nil,
		nil,
		(*C.double)(unsafe.Pointer(&burnValues[0])),
		options,
		nil,
		nil,
	)

	if err != 0 {
		return fmt.Errorf("栅格化失败，错误码: %d", int(err))
	}

	return nil
}

// rasterizeByAttribute 按属性值栅格化
func (gen *VectorTileGenerator) rasterizeByAttribute(rasterDS C.GDALDatasetH, GDALLayer *GDALLayer, attName string, colorValues map[string]string) error {
	for attrValue, colorStr := range colorValues {
		// 设置属性过滤器
		whereClause := fmt.Sprintf("%s = '%s'", attName, strings.ReplaceAll(attrValue, "'", "''"))
		cWhereClause := C.CString(whereClause)
		C.OGR_L_SetAttributeFilter(GDALLayer.layer, cWhereClause)
		C.free(unsafe.Pointer(cWhereClause))

		// 解析颜色
		rgb := ParseColor(colorStr)
		rgb.A = int(gen.config.Opacity * 255)

		// 栅格化当前过滤的要素
		burnValues := []C.double{C.double(rgb.R), C.double(rgb.G), C.double(rgb.B), C.double(rgb.A)}
		bands := []C.int{1, 2, 3, 4}

		options := C.CSLSetNameValue(nil, C.CString("ALL_TOUCHED"), C.CString("TRUE"))

		C.GDALRasterizeLayers(
			rasterDS,
			4,
			(*C.int)(unsafe.Pointer(&bands[0])),
			1,
			&GDALLayer.layer,
			nil,
			nil,
			(*C.double)(unsafe.Pointer(&burnValues[0])),
			options,
			nil,
			nil,
		)

		C.CSLDestroy(options)
	}

	// 清除过滤器
	C.OGR_L_SetAttributeFilter(GDALLayer.layer, nil)

	return nil
}

// rasterToPNG 将栅格转换为PNG
func (gen *VectorTileGenerator) rasterToPNG(rasterDS C.GDALDatasetH, tileSize int) ([]byte, error) {
	img := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))

	for band := 1; band <= 4; band++ {
		rasterBand := C.GDALGetRasterBand(rasterDS, C.int(band))
		if rasterBand == nil {
			return nil, fmt.Errorf("获取波段%d失败", band)
		}

		buffer := make([]byte, tileSize*tileSize)
		err := C.GDALRasterIO(
			rasterBand,
			C.GF_Read,
			0, 0,
			C.int(tileSize), C.int(tileSize),
			unsafe.Pointer(&buffer[0]),
			C.int(tileSize), C.int(tileSize),
			C.GDT_Byte,
			0, 0,
		)

		if err != 0 {
			return nil, fmt.Errorf("读取波段%d失败", band)
		}

		// 填充到图像
		for i := 0; i < tileSize*tileSize; i++ {
			img.Pix[i*4+band-1] = buffer[i]
		}
	}

	// 编码为PNG
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, fmt.Errorf("PNG编码失败: %v", err)
	}

	return buf.Bytes(), nil
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
