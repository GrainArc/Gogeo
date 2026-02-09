// RasterReader.go
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
	"runtime"
	"unsafe"
)

// RasterDataset 栅格数据集
type RasterDataset struct {
	dataset       C.GDALDatasetH
	warpedDS      C.GDALDatasetH
	filePath      string
	width         int
	height        int
	bandCount     int
	bounds        [4]float64 // minX, minY, maxX, maxY (Web Mercator)
	projection    string
	isReprojected bool // 标记是否已重投影
	hasGeoInfo    bool // 标记是否有地理信息
}

// DatasetInfo 数据集信息
type DatasetInfo struct {
	Width        int
	Height       int
	BandCount    int
	GeoTransform [6]float64
	Projection   string
	HasGeoInfo   bool
}

func (rd *RasterDataset) GetActiveDataset() C.GDALDatasetH {
	if rd == nil {
		return nil
	}
	// 优先返回 warpedDS（内存副本）
	if rd.warpedDS != nil {
		return rd.warpedDS
	}
	return rd.dataset
}

// imagePath: 影像文件路径
func OpenRasterDataset(imagePath string, reProj bool) (*RasterDataset, error) {
	cPath := C.CString(imagePath)
	defer C.free(unsafe.Pointer(cPath))

	// 打开数据集
	dataset := C.GDALOpen(cPath, C.GA_ReadOnly)
	if dataset == nil {
		return nil, fmt.Errorf("failed to open image: %s", imagePath)
	}

	var warpedDS C.GDALDatasetH
	var activeDS C.GDALDatasetH // 实际使用的数据集

	// 获取基本信息
	width := int(C.GDALGetRasterXSize(dataset))
	height := int(C.GDALGetRasterYSize(dataset))
	bandCount := int(C.GDALGetRasterCount(dataset))

	// 检查是否有地理信息
	var geoTransform [6]C.double
	hasGeoInfo := C.GDALGetGeoTransform(dataset, &geoTransform[0]) == C.CE_None

	// 获取投影信息
	projection := C.GoString(C.GDALGetProjectionRef(dataset))

	// 如果没有地理信息，检查是否有投影信息
	if !hasGeoInfo && projection == "" {
		hasGeoInfo = false
	} else if !hasGeoInfo && projection != "" {
		// 有投影但没有地理变换，仍然认为没有完整的地理信息
		hasGeoInfo = false
	}

	// 根据参数和地理信息决定是否重投影
	if reProj && hasGeoInfo {
		// 重投影到Web墨卡托
		warpedDS = C.reprojectToWebMercator(dataset)
		if warpedDS == nil {
			C.GDALClose(dataset)
			return nil, fmt.Errorf("failed to reproject image to Web Mercator")
		}
		activeDS = warpedDS

		// 重新获取重投影后的地理变换
		if C.GDALGetGeoTransform(activeDS, &geoTransform[0]) != C.CE_None {
			C.GDALClose(warpedDS)
			C.GDALClose(dataset)
			return nil, fmt.Errorf("failed to get geotransform from reprojected dataset")
		}
	} else {
		// 不重投影，直接使用原始数据集
		activeDS = dataset
		warpedDS = nil

		if !hasGeoInfo {
			// 像素坐标系：左上角(0,0)，Y轴向下
			geoTransform[0] = 0.0              // 左上角X = 0
			geoTransform[1] = 1.0              // 像素宽度 = 1
			geoTransform[2] = 0.0              // 无旋转
			geoTransform[3] = C.double(height) // 左上角Y = height
			geoTransform[4] = 0.0              // 无旋转
			geoTransform[5] = -1.0             // 像素高度 = -1（Y轴向下）
		}

	}

	// 计算边界
	minX := float64(geoTransform[0])
	maxY := float64(geoTransform[3])
	maxX := minX + float64(width)*float64(geoTransform[1])
	minY := maxY + float64(height)*float64(geoTransform[5])

	// 如果没有地理信息，更新投影信息
	if !hasGeoInfo {
		projection = "PIXEL" // 标记为像素坐标系
	} else if reProj {
		// 获取重投影后的投影信息
		projection = C.GoString(C.GDALGetProjectionRef(activeDS))
	}

	rd := &RasterDataset{
		dataset:       dataset,
		warpedDS:      warpedDS,
		width:         width,
		height:        height,
		filePath:      imagePath,
		bandCount:     bandCount,
		bounds:        [4]float64{minX, minY, maxX, maxY},
		projection:    projection,
		isReprojected: reProj && hasGeoInfo,
		hasGeoInfo:    hasGeoInfo,
	}

	runtime.SetFinalizer(rd, (*RasterDataset).Close)

	return rd, nil
}

// GetEPSGCode 获取栅格数据的EPSG代码
// 如果无法获取或没有投影信息，返回0
func (rd *RasterDataset) GetEPSGCode() int {
	if rd == nil {
		return 0
	}

	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return 0
	}

	// 如果没有地理信息，返回0
	if !rd.hasGeoInfo {
		return 0
	}

	// 获取投影信息
	projRef := C.GDALGetProjectionRef(activeDS)
	if projRef == nil {
		return 0
	}

	wkt := C.GoString(projRef)
	if wkt == "" || wkt == "PIXEL" {
		return 0
	}

	// 创建空间参考对象
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		return 0
	}
	defer C.OSRDestroySpatialReference(srs)

	// 从WKT导入
	cWKT := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWKT))

	if C.OSRImportFromWkt(srs, &cWKT) != C.OGRERR_NONE {
		return 0
	}

	// 尝试获取EPSG代码
	// 首先尝试从AUTHORITY获取
	authName := C.OSRGetAuthorityName(srs, nil)
	if authName != nil && C.GoString(authName) == "EPSG" {
		authCode := C.OSRGetAuthorityCode(srs, nil)
		if authCode != nil {
			var epsg int
			if _, err := fmt.Sscanf(C.GoString(authCode), "%d", &epsg); err == nil {
				return epsg
			}
		}
	}

	// 尝试自动识别EPSG
	if C.OSRAutoIdentifyEPSG(srs) == C.OGRERR_NONE {
		authCode := C.OSRGetAuthorityCode(srs, nil)
		if authCode != nil {
			var epsg int
			if _, err := fmt.Sscanf(C.GoString(authCode), "%d", &epsg); err == nil {
				return epsg
			}
		}
	}

	return 0
}

// Close 关闭数据集
func (rd *RasterDataset) Close() {
	if rd.warpedDS != nil {
		C.GDALClose(rd.warpedDS)
		rd.warpedDS = nil
	}
	if rd.dataset != nil {
		C.GDALClose(rd.dataset)
		rd.dataset = nil
	}
}
func (rd *RasterDataset) ensureMemoryCopy() error {
	// 已经有内存副本
	if rd.warpedDS != nil {
		return nil
	}

	if rd.dataset == nil {
		return fmt.Errorf("dataset is nil")
	}

	// 检查是否已经是MEM数据集
	driver := C.GDALGetDatasetDriver(rd.dataset)
	if driver != nil {
		driverName := C.GoString(C.GDALGetDriverShortName(driver))
		if driverName == "MEM" {
			return nil
		}
	}

	// 创建内存副本
	memDS := C.ensureMemoryDataset(rd.dataset)
	if memDS == nil {
		return fmt.Errorf("failed to create memory copy")
	}

	rd.warpedDS = memDS
	return nil
}

// GetInfo 获取数据集信息
func (rd *RasterDataset) GetInfo() DatasetInfo {
	var cInfo C.DatasetInfo
	C.getDatasetInfo(rd.warpedDS, &cInfo)

	info := DatasetInfo{
		Width:     int(cInfo.width),
		Height:    int(cInfo.height),
		BandCount: int(cInfo.bandCount),
	}

	for i := 0; i < 6; i++ {
		info.GeoTransform[i] = float64(cInfo.geoTransform[i])
	}

	info.Projection = C.GoString(&cInfo.projection[0])

	return info
}

// GetBounds 获取边界（Web墨卡托坐标）
func (rd *RasterDataset) GetBounds() (minX, minY, maxX, maxY float64) {
	return rd.bounds[0], rd.bounds[1], rd.bounds[2], rd.bounds[3]
}

// GetBoundsLatLon 获取边界（经纬度）
func (rd *RasterDataset) GetBoundsLatLon() (minLon, minLat, maxLon, maxLat float64) {
	minX, minY, maxX, maxY := rd.GetBounds()

	minLon, minLat = WebMercatorToLatLon(minX, minY)
	maxLon, maxLat = WebMercatorToLatLon(maxX, maxY)

	return
}

// GetTileRange 获取指定缩放级别的瓦片范围（符合Mapbox规范）
func (rd *RasterDataset) GetTileRange(zoom int) (minTileX, minTileY, maxTileX, maxTileY int) {
	minX, minY, maxX, maxY := rd.GetBounds()

	const (
		EarthRadius = 6378137.0
		OriginShift = math.Pi * EarthRadius // 20037508.342789244
	)

	// 🔥 修正：计算该缩放级别的瓦片总数
	numTiles := math.Exp2(float64(zoom))

	// 🔥 修正：计算单个瓦片的世界尺寸（米）
	tileWorldSize := (2 * OriginShift) / numTiles

	// 计算瓦片行列号（XYZ方案）
	minTileX = int(math.Floor((minX + OriginShift) / tileWorldSize))
	maxTileX = int(math.Floor((maxX + OriginShift) / tileWorldSize))

	// Y坐标：XYZ方案，Y轴向下
	minTileY = int(math.Floor((OriginShift - maxY) / tileWorldSize))
	maxTileY = int(math.Floor((OriginShift - minY) / tileWorldSize))

	// 边界检查
	maxTiles := int(numTiles) - 1
	if minTileX < 0 {
		minTileX = 0
	}
	if minTileY < 0 {
		minTileY = 0
	}
	if maxTileX > maxTiles {
		maxTileX = maxTiles
	}
	if maxTileY > maxTiles {
		maxTileY = maxTiles
	}

	return
}

// ReadTile 读取瓦片数据（黑色背景转透明）
func (rd *RasterDataset) ReadTile(zoom, x, y, tileSize int) ([]byte, error) {
	var minX, minY, maxX, maxY C.double

	C.getTileBounds(C.int(x), C.int(y), C.int(zoom), &minX, &minY, &maxX, &maxY)

	// 分配缓冲区（最多4个波段）
	bufferSize := tileSize * tileSize * 4
	buffer := make([]byte, bufferSize)

	bands := int(C.readTileData(
		rd.warpedDS,
		minX, minY, maxX, maxY,
		C.int(tileSize),
		(*C.uchar)(unsafe.Pointer(&buffer[0])),
	))

	if bands == 0 {
		return nil, fmt.Errorf("failed to read tile data")
	}

	// 创建 RGBA 图像（始终包含 Alpha 通道）
	rgbaImg := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))

	if bands == 3 {
		// RGB -> RGBA（黑色转透明）
		for i := 0; i < tileSize*tileSize; i++ {
			r := buffer[i]
			g := buffer[i+tileSize*tileSize]
			b := buffer[i+2*tileSize*tileSize]

			rgbaImg.Pix[i*4] = r
			rgbaImg.Pix[i*4+1] = g
			rgbaImg.Pix[i*4+2] = b

			// 黑色背景转透明（可以设置阈值，比如 r+g+b < 10）
			if r == 0 && g == 0 && b == 0 {
				rgbaImg.Pix[i*4+3] = 0 // 完全透明
			} else {
				rgbaImg.Pix[i*4+3] = 255 // 完全不透明
			}
		}
	} else if bands == 4 {
		// RGBA（直接使用）
		for i := 0; i < tileSize*tileSize; i++ {
			r := buffer[i]
			g := buffer[i+tileSize*tileSize]
			b := buffer[i+2*tileSize*tileSize]
			a := buffer[i+3*tileSize*tileSize]

			rgbaImg.Pix[i*4] = r
			rgbaImg.Pix[i*4+1] = g
			rgbaImg.Pix[i*4+2] = b

			// 如果是黑色，强制设为透明
			if r == 0 && g == 0 && b == 0 {
				rgbaImg.Pix[i*4+3] = 0
			} else {
				rgbaImg.Pix[i*4+3] = a
			}
		}
	} else {
		return nil, fmt.Errorf("unsupported band count: %d", bands)
	}

	// 编码为 PNG（PNG 支持透明度）
	var buf bytes.Buffer
	if err := png.Encode(&buf, rgbaImg); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// LatLonToWebMercator 经纬度转Web墨卡托（符合Mapbox规范）
func LatLonToWebMercator(lon, lat float64) (x, y float64) {
	const (
		EarthRadius = 6378137.0
		OriginShift = math.Pi * EarthRadius
	)

	x = lon * OriginShift / 180.0
	y = math.Log(math.Tan((90+lat)*math.Pi/360.0)) * OriginShift / math.Pi
	return
}

// WebMercatorToLatLon Web墨卡托转经纬度（符合Mapbox规范）
func WebMercatorToLatLon(x, y float64) (lon, lat float64) {
	const (
		EarthRadius = 6378137.0
		OriginShift = math.Pi * EarthRadius
	)

	lon = x * 180.0 / OriginShift
	lat = math.Atan(math.Exp(y*math.Pi/OriginShift))*360.0/math.Pi - 90.0
	return
}

// LonLatToTile 经纬度转瓦片坐标（符合Mapbox规范）
func LonLatToTile(lon, lat float64, zoom int) (x, y int) {
	const (
		EarthRadius = 6378137.0
		OriginShift = math.Pi * EarthRadius
	)

	// 转换为Web墨卡托
	mercX := lon * OriginShift / 180.0
	mercY := math.Log(math.Tan((90+lat)*math.Pi/360.0)) * OriginShift / math.Pi

	// **关键修复：使用整数位运算**
	numTiles := int64(1 << uint(zoom))
	tileSize := (2.0 * OriginShift) / float64(numTiles)

	x = int(math.Floor((mercX + OriginShift) / tileSize))
	y = int(math.Floor((OriginShift - mercY) / tileSize))

	// 边界检查
	maxTile := int(numTiles) - 1
	if x < 0 {
		x = 0
	} else if x > maxTile {
		x = maxTile
	}
	if y < 0 {
		y = 0
	} else if y > maxTile {
		y = maxTile
	}

	return
}

// TileToWebMercatorBounds 瓦片坐标转Web墨卡托边界
func TileToWebMercatorBounds(x, y, zoom int) (minX, minY, maxX, maxY float64) {
	const (
		EarthRadius = 6378137.0
		OriginShift = math.Pi * EarthRadius
	)

	// **关键修复：使用整数位运算**
	numTiles := int64(1 << uint(zoom))
	tileSize := (2.0 * OriginShift) / float64(numTiles)

	minX = float64(x)*tileSize - OriginShift
	maxX = float64(x+1)*tileSize - OriginShift
	maxY = OriginShift - float64(y)*tileSize
	minY = OriginShift - float64(y+1)*tileSize

	return
}

// ReadTileRaw 读取瓦片原始高程数据（返回float32数组，用于地形处理）
func (rd *RasterDataset) ReadTileRaw(zoom, x, y, tileSize int) ([]float32, error) {
	var minX, minY, maxX, maxY C.double

	C.getTileBounds(C.int(x), C.int(y), C.int(zoom), &minX, &minY, &maxX, &maxY)

	// 分配float32缓冲区（单波段高程数据）
	bufferSize := tileSize * tileSize
	buffer := make([]float32, bufferSize)

	// 调用C函数读取float32数据
	result := C.readTileDataFloat32(
		rd.warpedDS,
		minX, minY, maxX, maxY,
		C.int(tileSize),
		(*C.float)(unsafe.Pointer(&buffer[0])),
	)

	if result == 0 {
		return nil, fmt.Errorf("failed to read tile raw data")
	}

	return buffer, nil
}

// ReadTileRawWithNoData 读取瓦片原始高程数据，同时返回NoData值
func (rd *RasterDataset) ReadTileRawWithNoData(zoom, x, y, tileSize int) ([]float32, float32, error) {
	var minX, minY, maxX, maxY C.double

	C.getTileBounds(C.int(x), C.int(y), C.int(zoom), &minX, &minY, &maxX, &maxY)

	// 分配float32缓冲区
	bufferSize := tileSize * tileSize
	buffer := make([]float32, bufferSize)

	// 获取NoData值
	var noDataValue C.double
	var hasNoData C.int
	band := C.GDALGetRasterBand(rd.getActiveDataset(), 1)
	noDataValue = C.GDALGetRasterNoDataValue(band, &hasNoData)

	noData := float32(-9999) // 默认NoData值
	if hasNoData != 0 {
		noData = float32(noDataValue)
	}

	// 读取数据
	result := C.readTileDataFloat32(
		rd.getActiveDataset(),
		minX, minY, maxX, maxY,
		C.int(tileSize),
		(*C.float)(unsafe.Pointer(&buffer[0])),
	)

	if result == 0 {
		return nil, noData, fmt.Errorf("failed to read tile raw data")
	}

	return buffer, noData, nil
}

// getActiveDataset 获取当前活动的数据集
func (rd *RasterDataset) getActiveDataset() C.GDALDatasetH {
	if rd.warpedDS != nil {
		return rd.warpedDS
	}
	return rd.dataset
}

func (rd *RasterDataset) GetWidth() int {
	return rd.width
}

// GetHeight 获取数据集高度（像素）
func (rd *RasterDataset) GetHeight() int {
	return rd.height
}
func (rd *RasterDataset) GetProjection() string {
	return rd.projection
}
func (rd *RasterDataset) ExportToFile(outputPath, format string, options map[string]string) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	driver := C.GDALGetDriverByName(cFormat)
	if driver == nil {
		return fmt.Errorf("unsupported format: %s", format)
	}

	// 构建选项
	var cOptions **C.char
	var optionPtrs []*C.char
	if len(options) > 0 {
		optionPtrs = make([]*C.char, 0, len(options)+1)
		for k, v := range options {
			optStr := C.CString(fmt.Sprintf("%s=%s", k, v))
			optionPtrs = append(optionPtrs, optStr)
		}
		optionPtrs = append(optionPtrs, nil)
		cOptions = &optionPtrs[0]
	}

	defer func() {
		for _, ptr := range optionPtrs {
			if ptr != nil {
				C.free(unsafe.Pointer(ptr))
			}
		}
	}()

	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	// 创建输出文件
	outputDS := C.GDALCreateCopy(driver, cOutputPath, activeDS, C.int(0), cOptions, nil, nil)
	if outputDS == nil {
		return fmt.Errorf("failed to create output: %s", C.GoString(C.CPLGetLastErrorMsg()))
	}

	// 关键：同步元数据修改到输出数据集
	bandCount := int(C.GDALGetRasterCount(activeDS))
	for i := 1; i <= bandCount; i++ {
		srcBand := C.GDALGetRasterBand(activeDS, C.int(i))
		dstBand := C.GDALGetRasterBand(outputDS, C.int(i))

		if srcBand == nil || dstBand == nil {
			continue
		}

		// 同步颜色解释
		colorInterp := C.GDALGetRasterColorInterpretation(srcBand)
		C.GDALSetRasterColorInterpretation(dstBand, colorInterp)

		// 同步 NoData
		var hasNoData C.int
		noData := C.GDALGetRasterNoDataValue(srcBand, &hasNoData)
		if hasNoData != 0 {
			C.GDALSetRasterNoDataValue(dstBand, noData)
		}

		// 同步调色板
		colorTable := C.GDALGetRasterColorTable(srcBand)
		if colorTable != nil {
			C.GDALSetRasterColorTable(dstBand, colorTable)
		}
	}

	C.GDALFlushCache(outputDS)
	C.GDALClose(outputDS)

	return nil
}

// ==================== 投影定义与重投影 ====================

// AffineParams 仿射变换参数
type AffineParams struct {
	// 七参数
	Tx    float64 // X平移（米）
	Ty    float64 // Y平移（米）
	Tz    float64 // Z平移（米）
	Rx    float64 // X旋转（度）
	Ry    float64 // Y旋转（度）
	Rz    float64 // Z旋转（度）
	Scale float64 // 缩放因子

	// 四参数
	Dx     float64 // X平移（米）
	Dy     float64 // Y平移（米）
	DScale float64 // 缩放因子
	Angle  float64 // 旋转角度（度）
}

// DefineProjection 直接为栅格数据定义投影（修改原数据）
// epsgCode: EPSG代码
// 注意：此操作会直接修改原数据文件
func (rd *RasterDataset) DefineProjection(epsgCode int) error {
	if rd.dataset == nil {
		return fmt.Errorf("dataset is nil")
	}

	var errorMsg [256]C.char
	result := C.defineProjectionInPlace(rd.dataset, C.int(epsgCode), &errorMsg[0])

	if result == 0 {
		return fmt.Errorf("failed to define projection: %s", C.GoString(&errorMsg[0]))
	}

	// 更新内部投影信息
	rd.projection = C.GoString(C.GDALGetProjectionRef(rd.dataset))
	rd.hasGeoInfo = true

	return nil
}

// DefineProjectionWithGeoTransform 直接为栅格数据定义投影和地理变换
// epsgCode: EPSG代码
// geoTransform: 地理变换参数 [originX, pixelWidth, rotationX, originY, rotationY, pixelHeight]
// 注意：此操作会直接修改原数据文件
func (rd *RasterDataset) DefineProjectionWithGeoTransform(epsgCode int, geoTransform [6]float64) error {
	if rd.dataset == nil {
		return fmt.Errorf("dataset is nil")
	}

	cGeoTransform := [6]C.double{
		C.double(geoTransform[0]),
		C.double(geoTransform[1]),
		C.double(geoTransform[2]),
		C.double(geoTransform[3]),
		C.double(geoTransform[4]),
		C.double(geoTransform[5]),
	}

	var errorMsg [256]C.char
	result := C.defineProjectionWithGeoTransformInPlace(
		rd.dataset,
		C.int(epsgCode),
		&cGeoTransform[0],
		&errorMsg[0],
	)

	if result == 0 {
		return fmt.Errorf("failed to define projection with geotransform: %s", C.GoString(&errorMsg[0]))
	}

	// 更新内部信息
	rd.projection = C.GoString(C.GDALGetProjectionRef(rd.dataset))
	rd.hasGeoInfo = true

	// 更新边界信息
	width := rd.GetWidth()
	height := rd.GetHeight()
	minX := geoTransform[0]
	maxY := geoTransform[3]
	maxX := minX + geoTransform[1]*float64(width)
	minY := maxY + geoTransform[5]*float64(height)

	rd.bounds = [4]float64{minX, minY, maxX, maxY}

	return nil
}

// DefineProjectionWithWKT 直接为栅格数据定义投影（使用自定义WKT）
// wkt: WKT投影定义
// 注意：此操作会直接修改原数据文件
func (rd *RasterDataset) DefineProjectionWithWKT(wkt string) error {
	if rd.dataset == nil {
		return fmt.Errorf("dataset is nil")
	}

	if wkt == "" {
		return fmt.Errorf("WKT is empty")
	}

	cWKT := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWKT))

	var errorMsg [256]C.char
	result := C.defineProjectionWithWKTInPlace(rd.dataset, cWKT, &errorMsg[0])

	if result == 0 {
		return fmt.Errorf("failed to define projection with WKT: %s", C.GoString(&errorMsg[0]))
	}

	// 更新内部投影信息
	rd.projection = C.GoString(C.GDALGetProjectionRef(rd.dataset))
	rd.hasGeoInfo = true

	return nil
}

// DefineProjectionToMemory 为栅格数据定义投影（创建内存副本）
// 用于没有坐标系的栅格数据，定义其投影后返回内存副本
// 返回的数据集支持ExportToFile导出
func (rd *RasterDataset) DefineProjectionToMemory(epsgCode int) (*RasterDataset, error) {
	if rd.dataset == nil {
		return nil, fmt.Errorf("source dataset is nil")
	}

	var errorMsg [256]C.char
	memDS := C.defineProjectionToMemory(rd.dataset, C.int(epsgCode), &errorMsg[0])

	if memDS == nil {
		return nil, fmt.Errorf("failed to define projection: %s", C.GoString(&errorMsg[0]))
	}

	// 获取投影后的基本信息
	width := int(C.GDALGetRasterXSize(memDS))
	height := int(C.GDALGetRasterYSize(memDS))
	bandCount := int(C.GDALGetRasterCount(memDS))

	// 获取地理变换
	var geoTransform [6]C.double
	C.GDALGetGeoTransform(memDS, &geoTransform[0])

	// 获取投影信息
	projection := C.GoString(C.GDALGetProjectionRef(memDS))

	// 计算边界
	minX := float64(geoTransform[0])
	maxY := float64(geoTransform[3])
	maxX := minX + float64(width)*float64(geoTransform[1])
	minY := maxY + float64(height)*float64(geoTransform[5])

	newRD := &RasterDataset{
		dataset:       rd.dataset,
		warpedDS:      memDS,
		width:         width,
		height:        height,
		bandCount:     bandCount,
		bounds:        [4]float64{minX, minY, maxX, maxY},
		projection:    projection,
		isReprojected: false,
		hasGeoInfo:    true,
	}

	runtime.SetFinalizer(newRD, (*RasterDataset).Close)
	return newRD, nil
}

// ReprojectToEPSG 将栅格数据重投影到指定的EPSG坐标系
// srcEPSG: 源EPSG代码
// dstEPSG: 目标EPSG代码
// outputPath: 输出文件路径
// format: 输出格式（如"GTiff"、"JP2OpenJPEG"等）
// resampleMethod: 重采样方法
func (rd *RasterDataset) ReprojectToEPSG(srcEPSG, dstEPSG int, outputPath, format string, resampleMethod ResampleMethod) error {
	if rd.dataset == nil {
		return fmt.Errorf("source dataset is nil")
	}

	if outputPath == "" {
		return fmt.Errorf("output path is empty")
	}

	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	var errorMsg [256]C.char
	result := C.reprojectRasterDataset(
		rd.getActiveDataset(),
		C.int(srcEPSG),
		C.int(dstEPSG),
		nil, // 不使用自定义WKT
		cOutputPath,
		cFormat,
		C.int(resampleMethod),
		&errorMsg[0],
	)

	if result == 0 {
		return fmt.Errorf("reproject failed: %s", C.GoString(&errorMsg[0]))
	}

	return nil
}

// ReprojectWithCustomWKT 使用自定义WKT投影定义进行重投影
// srcEPSG: 源EPSG代码
// customWKT: 自定义WKT投影定义
// outputPath: 输出文件路径
// format: 输出格式
// resampleMethod: 重采样方法
func (rd *RasterDataset) ReprojectWithCustomWKT(srcEPSG int, customWKT, outputPath, format string, resampleMethod ResampleMethod) error {
	if rd.dataset == nil {
		return fmt.Errorf("source dataset is nil")
	}

	if outputPath == "" {
		return fmt.Errorf("output path is empty")
	}

	if customWKT == "" {
		return fmt.Errorf("custom WKT is empty")
	}

	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	cWKT := C.CString(customWKT)
	defer C.free(unsafe.Pointer(cWKT))

	var errorMsg [256]C.char
	result := C.reprojectRasterDataset(
		rd.getActiveDataset(),
		C.int(srcEPSG),
		-1, // 使用自定义WKT
		cWKT,
		cOutputPath,
		cFormat,
		C.int(resampleMethod),
		&errorMsg[0],
	)

	if result == 0 {
		return fmt.Errorf("reproject with custom WKT failed: %s", C.GoString(&errorMsg[0]))
	}

	return nil
}

// ReprojectWithAffineParams 使用仿射参数进行重投影
// mode: "geotransform" - 只修改地理变换（快速，不改变像素）
//
//	"resample" - 重采样（慢，像素会旋转/缩放）
func (rd *RasterDataset) ReprojectWithAffineParams(srcEPSG int, params *AffineParams,
	paramType, outputPath, format string, resampleMethod ResampleMethod, mode string) error {

	if rd.dataset == nil {
		return fmt.Errorf("source dataset is nil")
	}

	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	var paramArray []C.double
	var paramCount int

	if paramType == "7param" {
		paramArray = []C.double{
			C.double(params.Tx), C.double(params.Ty), C.double(params.Tz),
			C.double(params.Rx), C.double(params.Ry), C.double(params.Rz),
			C.double(params.Scale),
		}
		paramCount = 7
	} else {
		paramArray = []C.double{
			C.double(params.Dx), C.double(params.Dy),
			C.double(params.DScale), C.double(params.Angle),
		}
		paramCount = 4
	}

	var errorMsg [256]C.char
	var result C.int

	if mode == "resample" {
		result = C.reprojectRasterWithAffineParamsResample(
			rd.getActiveDataset(), C.int(srcEPSG),
			(*C.double)(&paramArray[0]), C.int(paramCount),
			cOutputPath, cFormat, C.int(resampleMethod), &errorMsg[0],
		)
	} else {
		result = C.reprojectRasterWithAffineParams(
			rd.getActiveDataset(), C.int(srcEPSG),
			(*C.double)(&paramArray[0]), C.int(paramCount),
			cOutputPath, cFormat, C.int(resampleMethod), &errorMsg[0],
		)
	}

	if result == 0 {
		return fmt.Errorf("reproject failed: %s", C.GoString(&errorMsg[0]))
	}
	return nil
}

// GetProjectionWKT 获取EPSG代码对应的WKT投影定义
func GetProjectionWKT(epsgCode int) (string, error) {
	wktBuffer := make([]C.char, 4096)
	result := C.getProjectionWKTFromEPSG(C.int(epsgCode), &wktBuffer[0], 4096)

	if result == 0 {
		return "", fmt.Errorf("failed to get WKT for EPSG code: %d", epsgCode)
	}

	return C.GoString(&wktBuffer[0]), nil
}

// ValidateProjectionWKT 验证WKT投影定义是否有效
func ValidateProjectionWKT(wkt string) bool {
	cWKT := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWKT))

	result := C.validateProjectionWKT(cWKT)
	return result != 0
}
