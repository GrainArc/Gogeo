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
	width         int
	height        int
	bandCount     int
	bounds        [4]float64 // minX, minY, maxX, maxY (Web Mercator)
	projection    string
	isReprojected bool // 标记是否已重投影
}

// DatasetInfo 数据集信息
type DatasetInfo struct {
	Width        int
	Height       int
	BandCount    int
	GeoTransform [6]float64
	Projection   string
}

// imagePath: 影像文件路径
// reProj: 是否重投影到Web墨卡托坐标系（EPSG:3857）
func OpenRasterDataset(imagePath string, reProj bool) (*RasterDataset, error) {
	cPath := C.CString(imagePath)
	defer C.free(unsafe.Pointer(cPath))
	InitializeGDAL()

	// 打开数据集
	dataset := C.GDALOpen(cPath, C.GA_ReadOnly)
	if dataset == nil {
		return nil, fmt.Errorf("failed to open image: %s", imagePath)
	}

	var warpedDS C.GDALDatasetH
	var activeDS C.GDALDatasetH // 实际使用的数据集

	// 根据参数决定是否重投影
	if reProj {
		// 重投影到Web墨卡托
		warpedDS = C.reprojectToWebMercator(dataset)
		if warpedDS == nil {
			C.GDALClose(dataset)
			return nil, fmt.Errorf("failed to reproject image to Web Mercator")
		}
		activeDS = warpedDS
	} else {
		// 不重投影，直接使用原始数据集
		activeDS = dataset
		warpedDS = nil
	}

	// 获取基本信息
	width := int(C.GDALGetRasterXSize(activeDS))
	height := int(C.GDALGetRasterYSize(activeDS))
	bandCount := int(C.GDALGetRasterCount(activeDS))

	// 计算边界
	var geoTransform [6]C.double
	if C.GDALGetGeoTransform(activeDS, &geoTransform[0]) != C.CE_None {
		if warpedDS != nil {
			C.GDALClose(warpedDS)
		}
		C.GDALClose(dataset)
		return nil, fmt.Errorf("failed to get geotransform")
	}

	minX := float64(geoTransform[0])
	maxY := float64(geoTransform[3])
	maxX := minX + float64(width)*float64(geoTransform[1])
	minY := maxY + float64(height)*float64(geoTransform[5])

	// 获取投影信息
	projection := C.GoString(C.GDALGetProjectionRef(activeDS))

	rd := &RasterDataset{
		dataset:       dataset,
		warpedDS:      warpedDS,
		width:         width,
		height:        height,
		bandCount:     bandCount,
		bounds:        [4]float64{minX, minY, maxX, maxY},
		projection:    projection,
		isReprojected: reProj,
	}

	runtime.SetFinalizer(rd, (*RasterDataset).Close)

	return rd, nil
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
