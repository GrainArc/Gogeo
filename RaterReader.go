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
	dataset   C.GDALDatasetH
	warpedDS  C.GDALDatasetH
	width     int
	height    int
	bandCount int
	bounds    [4]float64 // minX, minY, maxX, maxY (Web Mercator)
}

// DatasetInfo 数据集信息
type DatasetInfo struct {
	Width        int
	Height       int
	BandCount    int
	GeoTransform [6]float64
	Projection   string
}

// OpenRasterDataset 打开栅格数据集
func OpenRasterDataset(imagePath string) (*RasterDataset, error) {
	cPath := C.CString(imagePath)
	defer C.free(unsafe.Pointer(cPath))
	InitializeGDAL()
	// 打开数据集
	dataset := C.GDALOpen(cPath, C.GA_ReadOnly)
	if dataset == nil {
		return nil, fmt.Errorf("failed to open image: %s", imagePath)
	}

	// 重投影到Web墨卡托
	warpedDS := C.reprojectToWebMercator(dataset)
	if warpedDS == nil {
		C.GDALClose(dataset)
		return nil, fmt.Errorf("failed to reproject image to Web Mercator")
	}

	// 获取基本信息
	width := int(C.GDALGetRasterXSize(warpedDS))
	height := int(C.GDALGetRasterYSize(warpedDS))
	bandCount := int(C.GDALGetRasterCount(warpedDS))

	// 计算边界
	var geoTransform [6]C.double
	if C.GDALGetGeoTransform(warpedDS, &geoTransform[0]) != C.CE_None {
		C.GDALClose(warpedDS)
		C.GDALClose(dataset)
		return nil, fmt.Errorf("failed to get geotransform")
	}

	minX := float64(geoTransform[0])
	maxY := float64(geoTransform[3])
	maxX := minX + float64(width)*float64(geoTransform[1])
	minY := maxY + float64(height)*float64(geoTransform[5])

	rd := &RasterDataset{
		dataset:   dataset,
		warpedDS:  warpedDS,
		width:     width,
		height:    height,
		bandCount: bandCount,
		bounds:    [4]float64{minX, minY, maxX, maxY},
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

	minLon = minX * 180 / 20037508.34
	maxLon = maxX * 180 / 20037508.34
	minLat = math.Atan(math.Exp(minY*math.Pi/20037508.34))*360/math.Pi - 90
	maxLat = math.Atan(math.Exp(maxY*math.Pi/20037508.34))*360/math.Pi - 90

	return
}

// GetTileRange 获取指定缩放级别的瓦片范围
func (rd *RasterDataset) GetTileRange(zoom int) (minTileX, minTileY, maxTileX, maxTileY int) {
	minX, minY, maxX, maxY := rd.GetBounds()

	n := math.Pow(2, float64(zoom))

	minTileX = int((minX + 20037508.343) / 40075016.686 * n)
	maxTileX = int((maxX + 20037508.343) / 40075016.686 * n)
	minTileY = int((20037508.343 - maxY) / 40075016.686 * n)
	maxTileY = int((20037508.343 - minY) / 40075016.686 * n)

	// 边界检查
	if minTileX < 0 {
		minTileX = 0
	}
	if minTileY < 0 {
		minTileY = 0
	}
	maxTiles := int(n) - 1
	if maxTileX > maxTiles {
		maxTileX = maxTiles
	}
	if maxTileY > maxTiles {
		maxTileY = maxTiles
	}

	return
}

// ReadTile 读取瓦片数据
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

	// 创建图像
	var img image.Image

	if bands == 3 {
		// RGB
		rgbImg := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))
		for i := 0; i < tileSize*tileSize; i++ {
			rgbImg.Pix[i*4] = buffer[i]
			rgbImg.Pix[i*4+1] = buffer[i+tileSize*tileSize]
			rgbImg.Pix[i*4+2] = buffer[i+2*tileSize*tileSize]
			rgbImg.Pix[i*4+3] = 255
		}
		img = rgbImg
	} else if bands == 4 {
		// RGBA
		rgbaImg := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))
		for i := 0; i < tileSize*tileSize; i++ {
			rgbaImg.Pix[i*4] = buffer[i]
			rgbaImg.Pix[i*4+1] = buffer[i+tileSize*tileSize]
			rgbaImg.Pix[i*4+2] = buffer[i+2*tileSize*tileSize]
			rgbaImg.Pix[i*4+3] = buffer[i+3*tileSize*tileSize]
		}
		img = rgbaImg
	} else {
		return nil, fmt.Errorf("unsupported band count: %d", bands)
	}

	// 编码为PNG
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// LatLonToWebMercator 经纬度转Web墨卡托
func LatLonToWebMercator(lon, lat float64) (x, y float64) {
	x = lon * 20037508.34 / 180.0
	y = math.Log(math.Tan((90+lat)*math.Pi/360.0)) / (math.Pi / 180.0)
	y = y * 20037508.34 / 180.0
	return
}

// WebMercatorToLatLon Web墨卡托转经纬度
func WebMercatorToLatLon(x, y float64) (lon, lat float64) {
	lon = x * 180 / 20037508.34
	lat = math.Atan(math.Exp(y*math.Pi/20037508.34))*360/math.Pi - 90
	return
}
