package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"

import (
	"fmt"
	"math"
	"unsafe"
)

// TileOptions 切片选项
type TileOptions struct {
	TileWidth  int     // 切片宽度（像素）
	TileHeight int     // 切片高度（像素）
	OverlapX   int     // X方向重叠像素数
	OverlapY   int     // Y方向重叠像素数
	NamePrefix string  // 切片名称前缀
	StartIndex int     // 起始索引
	BufferDist float64 // 缓冲距离（可选）
}

// TileInfo 切片信息
type TileInfo2 struct {
	Name        string     // 切片名称
	Index       int        // 切片索引
	Row         int        // 行号
	Col         int        // 列号
	Bounds      [4]float64 // 切片边界 [minX, minY, maxX, maxY]
	PixelBounds [4]int     // 像素边界 [minX, minY, maxX, maxY]
	Width       int        // 切片宽度（像素）
	Height      int        // 切片高度（像素）
}

// RasterTiler 栅格切片器
type RasterTiler struct {
	raster   *RasterDataset
	options  *TileOptions
	tiles    []TileInfo2
	numRows  int
	numCols  int
	geoLayer *GDALLayer // 生成的矢量图层
}

// NewRasterTiler 创建栅格切片器
func NewRasterTiler(raster *RasterDataset, options *TileOptions) (*RasterTiler, error) {
	if raster == nil {
		return nil, fmt.Errorf("raster dataset is nil")
	}

	// 设置默认选项
	if options == nil {
		options = &TileOptions{
			TileWidth:  1024,
			TileHeight: 1024,
			OverlapX:   0,
			OverlapY:   0,
			NamePrefix: "tile",
			StartIndex: 0,
		}
	}

	if options.TileWidth <= 0 {
		options.TileWidth = 1024
	}
	if options.TileHeight <= 0 {
		options.TileHeight = 1024
	}
	if options.NamePrefix == "" {
		options.NamePrefix = "tile"
	}

	tiler := &RasterTiler{
		raster:  raster,
		options: options,
	}

	return tiler, nil
}

// GenerateTiles 生成切片信息
func (t *RasterTiler) GenerateTiles() error {
	// 计算步长（考虑重叠）
	stepX := t.options.TileWidth - t.options.OverlapX
	stepY := t.options.TileHeight - t.options.OverlapY

	if stepX <= 0 || stepY <= 0 {
		return fmt.Errorf("invalid step size: overlap is too large")
	}

	// 计算需要多少行和列
	t.numCols = int(math.Ceil(float64(t.raster.width) / float64(stepX)))
	t.numRows = int(math.Ceil(float64(t.raster.height) / float64(stepY)))

	// 获取地理变换参数
	bounds := t.raster.bounds
	geoWidth := bounds[2] - bounds[0]  // maxX - minX
	geoHeight := bounds[3] - bounds[1] // maxY - minY

	// 计算像素分辨率
	pixelResX := geoWidth / float64(t.raster.width)
	pixelResY := geoHeight / float64(t.raster.height)

	// 生成切片信息
	t.tiles = make([]TileInfo2, 0, t.numRows*t.numCols)
	index := t.options.StartIndex

	for row := 0; row < t.numRows; row++ {
		for col := 0; col < t.numCols; col++ {
			// 计算像素坐标范围
			pixelMinX := col * stepX
			pixelMinY := row * stepY
			pixelMaxX := pixelMinX + t.options.TileWidth
			pixelMaxY := pixelMinY + t.options.TileHeight

			// 确保不超出图像边界
			if pixelMaxX > t.raster.width {
				pixelMaxX = t.raster.width
			}
			if pixelMaxY > t.raster.height {
				pixelMaxY = t.raster.height
			}

			// 计算实际切片大小
			tileWidth := pixelMaxX - pixelMinX
			tileHeight := pixelMaxY - pixelMinY

			// 跳过太小的切片
			if tileWidth < t.options.TileWidth/4 || tileHeight < t.options.TileHeight/4 {
				continue
			}

			// 计算地理坐标范围
			var geoMinX, geoMinY, geoMaxX, geoMaxY float64

			if t.raster.hasGeoInfo {
				// 有地理信息：使用实际坐标
				geoMinX = bounds[0] + float64(pixelMinX)*pixelResX
				maxY := bounds[3] - float64(pixelMinY)*math.Abs(pixelResY)
				geoMaxX = bounds[0] + float64(pixelMaxX)*pixelResX
				geoMinY = bounds[3] - float64(pixelMaxY)*math.Abs(pixelResY)
				geoMaxY = maxY

				// 应用缓冲区
				if t.options.BufferDist > 0 {
					geoMinX -= t.options.BufferDist
					geoMinY -= t.options.BufferDist
					geoMaxX += t.options.BufferDist
					geoMaxY += t.options.BufferDist
				}
			} else {
				// 像素坐标系：直接使用像素坐标
				geoMinX = float64(pixelMinX)
				geoMinY = float64(pixelMinY)
				geoMaxX = float64(pixelMaxX)
				geoMaxY = float64(pixelMaxY)

				// 应用缓冲区（像素单位）
				if t.options.BufferDist > 0 {
					geoMinX -= t.options.BufferDist
					geoMinY -= t.options.BufferDist
					geoMaxX += t.options.BufferDist
					geoMaxY += t.options.BufferDist
				}
			}

			// 创建切片信息
			tile := TileInfo2{
				Name:        fmt.Sprintf("%s_%d_r%d_c%d", t.options.NamePrefix, index, row, col),
				Index:       index,
				Row:         row,
				Col:         col,
				Bounds:      [4]float64{geoMinX, geoMinY, geoMaxX, geoMaxY},
				PixelBounds: [4]int{pixelMinX, pixelMinY, pixelMaxX, pixelMaxY},
				Width:       tileWidth,
				Height:      tileHeight,
			}

			t.tiles = append(t.tiles, tile)
			index++
		}
	}

	return nil
}

// GetTiles 获取切片信息列表
func (t *RasterTiler) GetTiles() []TileInfo2 {
	return t.tiles
}

// GetTileCount 获取切片数量
func (t *RasterTiler) GetTileCount() int {
	return len(t.tiles)
}

// CreateTileLayer 创建切片矢量图层（内存图层）
func (t *RasterTiler) CreateTileLayer() (*GDALLayer, error) {
	if len(t.tiles) == 0 {
		return nil, fmt.Errorf("no tiles generated, call GenerateTiles() first")
	}

	// 创建内存驱动
	cDriverName := C.CString("Memory")
	defer C.free(unsafe.Pointer(cDriverName))

	driver := C.OGRGetDriverByName(cDriverName)
	if driver == nil {
		return nil, fmt.Errorf("failed to get Memory driver")
	}

	// 创建内存数据源
	cDatasetName := C.CString("")
	defer C.free(unsafe.Pointer(cDatasetName))

	dataset := C.OGR_Dr_CreateDataSource(driver, cDatasetName, nil)
	if dataset == nil {
		return nil, fmt.Errorf("failed to create memory data source")
	}

	// 确定空间参考
	var srs C.OGRSpatialReferenceH
	if t.raster.hasGeoInfo && t.raster.projection != "" && t.raster.projection != "PIXEL" {
		cProjection := C.CString(t.raster.projection)
		defer C.free(unsafe.Pointer(cProjection))

		srs = C.OSRNewSpatialReference(cProjection)
		defer C.OSRDestroySpatialReference(srs)
	}

	// 创建图层
	cLayerName := C.CString("tiles")
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, C.wkbPolygon, nil)
	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("failed to create layer")
	}

	// 添加属性字段
	cNameFieldName := C.CString("NAME")
	defer C.free(unsafe.Pointer(cNameFieldName))
	nameField := C.OGR_Fld_Create(cNameFieldName, C.OFTString)
	C.OGR_Fld_SetWidth(nameField, 255)
	C.OGR_L_CreateField(layer, nameField, 1)
	C.OGR_Fld_Destroy(nameField)

	cIndexFieldName := C.CString("INDEX")
	defer C.free(unsafe.Pointer(cIndexFieldName))
	indexField := C.OGR_Fld_Create(cIndexFieldName, C.OFTInteger)
	C.OGR_L_CreateField(layer, indexField, 1)
	C.OGR_Fld_Destroy(indexField)

	cRowFieldName := C.CString("ROW")
	defer C.free(unsafe.Pointer(cRowFieldName))
	rowField := C.OGR_Fld_Create(cRowFieldName, C.OFTInteger)
	C.OGR_L_CreateField(layer, rowField, 1)
	C.OGR_Fld_Destroy(rowField)

	cColFieldName := C.CString("COL")
	defer C.free(unsafe.Pointer(cColFieldName))
	colField := C.OGR_Fld_Create(cColFieldName, C.OFTInteger)
	C.OGR_L_CreateField(layer, colField, 1)
	C.OGR_Fld_Destroy(colField)

	cWidthFieldName := C.CString("WIDTH")
	defer C.free(unsafe.Pointer(cWidthFieldName))
	widthField := C.OGR_Fld_Create(cWidthFieldName, C.OFTInteger)
	C.OGR_L_CreateField(layer, widthField, 1)
	C.OGR_Fld_Destroy(widthField)

	cHeightFieldName := C.CString("HEIGHT")
	defer C.free(unsafe.Pointer(cHeightFieldName))
	heightField := C.OGR_Fld_Create(cHeightFieldName, C.OFTInteger)
	C.OGR_L_CreateField(layer, heightField, 1)
	C.OGR_Fld_Destroy(heightField)

	// 添加要素
	for _, tile := range t.tiles {
		// 创建矩形几何体
		ring := C.OGR_G_CreateGeometry(C.wkbLinearRing)
		C.OGR_G_AddPoint_2D(ring, C.double(tile.Bounds[0]), C.double(tile.Bounds[1]))
		C.OGR_G_AddPoint_2D(ring, C.double(tile.Bounds[2]), C.double(tile.Bounds[1]))
		C.OGR_G_AddPoint_2D(ring, C.double(tile.Bounds[2]), C.double(tile.Bounds[3]))
		C.OGR_G_AddPoint_2D(ring, C.double(tile.Bounds[0]), C.double(tile.Bounds[3]))
		C.OGR_G_AddPoint_2D(ring, C.double(tile.Bounds[0]), C.double(tile.Bounds[1]))

		polygon := C.OGR_G_CreateGeometry(C.wkbPolygon)
		C.OGR_G_AddGeometryDirectly(polygon, ring)

		// 创建要素
		featureDefn := C.OGR_L_GetLayerDefn(layer)
		feature := C.OGR_F_Create(featureDefn)

		// 设置几何体
		C.OGR_F_SetGeometry(feature, polygon)

		// 设置属性
		cName := C.CString(tile.Name)
		C.OGR_F_SetFieldString(feature, 0, cName) // NAME
		C.free(unsafe.Pointer(cName))

		C.OGR_F_SetFieldInteger(feature, 1, C.int(tile.Index))  // INDEX
		C.OGR_F_SetFieldInteger(feature, 2, C.int(tile.Row))    // ROW
		C.OGR_F_SetFieldInteger(feature, 3, C.int(tile.Col))    // COL
		C.OGR_F_SetFieldInteger(feature, 4, C.int(tile.Width))  // WIDTH
		C.OGR_F_SetFieldInteger(feature, 5, C.int(tile.Height)) // HEIGHT

		// 添加要素到图层
		C.OGR_L_CreateFeature(layer, feature)

		// 清理
		C.OGR_F_Destroy(feature)
		C.OGR_G_DestroyGeometry(polygon)
	}

	// 创建 GDALLayer 对象（修正字段名）
	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset, // 修正：使用 dataset 而不是 dataSource
		driver:  driver,  // 添加：driver 字段
	}

	t.geoLayer = gdalLayer
	return gdalLayer, nil
}

// GetTileLayer 获取已创建的切片图层
func (t *RasterTiler) GetTileLayer() *GDALLayer {
	return t.geoLayer
}

// ClipByTiles 使用切片图层裁剪栅格并返回二进制数据
func (t *RasterTiler) ClipByTiles(clipOptions *ClipOptions) ([]ClipResultByte, error) {
	if t.geoLayer == nil {
		return nil, fmt.Errorf("tile layer not created, call CreateTileLayer() first")
	}

	// 设置裁剪选项
	if clipOptions == nil {
		clipOptions = &ClipOptions{}
	}

	// 确保使用 NAME 字段
	clipOptions.NameField = "NAME"

	// 设置固定的输出尺寸
	clipOptions.TileSize = t.options.TileWidth

	// 根据是否有地理信息选择裁剪方法
	if t.raster.hasGeoInfo {
		return t.raster.ClipRasterByLayerByte(t.geoLayer, clipOptions)
	} else {
		return t.raster.ClipPixelRasterByLayerByte(t.geoLayer, clipOptions)
	}
}

// Close 关闭切片器并释放资源
func (t *RasterTiler) Close() {
	if t.geoLayer != nil {
		t.geoLayer.Close()
		t.geoLayer = nil
	}
}

// GetGridInfo 获取网格信息
func (t *RasterTiler) GetGridInfo() (rows, cols int) {
	return t.numRows, t.numCols
}

// GetTileByRowCol 根据行列号获取切片信息
func (t *RasterTiler) GetTileByRowCol(row, col int) (*TileInfo2, error) {
	for i := range t.tiles {
		if t.tiles[i].Row == row && t.tiles[i].Col == col {
			return &t.tiles[i], nil
		}
	}
	return nil, fmt.Errorf("tile not found at row=%d, col=%d", row, col)
}

// GetTileByIndex 根据索引获取切片信息
func (t *RasterTiler) GetTileByIndex(index int) (*TileInfo2, error) {
	for i := range t.tiles {
		if t.tiles[i].Index == index {
			return &t.tiles[i], nil
		}
	}
	return nil, fmt.Errorf("tile not found with index=%d", index)
}
