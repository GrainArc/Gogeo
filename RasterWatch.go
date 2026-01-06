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
	"sync"
)

// TileServer 动态瓦片服务器（免切片）
type TileServer struct {
	imagePath string
	tileSize  int

	// 数据集池（避免重复打开）
	pool     []*RasterDataset
	poolSize int
	poolMu   sync.Mutex
	poolCond *sync.Cond

	// 缓存配置
	maxPoolSize int
}

// TileServerOptions 瓦片服务器选项
type TileServerOptions struct {
	TileSize  int  // 瓦片大小，默认256
	PoolSize  int  // 数据集池大小，默认为CPU核心数
	Reproject bool // 是否重投影到Web Mercator
}

// NewTileServer 创建动态瓦片服务器
func NewTileServer(imagePath string, options *TileServerOptions) (*TileServer, error) {
	if options == nil {
		options = &TileServerOptions{}
	}
	if options.TileSize <= 0 {
		options.TileSize = 256
	}
	if options.PoolSize <= 0 {
		options.PoolSize = 4
	}

	ts := &TileServer{
		imagePath:   imagePath,
		tileSize:    options.TileSize,
		pool:        make([]*RasterDataset, 0, options.PoolSize),
		maxPoolSize: options.PoolSize,
	}
	ts.poolCond = sync.NewCond(&ts.poolMu)

	// 预热连接池
	for i := 0; i < options.PoolSize; i++ {
		ds, err := OpenRasterDataset(imagePath, true)
		if err != nil {
			ts.Close()
			return nil, fmt.Errorf("failed to open dataset: %w", err)
		}
		ts.pool = append(ts.pool, ds)
	}

	return ts, nil
}

// Close 关闭瓦片服务器
func (ts *TileServer) Close() {
	ts.poolMu.Lock()
	defer ts.poolMu.Unlock()

	for _, ds := range ts.pool {
		if ds != nil {
			ds.Close()
		}
	}
	ts.pool = nil
}

// acquireDataset 从池中获取数据集
func (ts *TileServer) acquireDataset() *RasterDataset {
	ts.poolMu.Lock()
	defer ts.poolMu.Unlock()

	// 等待可用的数据集
	for len(ts.pool) == 0 {
		ts.poolCond.Wait()
	}

	// 取出最后一个
	ds := ts.pool[len(ts.pool)-1]
	ts.pool = ts.pool[:len(ts.pool)-1]
	return ds
}

// releaseDataset 归还数据集到池中
func (ts *TileServer) releaseDataset(ds *RasterDataset) {
	ts.poolMu.Lock()
	defer ts.poolMu.Unlock()

	ts.pool = append(ts.pool, ds)
	ts.poolCond.Signal()
}

// GetTile 获取瓦片（核心方法 - 高性能）
func (ts *TileServer) GetTile(z, x, y int) ([]byte, error) {
	// 从池中获取数据集
	ds := ts.acquireDataset()
	defer ts.releaseDataset(ds)

	// 直接读取瓦片
	return ds.ReadTile(z, x, y, ts.tileSize)
}

// GetTilePNG 获取PNG格式瓦片
func (ts *TileServer) GetTilePNG(z, x, y int) ([]byte, error) {
	return ts.GetTile(z, x, y)
}

// GetTerrainTile 获取地形瓦片（Terrain-RGB格式）
func (ts *TileServer) GetTerrainTile(z, x, y int, encoding string) ([]byte, error) {
	if encoding == "" {
		encoding = "mapbox"
	}

	ds := ts.acquireDataset()
	defer ts.releaseDataset(ds)

	return readTerrainTileFromDataset(ds, z, x, y, ts.tileSize, encoding)
}

// GetBounds 获取边界（经纬度）
func (ts *TileServer) GetBounds() (minLon, minLat, maxLon, maxLat float64) {
	ds := ts.acquireDataset()
	defer ts.releaseDataset(ds)
	return ds.GetBoundsLatLon()
}

// GetTileRange 获取指定缩放级别的瓦片范围
func (ts *TileServer) GetTileRange(zoom int) (minTileX, minTileY, maxTileX, maxTileY int) {
	ds := ts.acquireDataset()
	defer ts.releaseDataset(ds)
	return ds.GetTileRange(zoom)
}

// readTerrainTileFromDataset 从数据集读取地形瓦片
func readTerrainTileFromDataset(ds *RasterDataset, zoom, x, y, tileSize int, encoding string) ([]byte, error) {
	elevationData, noDataValue, err := ds.ReadTileRawWithNoData(zoom, x, y, tileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read elevation data: %w", err)
	}

	rgbData := elevationToTerrainRGBFast(elevationData, noDataValue, tileSize, encoding)

	img := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))
	copy(img.Pix, rgbData)

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, fmt.Errorf("failed to encode PNG: %w", err)
	}

	return buf.Bytes(), nil
}

// elevationToTerrainRGBFast 快速高程转RGB（优化版本）
func elevationToTerrainRGBFast(elevationData []float32, noDataValue float32, tileSize int, encoding string) []byte {
	size := tileSize * tileSize
	rgbData := make([]byte, size*4)

	isMapbox := encoding != "terrarium"

	for i := 0; i < size; i++ {
		height := elevationData[i]
		idx := i * 4

		// 检查NoData
		if height == noDataValue || height != height { // NaN check
			if isMapbox {
				rgbData[idx] = 1
				rgbData[idx+1] = 134
				rgbData[idx+2] = 160
			} else {
				rgbData[idx] = 128
				rgbData[idx+1] = 0
				rgbData[idx+2] = 0
			}
			rgbData[idx+3] = 255
			continue
		}

		var r, g, b uint8

		if isMapbox {
			// Mapbox编码
			value := int64((float64(height) + 10000.0) * 10.0)
			if value < 0 {
				value = 0
			} else if value > 16777215 {
				value = 16777215
			}
			r = uint8(value >> 16)
			g = uint8((value >> 8) & 0xFF)
			b = uint8(value & 0xFF)
		} else {
			// Terrarium编码
			value := height + 32768.0
			if value < 0 {
				value = 0
			} else if value > 65535 {
				value = 65535
			}
			r = uint8(int(value) >> 8)
			g = uint8(int(value) & 0xFF)
			b = uint8((value - float32(int(value))) * 256)
		}

		rgbData[idx] = r
		rgbData[idx+1] = g
		rgbData[idx+2] = b
		rgbData[idx+3] = 255
	}

	return rgbData
}

// ============================================
// 更高性能版本：使用VRT缓存
// ============================================

// FastTileServer 高性能瓦片服务器（使用内存缓存）
type FastTileServer struct {
	imagePath string
	tileSize  int

	// 主数据集（只读）
	dataset *RasterDataset

	// 读取锁（GDAL不是完全线程安全的）
	mu sync.RWMutex

	// 瓦片缓存
	cache     map[string][]byte
	cacheMu   sync.RWMutex
	cacheSize int
	maxCache  int
}

// FastTileServerOptions 高性能服务器选项
type FastTileServerOptions struct {
	TileSize  int // 瓦片大小
	CacheSize int // 缓存瓦片数量
}

// NewFastTileServer 创建高性能瓦片服务器
func NewFastTileServer(imagePath string, options *FastTileServerOptions) (*FastTileServer, error) {
	if options == nil {
		options = &FastTileServerOptions{}
	}
	if options.TileSize <= 0 {
		options.TileSize = 256
	}
	if options.CacheSize <= 0 {
		options.CacheSize = 1000 // 默认缓存1000个瓦片
	}

	ds, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open dataset: %w", err)
	}

	return &FastTileServer{
		imagePath: imagePath,
		tileSize:  options.TileSize,
		dataset:   ds,
		cache:     make(map[string][]byte),
		maxCache:  options.CacheSize,
	}, nil
}

// Close 关闭服务器
func (fs *FastTileServer) Close() {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.dataset != nil {
		fs.dataset.Close()
		fs.dataset = nil
	}

	fs.cacheMu.Lock()
	fs.cache = nil
	fs.cacheMu.Unlock()
}

// GetTile 获取瓦片（带缓存）
func (fs *FastTileServer) GetTile(z, x, y int) ([]byte, error) {
	key := fmt.Sprintf("%d/%d/%d", z, x, y)

	// 先查缓存
	fs.cacheMu.RLock()
	if data, ok := fs.cache[key]; ok {
		fs.cacheMu.RUnlock()
		return data, nil
	}
	fs.cacheMu.RUnlock()

	// 读取瓦片
	fs.mu.Lock()
	data, err := fs.dataset.ReadTile(z, x, y, fs.tileSize)
	fs.mu.Unlock()

	if err != nil {
		return nil, err
	}

	// 存入缓存
	fs.cacheMu.Lock()
	if fs.cacheSize < fs.maxCache {
		fs.cache[key] = data
		fs.cacheSize++
	}
	fs.cacheMu.Unlock()

	return data, nil
}

// GetTileNoCache 获取瓦片（不使用缓存，最快）
func (fs *FastTileServer) GetTileNoCache(z, x, y int) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.dataset.ReadTile(z, x, y, fs.tileSize)
}

// ClearCache 清空缓存
func (fs *FastTileServer) ClearCache() {
	fs.cacheMu.Lock()
	defer fs.cacheMu.Unlock()
	fs.cache = make(map[string][]byte)
	fs.cacheSize = 0
}

// GetBounds 获取边界
func (fs *FastTileServer) GetBounds() (minLon, minLat, maxLon, maxLat float64) {
	return fs.dataset.GetBoundsLatLon()
}

// ============================================
// 直接函数调用版本（最简单）
// ============================================

// ReadTileDirect 直接读取瓦片（一次性使用，每次打开关闭数据集）
// 适用于低频请求场景
func ReadTileDirect(imagePath string, z, x, y, tileSize int) ([]byte, error) {
	ds, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		return nil, err
	}
	defer ds.Close()

	return ds.ReadTile(z, x, y, tileSize)
}

// ReadTerrainTileDirect 直接读取地形瓦片
func ReadTerrainTileDirect(imagePath string, z, x, y, tileSize int, encoding string) ([]byte, error) {
	ds, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		return nil, err
	}
	defer ds.Close()

	return readTerrainTileFromDataset(ds, z, x, y, tileSize, encoding)
}
