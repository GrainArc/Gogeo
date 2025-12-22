package Gogeo

import (
	"bytes"
	"database/sql"
	"fmt"
	"image"
	"image/png"
	"log"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// MBTilesGenerator MBTiles生成器
type MBTilesGenerator struct {
	dataset   *RasterDataset
	tileSize  int
	imagePath string
	minZoom   int
	maxZoom   int

	progressCallback ProgressCallback
}

// MBTilesOptions MBTiles生成选项
type MBTilesOptions struct {
	TileSize int               // 瓦片大小，默认256
	MinZoom  int               // 最小缩放级别，默认0
	MaxZoom  int               // 最大缩放级别，默认18
	Metadata map[string]string // 自定义元数据

	Concurrency      int              // 并发数，默认为CPU核心数
	ProgressCallback ProgressCallback // 进度回调函数
	BatchSize        int              // 批量插入大小，默认1000
}

// TileTask 瓦片任务
type TileTask struct {
	Zoom int
	X    int
	Y    int
}

// RasterTileResult 瓦片结果
type RasterTileResult struct {
	Zoom  int
	X     int
	Y     int
	Data  []byte
	Error error
}

// NewMBTilesGenerator 创建MBTiles生成器
func NewMBTilesGenerator(imagePath string, options *MBTilesOptions) (*MBTilesGenerator, error) {
	dataset, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		return nil, err
	}

	if options == nil {
		options = &MBTilesOptions{}
	}
	if options.TileSize <= 0 {
		options.TileSize = 256
	}
	if options.MinZoom < 0 {
		options.MinZoom = 0
	}
	if options.MaxZoom <= 0 || options.MaxZoom > 22 {
		options.MaxZoom = 18
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}

	gen := &MBTilesGenerator{
		dataset:   dataset,
		imagePath: imagePath,
		tileSize:  options.TileSize,
		minZoom:   options.MinZoom,
		maxZoom:   options.MaxZoom,

		progressCallback: options.ProgressCallback,
	}

	return gen, nil
}

// Close 关闭生成器
func (gen *MBTilesGenerator) Close() {
	if gen.dataset != nil {
		gen.dataset.Close()
		gen.dataset = nil
	}
}

// Generate 生成MBTiles文件
func (gen *MBTilesGenerator) Generate(outputPath string, metadata map[string]string) error {
	// 创建SQLite数据库
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	// 优化数据库配置
	if err := gen.optimizeDatabase(db); err != nil {
		return fmt.Errorf("failed to optimize database: %w", err)
	}

	// 创建表结构
	if err := gen.createTables(db); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 写入元数据
	if err := gen.writeMetadata(db, metadata); err != nil {
		return fmt.Errorf("failed to write meta%w", err)
	}

	// 生成瓦片
	if err := gen.generateTiles(db); err != nil {
		return fmt.Errorf("failed to generate tiles: %w", err)
	}

	log.Printf("MBTiles generation completed: %s", outputPath)
	return nil
}

// GenerateWithConcurrency 并发生成MBTiles文件
func (gen *MBTilesGenerator) GenerateWithConcurrency(outputPath string, metadata map[string]string, concurrency int) error {
	// 创建SQLite数据库
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// 优化数据库配置
	if err := gen.optimizeDatabase(db); err != nil {
		db.Close()
		return fmt.Errorf("failed to optimize database: %w", err)
	}

	// 创建表结构
	if err := gen.createTables(db); err != nil {
		db.Close()
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 写入元数据
	if err := gen.writeMetadata(db, metadata); err != nil {
		db.Close()
		return fmt.Errorf("failed to write meta%w", err)
	}

	// 并发生成瓦片
	if err := gen.generateTilesConcurrent(db, concurrency, gen.imagePath); err != nil {
		db.Close()
		return fmt.Errorf("failed to generate tiles: %w", err)
	}

	return nil
}

// optimizeDatabase 优化数据库配置
func (gen *MBTilesGenerator) optimizeDatabase(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",        // 启用WAL模式，允许并发读写
		"PRAGMA synchronous=NORMAL",      // 降低同步级别，提升性能
		"PRAGMA cache_size=10000",        // 增加缓存大小
		"PRAGMA page_size=4096",          // 设置页面大小
		"PRAGMA temp_store=MEMORY",       // 临时表存储在内存
		"PRAGMA mmap_size=268435456",     // 启用内存映射（256MB）
		"PRAGMA locking_mode=EXCLUSIVE",  // 独占模式（可选）
		"PRAGMA auto_vacuum=INCREMENTAL", // 增量自动清理
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Printf("Warning: failed to execute %s: %v", pragma, err)
			// 不返回错误，继续执行
		}
	}

	return nil
}

// createTables 创建MBTiles数据库表
func (gen *MBTilesGenerator) createTables(db *sql.DB) error {
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS metadata (
			name TEXT PRIMARY KEY, 
			value TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS tiles (
			zoom_level INTEGER,
			tile_column INTEGER,
			tile_row INTEGER,
			tile_data BLOB,
			PRIMARY KEY (zoom_level, tile_column, tile_row)
		)`,
		`CREATE INDEX IF NOT EXISTS tiles_idx ON tiles(zoom_level, tile_column, tile_row)`,
	}

	for _, schema := range schemas {
		if _, err := db.Exec(schema); err != nil {
			return err
		}
	}

	return nil
}

// writeMetadata 写入MBTiles元数据
func (gen *MBTilesGenerator) writeMetadata(db *sql.DB, customMetadata map[string]string) error {
	minLon, minLat, maxLon, maxLat := gen.dataset.GetBoundsLatLon()

	// 默认元数据
	defaultMetadata := map[string]string{
		"name":        "Generated Tiles",
		"type":        "baselayer",
		"version":     "1.0",
		"description": "Tiles generated from raster image",
		"format":      "png",
		"bounds":      fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", minLon, minLat, maxLon, maxLat),
		"center":      fmt.Sprintf("%.6f,%.6f,%d", (minLon+maxLon)/2, (minLat+maxLat)/2, gen.minZoom),
		"minzoom":     fmt.Sprintf("%d", gen.minZoom),
		"maxzoom":     fmt.Sprintf("%d", gen.maxZoom),
	}

	// 合并自定义元数据
	for k, v := range customMetadata {
		defaultMetadata[k] = v
	}

	// 使用事务批量插入
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for k, v := range defaultMetadata {
		if _, err := stmt.Exec(k, v); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// generateTiles 生成所有瓦片（单线程版本，使用批量插入）
func (gen *MBTilesGenerator) generateTiles(db *sql.DB) error {
	totalTiles := 0
	estimatedTotal := gen.EstimateTileCount()

	// 调用进度回调 - 开始
	if gen.progressCallback != nil {
		if !gen.progressCallback(0, "Starting tile generation") {
			return fmt.Errorf("operation cancelled by user")
		}
	}

	// 批量插入缓冲区
	const batchSize = 100
	batch := make([]RasterTileResult, 0, batchSize)

	// 遍历缩放级别
	for zoom := gen.minZoom; zoom <= gen.maxZoom; zoom++ {
		minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

		// 遍历瓦片
		for x := minTileX; x <= maxTileX; x++ {
			for y := minTileY; y <= maxTileY; y++ {
				// 读取瓦片数据
				tileData, err := gen.dataset.ReadTile(zoom, x, y, gen.tileSize)
				if err != nil {
					log.Printf("Warning: failed to generate tile %d/%d/%d: %v", zoom, x, y, err)
					continue
				}

				batch = append(batch, RasterTileResult{
					Zoom: zoom,
					X:    x,
					Y:    y,
					Data: tileData,
				})

				// 批量插入
				if len(batch) >= batchSize {
					if err := gen.batchInsertTiles(db, batch); err != nil {
						return err
					}
					totalTiles += len(batch)
					batch = batch[:0]

					// 输出进度
					progress := float64(totalTiles) / float64(estimatedTotal)
					message := fmt.Sprintf("Generated %d/%d tiles (%.2f%%)", totalTiles, estimatedTotal, progress*100)

					if gen.progressCallback != nil {
						if !gen.progressCallback(progress, message) {
							return fmt.Errorf("operation cancelled by user")
						}
					}
				}
			}
		}
	}

	// 插入剩余的瓦片
	if len(batch) > 0 {
		if err := gen.batchInsertTiles(db, batch); err != nil {
			return err
		}
		totalTiles += len(batch)
	}

	// 调用进度回调 - 完成
	if gen.progressCallback != nil {
		gen.progressCallback(1.0, fmt.Sprintf("Successfully generated %d tiles", totalTiles))
	}

	log.Printf("Successfully generated %d tiles", totalTiles)
	return nil
}

// batchInsertTiles 批量插入瓦片
func (gen *MBTilesGenerator) batchInsertTiles(db *sql.DB, tiles []RasterTileResult) error {
	if len(tiles) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, tile := range tiles {
		if _, err := stmt.Exec(tile.Zoom, tile.X, tile.Y, tile.Data); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// tileWriter 瓦片写入协程（使用批量插入）
func (gen *MBTilesGenerator) tileWriter(db *sql.DB, results <-chan RasterTileResult, totalTiles, errorCount, cancelled, earlyReturn *int32, estimatedTotal int, batchSize int) error {
	batch := make([]RasterTileResult, 0, batchSize)
	ticker := time.NewTicker(100 * time.Millisecond) // 定时刷新
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := gen.batchInsertTiles(db, batch); err != nil {
			return err
		}

		current := atomic.AddInt32(totalTiles, int32(len(batch)))
		batch = batch[:0]

		// 输出进度
		if atomic.LoadInt32(earlyReturn) == 0 {
			progress := float64(current) / float64(estimatedTotal)
			message := fmt.Sprintf("Progress: %d/%d tiles (%.2f%%)", current, estimatedTotal, progress*100)
			if gen.progressCallback != nil {
				if !gen.progressCallback(progress, message) {
					atomic.StoreInt32(cancelled, 1)
					return fmt.Errorf("operation cancelled by user")
				}
			}
		}

		return nil
	}

	for {
		select {
		case result, ok := <-results:
			if !ok {
				// 通道关闭，刷新剩余数据
				return flush()
			}

			// 检查是否已取消
			if atomic.LoadInt32(cancelled) == 1 {
				return fmt.Errorf("operation cancelled by user")
			}

			if result.Error != nil {
				log.Printf("Warning: failed to generate tile %d/%d/%d: %v",
					result.Zoom, result.X, result.Y, result.Error)
				atomic.AddInt32(errorCount, 1)
				continue
			}

			batch = append(batch, result)

			// 达到批量大小，执行插入
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}

		case <-ticker.C:
			// 定时刷新
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// generateTilesConcurrent 并发生成所有瓦片
func (gen *MBTilesGenerator) generateTilesConcurrent(db *sql.DB, concurrency int, imagePath string) error {
	if concurrency <= 0 {
		concurrency = 4
	}

	const batchSize = 500 // 批量插入大小

	// 创建任务通道和结果通道
	taskChan := make(chan TileTask, concurrency*10)
	resultChan := make(chan RasterTileResult, concurrency*10)

	// 统计变量
	var totalTiles int32
	var errorCount int32
	var cancelled int32
	var earlyReturn int32
	estimatedTotal := gen.EstimateTileCount()

	// 调用进度回调 - 开始
	if gen.progressCallback != nil {
		if !gen.progressCallback(0, "Starting concurrent tile generation") {
			db.Close()
			return fmt.Errorf("operation cancelled by user")
		}
	}

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			gen.tileWorker(workerID, imagePath, taskChan, resultChan, &cancelled, &earlyReturn)
		}(i)
	}

	// 启动结果写入协程
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- gen.tileWriter(db, resultChan, &totalTiles, &errorCount, &cancelled, &earlyReturn, estimatedTotal, batchSize)
	}()

	// 生成任务
	go func() {
		defer close(taskChan)

		for zoom := gen.minZoom; zoom <= gen.maxZoom; zoom++ {
			if atomic.LoadInt32(&cancelled) == 1 || atomic.LoadInt32(&earlyReturn) == 1 {
				log.Printf("Task generation stopped due to early return")
				return
			}

			minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

			tileCount := (maxTileX - minTileX + 1) * (maxTileY - minTileY + 1)
			log.Printf("Queuing zoom level %d: tiles %d-%d, %d-%d (total: %d)",
				zoom, minTileX, maxTileX, minTileY, maxTileY, tileCount)

			for x := minTileX; x <= maxTileX; x++ {
				for y := minTileY; y <= maxTileY; y++ {
					if atomic.LoadInt32(&cancelled) == 1 || atomic.LoadInt32(&earlyReturn) == 1 {
						return
					}

					taskChan <- TileTask{
						Zoom: zoom,
						X:    x,
						Y:    y,
					}
				}
			}
		}
	}()

	// 监控进度并在99%时强制返回
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			current := atomic.LoadInt32(&totalTiles)
			progress := float64(current) / float64(estimatedTotal)

			// 当进度达到99%时，强制关闭并返回
			if progress >= 0.99 && atomic.LoadInt32(&earlyReturn) == 0 {
				atomic.StoreInt32(&earlyReturn, 1)

				log.Printf("Progress reached 99%% (%d/%d tiles), forcing completion...", current, estimatedTotal)

				// 回调通知99%完成
				if gen.progressCallback != nil {
					gen.progressCallback(0.99, fmt.Sprintf("99%% completed (%d/%d tiles), forcing shutdown...", current, estimatedTotal))
				}

				// 等待一小段时间让当前批次写入完成
				time.Sleep(200 * time.Millisecond)

				// 强制关闭数据库
				if err := db.Close(); err != nil {
					log.Printf("Warning: Error closing database: %v", err)
				} else {
					log.Printf("Database closed successfully")
				}

				// 强制GC回收
				log.Printf("Forcing garbage collection...")
				runtime.GC()
				runtime.GC()         // 调用两次确保彻底回收
				debug.FreeOSMemory() // 释放内存给操作系统

				log.Printf("Memory cleanup completed")

				// 立即返回,视为完成
				if gen.progressCallback != nil {
					gen.progressCallback(1.0, fmt.Sprintf("Force completed with %d tiles", current))
				}

				return nil
			}

			// 正常完成检查
			if current >= int32(estimatedTotal) {
				// 等待所有协程完成
				wg.Wait()
				close(resultChan)

				if err := <-writerDone; err != nil {
					db.Close()
					return err
				}

				if err := db.Close(); err != nil {
					return fmt.Errorf("error closing database: %w", err)
				}

				if gen.progressCallback != nil {
					gen.progressCallback(1.0, fmt.Sprintf("Successfully generated %d tiles with %d errors", totalTiles, errorCount))
				}

				log.Printf("Successfully generated %d tiles with %d errors", totalTiles, errorCount)
				return nil
			}

			// 取消检查
			if atomic.LoadInt32(&cancelled) == 1 {
				db.Close()
				return fmt.Errorf("operation cancelled by user")
			}
		}
	}
}

// tileWorker 瓦片生成工作协程 (修改签名,添加earlyReturn参数)
func (gen *MBTilesGenerator) tileWorker(workerID int, imagePath string, tasks <-chan TileTask, results chan<- RasterTileResult, cancelled *int32, earlyReturn *int32) {
	dataset, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		log.Printf("Worker %d failed to open dataset: %v", workerID, err)
		return
	}
	defer dataset.Close()

	for task := range tasks {
		// 检查是否需要提前返回
		if atomic.LoadInt32(cancelled) == 1 || atomic.LoadInt32(earlyReturn) == 1 {
			return
		}

		tileData, err := dataset.ReadTile(task.Zoom, task.X, task.Y, gen.tileSize)

		// 尝试发送结果,如果通道已关闭则退出
		select {
		case results <- RasterTileResult{
			Zoom:  task.Zoom,
			X:     task.X,
			Y:     task.Y,
			Data:  tileData,
			Error: err,
		}:
		default:
			// 通道可能已关闭,直接返回
			if atomic.LoadInt32(earlyReturn) == 1 {
				return
			}
		}
	}
}

// GetDatasetInfo 获取数据集信息
func (gen *MBTilesGenerator) GetDatasetInfo() DatasetInfo {
	return gen.dataset.GetInfo()
}

// GetBounds 获取边界（经纬度）
func (gen *MBTilesGenerator) GetBounds() (minLon, minLat, maxLon, maxLat float64) {
	return gen.dataset.GetBoundsLatLon()
}

// EstimateTileCount 估算瓦片数量
func (gen *MBTilesGenerator) EstimateTileCount() int {
	total := 0
	for zoom := gen.minZoom; zoom <= gen.maxZoom; zoom++ {
		minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)
		count := (maxTileX - minTileX + 1) * (maxTileY - minTileY + 1)
		total += count
	}
	return total
}

// TerrainTileResult 地形瓦片结果
type TerrainTileResult struct {
	Zoom  int
	X     int
	Y     int
	Data  []byte
	Error error
}

// TerrainOptions 地形瓦片生成选项
type TerrainOptions struct {
	TileSize         int              // 瓦片大小，默认256（Mapbox Terrain-RGB使用256或512）
	MinZoom          int              // 最小缩放级别
	MaxZoom          int              // 最大缩放级别
	Encoding         string           // 编码方式: "mapbox" 或 "terrarium"
	Concurrency      int              // 并发数
	BatchSize        int              // 批量插入大小
	ProgressCallback ProgressCallback // 进度回调
}

// GenerateTerrainMBTiles 生成Mapbox规范的地形瓦片MBTiles文件
// demPath: DEM高程数据文件路径
// outputPath: 输出MBTiles文件路径
// options: 生成选项
func (gen *MBTilesGenerator) GenerateTerrainMBTiles(outputPath string, options *TerrainOptions) error {
	if options == nil {
		options = &TerrainOptions{}
	}

	// 设置默认值
	if options.TileSize <= 0 {
		options.TileSize = 256
	}
	if options.MinZoom < 0 {
		options.MinZoom = gen.minZoom
	}
	if options.MaxZoom <= 0 {
		options.MaxZoom = gen.maxZoom
	}
	if options.Encoding == "" {
		options.Encoding = "mapbox"
	}
	if options.Concurrency <= 0 {
		options.Concurrency = runtime.NumCPU()
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.ProgressCallback == nil {
		options.ProgressCallback = gen.progressCallback
	}

	// 创建SQLite数据库
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	// 优化数据库配置
	if err := gen.optimizeDatabase(db); err != nil {
		return fmt.Errorf("failed to optimize database: %w", err)
	}

	// 创建表结构
	if err := gen.createTables(db); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 写入地形元数据
	if err := gen.writeTerrainMetadata(db, options); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// 生成地形瓦片
	if options.Concurrency > 1 {
		if err := gen.generateTerrainTilesConcurrent(db, options); err != nil {
			return fmt.Errorf("failed to generate terrain tiles: %w", err)
		}
	} else {
		if err := gen.generateTerrainTiles(db, options); err != nil {
			return fmt.Errorf("failed to generate terrain tiles: %w", err)
		}
	}

	log.Printf("Terrain MBTiles generation completed: %s", outputPath)
	return nil
}

// writeTerrainMetadata 写入地形MBTiles元数据
func (gen *MBTilesGenerator) writeTerrainMetadata(db *sql.DB, options *TerrainOptions) error {
	minLon, minLat, maxLon, maxLat := gen.dataset.GetBoundsLatLon()

	metadata := map[string]string{
		"name":        "Terrain RGB Tiles",
		"type":        "baselayer",
		"version":     "1.0",
		"description": "Mapbox Terrain-RGB tiles generated from DEM",
		"format":      "png",
		"encoding":    options.Encoding,
		"bounds":      fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", minLon, minLat, maxLon, maxLat),
		"center":      fmt.Sprintf("%.6f,%.6f,%d", (minLon+maxLon)/2, (minLat+maxLat)/2, options.MinZoom),
		"minzoom":     fmt.Sprintf("%d", options.MinZoom),
		"maxzoom":     fmt.Sprintf("%d", options.MaxZoom),
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for k, v := range metadata {
		if _, err := stmt.Exec(k, v); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// generateTerrainTiles 单线程生成地形瓦片
func (gen *MBTilesGenerator) generateTerrainTiles(db *sql.DB, options *TerrainOptions) error {
	totalTiles := 0
	estimatedTotal := gen.estimateTerrainTileCount(options.MinZoom, options.MaxZoom)

	if options.ProgressCallback != nil {
		if !options.ProgressCallback(0, "Starting terrain tile generation") {
			return fmt.Errorf("operation cancelled by user")
		}
	}

	batch := make([]RasterTileResult, 0, options.BatchSize)

	for zoom := options.MinZoom; zoom <= options.MaxZoom; zoom++ {
		minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

		for x := minTileX; x <= maxTileX; x++ {
			for y := minTileY; y <= maxTileY; y++ {
				// 读取高程数据并转换为Terrain-RGB
				tileData, err := gen.readTerrainTile(zoom, x, y, options.TileSize, options.Encoding)
				if err != nil {
					log.Printf("Warning: failed to generate terrain tile %d/%d/%d: %v", zoom, x, y, err)
					continue
				}

				batch = append(batch, RasterTileResult{
					Zoom: zoom,
					X:    x,
					Y:    y,
					Data: tileData,
				})

				if len(batch) >= options.BatchSize {
					if err := gen.batchInsertTiles(db, batch); err != nil {
						return err
					}
					totalTiles += len(batch)
					batch = batch[:0]

					progress := float64(totalTiles) / float64(estimatedTotal)
					if options.ProgressCallback != nil {
						if !options.ProgressCallback(progress, fmt.Sprintf("Generated %d/%d terrain tiles (%.2f%%)", totalTiles, estimatedTotal, progress*100)) {
							return fmt.Errorf("operation cancelled by user")
						}
					}
				}
			}
		}
	}

	// 插入剩余瓦片
	if len(batch) > 0 {
		if err := gen.batchInsertTiles(db, batch); err != nil {
			return err
		}
		totalTiles += len(batch)
	}

	if options.ProgressCallback != nil {
		options.ProgressCallback(1.0, fmt.Sprintf("Successfully generated %d terrain tiles", totalTiles))
	}

	return nil
}

// generateTerrainTilesConcurrent 并发生成地形瓦片
func (gen *MBTilesGenerator) generateTerrainTilesConcurrent(db *sql.DB, options *TerrainOptions) error {
	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	taskChan := make(chan TileTask, concurrency*10)
	resultChan := make(chan RasterTileResult, concurrency*10)

	var totalTiles int32
	var errorCount int32
	var cancelled int32
	var earlyReturn int32
	estimatedTotal := gen.estimateTerrainTileCount(options.MinZoom, options.MaxZoom)

	if options.ProgressCallback != nil {
		if !options.ProgressCallback(0, "Starting concurrent terrain tile generation") {
			return fmt.Errorf("operation cancelled by user")
		}
	}

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			gen.terrainTileWorker(workerID, gen.imagePath, taskChan, resultChan, &cancelled, &earlyReturn, options)
		}(i)
	}

	// 启动写入协程
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- gen.tileWriter(db, resultChan, &totalTiles, &errorCount, &cancelled, &earlyReturn, estimatedTotal, options.BatchSize)
	}()

	// 生成任务
	go func() {
		defer close(taskChan)

		for zoom := options.MinZoom; zoom <= options.MaxZoom; zoom++ {
			if atomic.LoadInt32(&cancelled) == 1 || atomic.LoadInt32(&earlyReturn) == 1 {
				return
			}

			minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

			for x := minTileX; x <= maxTileX; x++ {
				for y := minTileY; y <= maxTileY; y++ {
					if atomic.LoadInt32(&cancelled) == 1 || atomic.LoadInt32(&earlyReturn) == 1 {
						return
					}

					taskChan <- TileTask{Zoom: zoom, X: x, Y: y}
				}
			}
		}
	}()

	// 等待完成
	wg.Wait()
	close(resultChan)

	if err := <-writerDone; err != nil {
		return err
	}

	if options.ProgressCallback != nil {
		options.ProgressCallback(1.0, fmt.Sprintf("Successfully generated %d terrain tiles with %d errors", totalTiles, errorCount))
	}

	return nil
}

// terrainTileWorker 地形瓦片工作协程
func (gen *MBTilesGenerator) terrainTileWorker(workerID int, imagePath string, tasks <-chan TileTask, results chan<- RasterTileResult, cancelled *int32, earlyReturn *int32, options *TerrainOptions) {
	dataset, err := OpenRasterDataset(imagePath, true)
	if err != nil {
		log.Printf("Worker %d failed to open dataset: %v", workerID, err)
		return
	}
	defer dataset.Close()

	for task := range tasks {
		if atomic.LoadInt32(cancelled) == 1 || atomic.LoadInt32(earlyReturn) == 1 {
			return
		}

		tileData, err := gen.readTerrainTileFromDataset(dataset, task.Zoom, task.X, task.Y, options.TileSize, options.Encoding)

		select {
		case results <- RasterTileResult{
			Zoom:  task.Zoom,
			X:     task.X,
			Y:     task.Y,
			Data:  tileData,
			Error: err,
		}:
		default:
			if atomic.LoadInt32(earlyReturn) == 1 {
				return
			}
		}
	}
}

// readTerrainTile 读取地形瓦片（使用当前数据集）
func (gen *MBTilesGenerator) readTerrainTile(zoom, x, y, tileSize int, encoding string) ([]byte, error) {
	return gen.readTerrainTileFromDataset(gen.dataset, zoom, x, y, tileSize, encoding)
}

// readTerrainTileFromDataset 从指定数据集读取地形瓦片
func (gen *MBTilesGenerator) readTerrainTileFromDataset(dataset *RasterDataset, zoom, x, y, tileSize int, encoding string) ([]byte, error) {
	// 读取原始高程数据
	elevationData, noDataValue, err := dataset.ReadTileRawWithNoData(zoom, x, y, tileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read elevation data: %w", err)
	}

	// 转换为Terrain-RGB格式
	rgbData := gen.elevationToTerrainRGB(elevationData, noDataValue, tileSize, encoding)

	// 编码为PNG
	img := image.NewRGBA(image.Rect(0, 0, tileSize, tileSize))
	copy(img.Pix, rgbData)

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, fmt.Errorf("failed to encode PNG: %w", err)
	}

	return buf.Bytes(), nil
}

// elevationToTerrainRGB 将高程数据转换为Terrain-RGB格式
// Mapbox Terrain-RGB公式:
// height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
// 反向计算:
// value = (height + 10000) / 0.1
// R = floor(value / (256 * 256))
// G = floor((value % (256 * 256)) / 256)
// B = value % 256
func (gen *MBTilesGenerator) elevationToTerrainRGB(elevationData []float32, noDataValue float32, tileSize int, encoding string) []byte {
	rgbData := make([]byte, tileSize*tileSize*4)

	for i := 0; i < tileSize*tileSize; i++ {
		height := elevationData[i]

		var r, g, b, a uint8

		// 检查NoData值 - 将NoData区域的高程设为0
		if height == noDataValue || math.IsNaN(float64(height)) || math.IsInf(float64(height), 0) {
			// NoData区域编码为高程0
			if encoding == "terrarium" {
				// Terrarium: height = 0 -> value = 0 + 32768 = 32768
				// R = 32768 / 256 = 128, G = 32768 % 256 = 0, B = 0
				r, g, b, a = 128, 0, 0, 255
			} else {
				// Mapbox: height = 0 -> value = (0 + 10000) / 0.1 = 100000
				// R = 100000 / 65536 = 1
				// G = (100000 % 65536) / 256 = 134
				// B = 100000 % 256 = 160
				r, g, b, a = 1, 134, 160, 255
			}
		} else {
			a = 255

			if encoding == "terrarium" {
				// Terrarium编码: height = (R * 256 + G + B / 256) - 32768
				// 反向: value = height + 32768
				value := height + 32768.0
				if value < 0 {
					value = 0
				}
				if value > 256*256*256-1 {
					value = 256*256*256 - 1
				}

				r = uint8(int(value) / 256)
				g = uint8(int(value) % 256)
				b = uint8(int((value - float32(int(value))) * 256))
			} else {
				// Mapbox编码 (默认)
				// value = (height + 10000) / 0.1
				value := (float64(height) + 10000.0) / 0.1

				// 限制范围
				if value < 0 {
					value = 0
				}
				if value > 256*256*256-1 {
					value = 256*256*256 - 1
				}

				intValue := int64(math.Floor(value))
				r = uint8(intValue / (256 * 256))
				g = uint8((intValue % (256 * 256)) / 256)
				b = uint8(intValue % 256)
			}
		}

		rgbData[i*4] = r
		rgbData[i*4+1] = g
		rgbData[i*4+2] = b
		rgbData[i*4+3] = a
	}

	return rgbData
}

// estimateTerrainTileCount 估算地形瓦片数量
func (gen *MBTilesGenerator) estimateTerrainTileCount(minZoom, maxZoom int) int {
	total := 0
	for zoom := minZoom; zoom <= maxZoom; zoom++ {
		minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)
		count := (maxTileX - minTileX + 1) * (maxTileY - minTileY + 1)
		total += count
	}
	return total
}

// DecodeTerrainRGB 解码Terrain-RGB值为高程（辅助函数，用于验证）
func DecodeTerrainRGB(r, g, b uint8, encoding string) float64 {
	if encoding == "terrarium" {
		// Terrarium: height = (R * 256 + G + B / 256) - 32768
		return float64(r)*256.0 + float64(g) + float64(b)/256.0 - 32768.0
	}
	// Mapbox: height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
	return -10000.0 + float64(int(r)*256*256+int(g)*256+int(b))*0.1
}

// EncodeTerrainRGB 编码高程为Terrain-RGB值（辅助函数）
func EncodeTerrainRGB(height float64, encoding string) (r, g, b uint8) {
	if encoding == "terrarium" {
		value := height + 32768.0
		if value < 0 {
			value = 0
		}
		if value > 256*256*256-1 {
			value = 256*256*256 - 1
		}
		r = uint8(int(value) / 256)
		g = uint8(int(value) % 256)
		b = uint8(int((value - float64(int(value))) * 256))
		return
	}

	// Mapbox
	value := (height + 10000.0) / 0.1
	if value < 0 {
		value = 0
	}
	if value > 256*256*256-1 {
		value = 256*256*256 - 1
	}
	intValue := int64(math.Floor(value))
	r = uint8(intValue / (256 * 256))
	g = uint8((intValue % (256 * 256)) / 256)
	b = uint8(intValue % 256)
	return
}
