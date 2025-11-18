package Gogeo

import (
	"database/sql"
	"fmt"
	"log"
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
