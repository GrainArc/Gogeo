package Gogeo

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

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
	dataset, err := OpenRasterDataset(imagePath)
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

	gen := &MBTilesGenerator{
		dataset:   dataset,
		imagePath: imagePath, // 保存路径
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
	defer db.Close()

	// 创建表结构
	if err := gen.createTables(db); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 写入元数据
	if err := gen.writeMetadata(db, metadata); err != nil {
		return fmt.Errorf("failed to write meta%w", err)
	}

	// 并发生成瓦片
	if err := gen.generateTilesConcurrent(db, concurrency, gen.imagePath); err != nil {
		return fmt.Errorf("failed to generate tiles: %w", err)
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
		`CREATE INDEX IF NOT EXISTS tile_index ON tiles (
			zoom_level, tile_column, tile_row
		)`,
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

	// 插入元数据
	stmt, err := db.Prepare("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for k, v := range defaultMetadata {
		if _, err := stmt.Exec(k, v); err != nil {
			return err
		}
	}

	return nil
}

// generateTiles 生成所有瓦片（单线程版本）
func (gen *MBTilesGenerator) generateTiles(db *sql.DB) error {
	// 开始事务
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

	totalTiles := 0
	estimatedTotal := gen.EstimateTileCount()

	// 调用进度回调 - 开始
	if gen.progressCallback != nil {
		if !gen.progressCallback(0, "Starting tile generation") {
			return fmt.Errorf("operation cancelled by user")
		}
	}

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

				tileY := y

				if _, err := stmt.Exec(zoom, x, tileY, tileData); err != nil {
					return err
				}

				totalTiles++

				// 定期输出进度和调用回调
				if totalTiles%100 == 0 {
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

	// 提交事务
	if err := tx.Commit(); err != nil {
		return err
	}

	// 调用进度回调 - 完成
	if gen.progressCallback != nil {
		gen.progressCallback(1.0, fmt.Sprintf("Successfully generated %d tiles", totalTiles))
	}

	log.Printf("Successfully generated %d tiles", totalTiles)
	return nil
}

// generateTilesConcurrent 并发生成所有瓦片
func (gen *MBTilesGenerator) generateTilesConcurrent(db *sql.DB, concurrency int, imagePath string) error {
	if concurrency <= 0 {
		concurrency = 4 // 默认并发数
	}

	// 创建任务通道和结果通道
	taskChan := make(chan TileTask, 1000)
	resultChan := make(chan RasterTileResult, 1000)

	// 统计变量
	var totalTiles int32
	var errorCount int32
	var cancelled int32
	estimatedTotal := gen.EstimateTileCount()

	// 调用进度回调 - 开始
	if gen.progressCallback != nil {
		if !gen.progressCallback(0, "Starting concurrent tile generation") {
			return fmt.Errorf("operation cancelled by user")
		}
	}

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			gen.tileWorker(workerID, imagePath, taskChan, resultChan, &cancelled)
		}(i)
	}

	// 启动结果写入协程
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- gen.tileWriter(db, resultChan, &totalTiles, &errorCount, &cancelled, estimatedTotal)
	}()

	// 生成任务
	go func() {
		defer close(taskChan)

		for zoom := gen.minZoom; zoom <= gen.maxZoom; zoom++ {
			// 检查是否已取消
			if atomic.LoadInt32(&cancelled) == 1 {
				log.Printf("Task generation cancelled")
				return
			}

			minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

			tileCount := (maxTileX - minTileX + 1) * (maxTileY - minTileY + 1)
			log.Printf("Queuing zoom level %d: tiles %d-%d, %d-%d (total: %d)",
				zoom, minTileX, maxTileX, minTileY, maxTileY, tileCount)

			for x := minTileX; x <= maxTileX; x++ {
				for y := minTileY; y <= maxTileY; y++ {
					// 检查是否已取消
					if atomic.LoadInt32(&cancelled) == 1 {
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

	// 等待所有工作协程完成
	wg.Wait()
	close(resultChan)

	// 等待写入完成
	if err := <-writerDone; err != nil {
		return err
	}

	// 检查是否被取消
	if atomic.LoadInt32(&cancelled) == 1 {
		return fmt.Errorf("operation cancelled by user")
	}

	// 调用进度回调 - 完成
	if gen.progressCallback != nil {
		gen.progressCallback(1.0, fmt.Sprintf("Successfully generated %d tiles with %d errors", totalTiles, errorCount))
	}

	log.Printf("Successfully generated %d tiles with %d errors", totalTiles, errorCount)
	return nil
}

// tileWorker 瓦片生成工作协程
func (gen *MBTilesGenerator) tileWorker(workerID int, imagePath string, tasks <-chan TileTask, results chan<- RasterTileResult, cancelled *int32) {
	// 每个worker打开自己的数据集副本
	dataset, err := OpenRasterDataset(imagePath)
	if err != nil {
		log.Printf("Worker %d failed to open dataset: %v", workerID, err)
		return
	}
	defer dataset.Close()

	for task := range tasks {
		if atomic.LoadInt32(cancelled) == 1 {
			return
		}

		// 使用worker自己的数据集读取
		tileData, err := dataset.ReadTile(task.Zoom, task.X, task.Y, gen.tileSize)

		results <- RasterTileResult{
			Zoom:  task.Zoom,
			X:     task.X,
			Y:     task.Y,
			Data:  tileData,
			Error: err,
		}
	}
}

// tileWriter 瓦片写入协程
func (gen *MBTilesGenerator) tileWriter(db *sql.DB, results <-chan RasterTileResult, totalTiles, errorCount, cancelled *int32, estimatedTotal int) error {
	// 开始事务
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

	batchSize := 0
	maxBatchSize := 1000

	for result := range results {
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

		tileY := result.Y

		if _, err := stmt.Exec(result.Zoom, result.X, tileY, result.Data); err != nil {
			log.Printf("Error writing tile %d/%d/%d: %v", result.Zoom, result.X, result.Y, err)
			atomic.AddInt32(errorCount, 1)
			continue
		}

		current := atomic.AddInt32(totalTiles, 1)
		batchSize++

		// 定期提交事务
		if batchSize >= maxBatchSize {
			if err := tx.Commit(); err != nil {
				return err
			}

			// 开始新事务
			tx, err = db.Begin()
			if err != nil {
				return err
			}

			stmt, err = tx.Prepare("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)")
			if err != nil {
				return err
			}

			batchSize = 0
		}

		// 定期输出进度和调用回调
		if current%100 == 0 {
			progress := float64(current) / float64(estimatedTotal)
			message := fmt.Sprintf("Progress: %d/%d tiles (%.2f%%)", current, estimatedTotal, progress*100)
			if gen.progressCallback != nil {
				if !gen.progressCallback(progress, message) {
					atomic.StoreInt32(cancelled, 1)
					return fmt.Errorf("operation cancelled by user")
				}
			}
		}
	}

	// 提交最后的事务
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
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
