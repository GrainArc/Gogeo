package Gogeo

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

// MBTilesGenerator MBTiles生成器
type MBTilesGenerator struct {
	dataset  *RasterDataset
	tileSize int
	minZoom  int
	maxZoom  int
	useTMS   bool
}

// MBTilesOptions MBTiles生成选项
type MBTilesOptions struct {
	TileSize int               // 瓦片大小，默认256
	MinZoom  int               // 最小缩放级别，默认0
	MaxZoom  int               // 最大缩放级别，默认18
	Metadata map[string]string // 自定义元数据
	UseTMS   bool
}

// NewMBTilesGenerator 创建MBTiles生成器
func NewMBTilesGenerator(imagePath string, options *MBTilesOptions) (*MBTilesGenerator, error) {
	// 打开栅格数据集
	dataset, err := OpenRasterDataset(imagePath)
	if err != nil {
		return nil, err
	}

	// 设置默认选项
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
		dataset:  dataset,
		tileSize: options.TileSize,
		minZoom:  options.MinZoom,
		maxZoom:  options.MaxZoom,
		useTMS:   options.UseTMS,
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

// generateTiles 生成所有瓦片
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

	// 遍历缩放级别
	for zoom := gen.minZoom; zoom <= gen.maxZoom; zoom++ {
		minTileX, minTileY, maxTileX, maxTileY := gen.dataset.GetTileRange(zoom)

		tileCount := (maxTileX - minTileX + 1) * (maxTileY - minTileY + 1)
		log.Printf("Generating zoom level %d: tiles %d-%d, %d-%d (total: %d)",
			zoom, minTileX, maxTileX, minTileY, maxTileY, tileCount)

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
				if gen.useTMS {
					tileY = (1 << uint(zoom)) - 1 - y
				}

				if _, err := stmt.Exec(zoom, x, tileY, tileData); err != nil {
					return err
				}

				totalTiles++

				// 定期输出进度
				if totalTiles%100 == 0 {
					log.Printf("Generated %d tiles", totalTiles)
				}
			}
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Successfully generated %d tiles", totalTiles)
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
