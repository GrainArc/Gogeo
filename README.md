# Gogeo - High-Performance GIS Spatial Analysis Library for Go

[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.20-blue.svg)](https://golang.org/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![GDAL](https://img.shields.io/badge/GDAL-%3E%3D%203.11-orange.svg)](https://gdal.org/)

Gogeo is a high-performance Go GIS spatial analysis library built on GDAL/OGR, designed for large-scale geospatial data processing. It provides comprehensive spatial analysis capabilities through parallel computing, tile-based processing, and precision control.

## ✨ Key Features

### 🚀 High-Performance Parallel Computing
- **Tile-based Processing**: Automatically splits large datasets into tiles for parallel processing
- **Multi-threaded Worker Pool**: Configurable concurrent worker threads
- **Memory Optimization**: Smart memory management and resource cleanup
- **Progress Monitoring**: Real-time progress callbacks and user cancellation support

### 🎯 Complete Spatial Analysis Operations
- **Clip**: Clip one layer with another layer
- **Erase**: Remove overlapping parts from input layer
- **Identity**: Preserve input features and add overlapping attributes
- **Intersect**: Calculate intersection of two layers
- **SymDifference**: Calculate symmetric difference of two layers
- **Union**: Calculate union of two layers
- **Update**: Update one layer with another layer

### 📁 Comprehensive Data I/O Support
- **PostGIS Database**: Read from and write to PostGIS databases
- **Shapefile**: Support for ESRI Shapefile format
- **File Geodatabase**: Support for ESRI File Geodatabase (.gdb)
- **Format Conversion**: Convert between different geospatial formats
- **Layer Management**: List layers, get layer information, and metadata

### 🔧 Advanced Features
- **Geometry Precision Control**: Configurable geometry precision grid
- **Field Management**: Smart field mapping and conflict resolution
- **Spatial Indexing**: Automatic spatial index optimization for query performance
- **Boundary Processing**: Intelligent boundary feature deduplication
- **Resource Management**: Automatic cleanup with finalizers

## 📦 Installation

### Prerequisites
Ensure GDAL development libraries are installed on your system:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install libgdal-dev gdal-bin
```

**CentOS/RHEL:**
```bash
sudo yum install gdal-devel gdal
```

**macOS:**
```bash
brew install gdal
```

**Windows:**
Download and install [OSGeo4W](https://trac.osgeo.org/osgeo4w/) or [GDAL Windows binaries](https://www.gisinternals.com/)

### Install Gogeo
```bash
go get github.com/fmecool/Gogeo
```

## 🚀 Quick Start

### Basic Usage Example

```go
package main

import (
   "fmt"
   "log"
   "path/filepath"
   "runtime"
   "time"

   "github.com/fmecool/Gogeo" // 根据您的实际包路径调整
)

func main() {
   // 设置输入文件路径
   shpFile1 := "data/layer1.shp"  // 第一个shapefile路径
   shpFile2 := "data/layer2.shp"  // 第二个shapefile路径
   outputFile := "output/intersection_result.shp" // 输出文件路径

   fmt.Println("开始空间相交分析测试...")
   fmt.Printf("输入文件1: %s\n", shpFile1)
   fmt.Printf("输入文件2: %s\n", shpFile2)
   fmt.Printf("输出文件: %s\n", outputFile)

   // 执行空间相交分析
   err := performSpatialIntersectionTest(shpFile1, shpFile2, outputFile)
   if err != nil {
      log.Fatalf("空间相交分析失败: %v", err)
   }

   fmt.Println("空间相交分析完成!")
}

func performSpatialIntersectionTest(shpFile1, shpFile2, outputFile string) error {
   startTime := time.Now()

   // 1. 读取第一个shapefile
   fmt.Println("正在读取第一个shapefile...")
   reader1, err := Gogeo.NewFileGeoReader(shpFile1)
   if err != nil {
      return fmt.Errorf("创建第一个文件读取器失败: %v", err)
   }

   layer1, err := reader1.ReadShapeFile()
   if err != nil {
      return fmt.Errorf("读取第一个shapefile失败: %v", err)
   }

   // 打印第一个图层信息
   fmt.Println("第一个图层信息:")
   layer1.PrintLayerInfo()

   // 2. 读取第二个shapefile
   fmt.Println("\n正在读取第二个shapefile...")
   reader2, err := Gogeo.NewFileGeoReader(shpFile2)
   if err != nil {
      layer1.Close()
      return fmt.Errorf("创建第二个文件读取器失败: %v", err)
   }

   layer2, err := reader2.ReadShapeFile()
   if err != nil {
      layer1.Close()
      return fmt.Errorf("读取第二个shapefile失败: %v", err)
   }

   // 打印第二个图层信息
   fmt.Println("第二个图层信息:")
   layer2.PrintLayerInfo()

   // 3. 配置并行相交分析参数
   config := &Gogeo.ParallelGeosConfig{
      TileCount:      4,                    // 4x4分块
      MaxWorkers:     runtime.NumCPU(),     // 使用所有CPU核心
      BufferDistance: 0.001,                // 分块缓冲距离
      IsMergeTile:    true,                 // 合并分块结果
      ProgressCallback: progressCallback,   // 进度回调函数
      PrecisionConfig: &Gogeo.GeometryPrecisionConfig{
         Enabled:       true,
         GridSize:      0.0001,  // 几何精度网格大小
         PreserveTopo:  true,    // 保持拓扑
         KeepCollapsed: false,   // 不保留退化几何
      },
   }

   // 4. 选择字段合并策略
   strategy := Gogeo.MergeWithPrefix // 使用前缀区分字段来源

   fmt.Printf("\n开始执行空间相交分析...")
   fmt.Printf("分块配置: %dx%d, 工作线程: %d\n",
      config.TileCount, config.TileCount, config.MaxWorkers)
   fmt.Printf("字段合并策略: %s\n", strategy.String())

   // 5. 执行空间相交分析
   result, err := Gogeo.SpatialIntersectionAnalysis(layer1, layer2, strategy, config)
   if err != nil {
      return fmt.Errorf("空间相交分析执行失败: %v", err)
   }

   analysisTime := time.Since(startTime)
   fmt.Printf("\n相交分析完成! 耗时: %v\n", analysisTime)
   fmt.Printf("结果要素数量: %d\n", result.ResultCount)

   // 6. 将结果写出为shapefile
   fmt.Println("正在写出结果到shapefile...")
   writeStartTime := time.Now()

   // 获取输出文件的图层名称（不含扩展名）
   layerName := getFileNameWithoutExt(outputFile)

   err = Gogeo.WriteShapeFileLayer(result.OutputLayer, outputFile, layerName, true)
   if err != nil {
      result.OutputLayer.Close()
      return fmt.Errorf("写出shapefile失败: %v", err)
   }

   writeTime := time.Since(writeStartTime)
   totalTime := time.Since(startTime)

   fmt.Printf("结果写出完成! 耗时: %v\n", writeTime)
   fmt.Printf("总耗时: %v\n", totalTime)
   fmt.Printf("输出文件: %s\n", outputFile)

   // 7. 验证输出文件
   err = verifyOutputFile(outputFile)
   if err != nil {
      fmt.Printf("警告: 输出文件验证失败: %v\n", err)
   } else {
      fmt.Println("输出文件验证成功!")
   }

   // 清理资源
   result.OutputLayer.Close()

   return nil
}

// progressCallback 进度回调函数
func progressCallback(complete float64, message string) bool {
   // 显示进度信息
   fmt.Printf("\r进度: %.1f%% - %s", complete*100, message)

   // 如果进度完成，换行
   if complete >= 1.0 {
      fmt.Println()
   }

   // 返回true继续执行，返回false取消执行
   return true
}

// getFileNameWithoutExt 获取不含扩展名的文件名
func getFileNameWithoutExt(filePath string) string {
   fileName := filepath.Base(filePath)
   return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}

// verifyOutputFile 验证输出文件
func verifyOutputFile(filePath string) error {
   // 读取输出文件验证
   reader, err := Gogeo.NewFileGeoReader(filePath)
   if err != nil {
      return fmt.Errorf("无法读取输出文件: %v", err)
   }

   layer, err := reader.ReadShapeFile()
   if err != nil {
      return fmt.Errorf("无法读取输出图层: %v", err)
   }
   defer layer.Close()

   // 打印输出图层信息
   fmt.Println("\n输出图层信息:")
   layer.PrintLayerInfo()

   // 检查要素数量
   featureCount := layer.GetFeatureCount()
   if featureCount == 0 {
      return fmt.Errorf("输出文件中没有要素")
   }

   fmt.Printf("验证通过: 输出文件包含 %d 个要素\n", featureCount)
   return nil
}

// 高级测试函数：测试不同的字段合并策略
func testDifferentStrategies(shpFile1, shpFile2 string) error {
   strategies := []Gogeo.FieldMergeStrategy{
      Gogeo.UseTable1Fields,
      Gogeo.UseTable2Fields,
      Gogeo.MergePreferTable1,
      Gogeo.MergePreferTable2,
      Gogeo.MergeWithPrefix,
   }

   config := &Gogeo.ParallelGeosConfig{
      TileCount:      2,
      MaxWorkers:     runtime.NumCPU(),
      BufferDistance: 0.001,
      IsMergeTile:    true,
      ProgressCallback: progressCallback,
   }

   for i, strategy := range strategies {
      fmt.Printf("\n=== 测试策略 %d: %s ===\n", i+1, strategy.String())

      outputFile := fmt.Sprintf("output/test_strategy_%d.shp", i+1)

      // 读取图层
      layer1, err := Gogeo.ReadShapeFileLayer(shpFile1)
      if err != nil {
         return err
      }

      layer2, err := Gogeo.ReadShapeFileLayer(shpFile2)
      if err != nil {
         layer1.Close()
         return err
      }

      // 执行分析
      result, err := Gogeo.SpatialIntersectionAnalysis(layer1, layer2, strategy, config)
      if err != nil {
         return fmt.Errorf("策略 %s 执行失败: %v", strategy.String(), err)
      }

      // 写出结果
      layerName := fmt.Sprintf("strategy_%d", i+1)
      err = Gogeo.WriteShapeFileLayer(result.OutputLayer, outputFile, layerName, true)
      if err != nil {
         result.OutputLayer.Close()
         return fmt.Errorf("策略 %s 写出失败: %v", strategy.String(), err)
      }

      fmt.Printf("策略 %s 完成，结果要素: %d，输出: %s\n",
         strategy.String(), result.ResultCount, outputFile)

      result.OutputLayer.Close()
   }

   return nil
}

// 性能测试函数
func performanceTest(shpFile1, shpFile2 string) error {
   fmt.Println("\n=== 性能测试 ===")

   // 测试不同的分块配置
   tileConfigs := []int{2, 4, 8}

   for _, tileCount := range tileConfigs {
      fmt.Printf("\n--- 测试分块配置: %dx%d ---\n", tileCount, tileCount)

      config := &Gogeo.ParallelGeosConfig{
         TileCount:      tileCount,
         MaxWorkers:     runtime.NumCPU(),
         BufferDistance: 0.001,
         IsMergeTile:    true,
         ProgressCallback: nil, // 性能测试时不显示进度
      }

      startTime := time.Now()

      // 读取图层
      layer1, err := Gogeo.ReadShapeFileLayer(shpFile1)
      if err != nil {
         return err
      }

      layer2, err := Gogeo.ReadShapeFileLayer(shpFile2)
      if err != nil {
         layer1.Close()
         return err
      }

      // 执行分析
      result, err := Gogeo.SpatialIntersectionAnalysis(layer1, layer2,
         Gogeo.MergePreferTable1, config)
      if err != nil {
         return err
      }

      duration := time.Since(startTime)
      fmt.Printf("分块 %dx%d: 耗时 %v, 结果要素 %d\n",
         tileCount, tileCount, duration, result.ResultCount)

      result.OutputLayer.Close()
   }

   return nil
}

```

### PostGIS Database Example

```go
// Configure PostGIS connection
config := &gogeo.PostGISConfig{
    Host:     "localhost",
    Port:     "5432",
    Database: "gis_db",
    User:     "postgres",
    Password: "password",
    Schema:   "public",
    Table:    "land_use",
}

// Create PostGIS reader
reader := gogeo.NewPostGISReader(config)
layer, err := reader.ReadGeometryTable()
if err != nil {
    log.Fatal("Failed to read PostGIS table:", err)
}
defer layer.Close()

// Print layer information
layer.PrintLayerInfo()
```

## 📚 API Documentation

### Core Data Structures

```go
// Parallel processing configuration
type ParallelGeosConfig struct {
    MaxWorkers       int                        // Maximum worker threads
    TileCount        int                        // Tile count (N×N grid)
    IsMergeTile      bool                       // Whether to merge tile results
    PrecisionConfig  *GeometryPrecisionConfig   // Geometry precision configuration
    ProgressCallback ProgressCallback           // Progress callback function
}

// Geometry precision configuration
type GeometryPrecisionConfig struct {
    Enabled           bool    // Enable precision control
    GridSize          float64 // Precision grid size
    PreserveCollinear bool    // Preserve collinear points
    KeepCollapsed     bool    // Keep collapsed geometries
}

// Analysis result
type GeosAnalysisResult struct {
    OutputLayer *GDALLayer // Output layer
    ResultCount int        // Number of result features
}

// PostGIS connection configuration
type PostGISConfig struct {
    Host     string // Database host
    Port     string // Database port
    Database string // Database name
    User     string // Username
    Password string // Password
    Schema   string // Schema name
    Table    string // Table name
}
```

### Spatial Analysis Functions

```go
// Spatial clip
func SpatialClipAnalysis(inputLayer, clipLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Spatial erase
func SpatialEraseAnalysis(inputLayer, eraseLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Spatial identity
func SpatialIdentityAnalysis(inputLayer, methodLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Spatial intersect
func SpatialIntersectAnalysis(inputLayer, intersectLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Spatial union
func SpatialUnionAnalysis(inputLayer, unionLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Symmetric difference
func SpatialSymDifferenceAnalysis(inputLayer, diffLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)

// Spatial update
func SpatialUpdateAnalysis(inputLayer, updateLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error)
```

### Data I/O Functions

```go
// Read functions
func ReadShapeFileLayer(filePath string, layerName ...string) (*GDALLayer, error)
func ReadGDBLayer(filePath string, layerName ...string) (*GDALLayer, error)
func ReadGeospatialFile(filePath string, layerName ...string) (*GDALLayer, error)

// Write functions
func WriteShapeFileLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error
func WriteGDBLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error
func WriteGeospatialFile(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error

// Utility functions
func ConvertFile(sourceFilePath, targetFilePath, sourceLayerName, targetLayerName string, overwrite bool) error
func CopyLayerToFile(sourceLayer *GDALLayer, targetFilePath, targetLayerName string, overwrite bool) error
```

### PostGIS Functions

```go
// Create PostGIS reader
func NewPostGISReader(config *PostGISConfig) *PostGISReader

// Read geometry table
func (r *PostGISReader) ReadGeometryTable() (*GDALLayer, error)

// Convenience function
func MakePGReader(table string) *PostGISReader
```

## 🎯 Use Cases

### 1. Large-Scale Land Use Analysis
```go
// Process provincial land use data with administrative boundaries
landUseResult, err := gogeo.SpatialIdentityAnalysis(landUseLayer, adminBoundaryLayer, &gogeo.ParallelGeosConfig{
    MaxWorkers:  12,
    TileCount:   6,
    IsMergeTile: true,
})
```

### 2. Environmental Impact Assessment
```go
// Calculate intersection of project impact area with protected areas
impactResult, err := gogeo.SpatialIntersectAnalysis(projectAreaLayer, protectedAreaLayer, config)
```

### 3. Urban Planning Analysis
```go
// Erase ecological protection areas from construction land
buildableResult, err := gogeo.SpatialEraseAnalysis(constructionLayer, ecologyLayer, config)
```

### 4. Data Format Migration
```go
// Migrate Shapefile data to PostGIS
sourceLayer, _ := gogeo.ReadShapeFileLayer("data.shp")
// Process and save to PostGIS (implementation depends on your PostGIS writer)
```

## ⚡ Performance Optimization

### 1. Parallel Configuration Recommendations
```go
// CPU-intensive tasks
config.MaxWorkers = runtime.NumCPU()

// I/O-intensive tasks  
config.MaxWorkers = runtime.NumCPU() * 2

// Large dataset processing
config.TileCount = 8  // 64 tiles
```

### 2. Memory Optimization
```go
// Enable result merging to reduce memory usage
config.IsMergeTile = true

// Appropriate precision settings to avoid over-computation
config.PrecisionConfig.GridSize = 0.001  // 1mm precision is usually sufficient
```

### 3. Data Preprocessing
- Build spatial indexes on data before analysis
- Remove invalid geometries
- Ensure consistent coordinate reference systems

## 🔧 Configuration Parameters

### MaxWorkers (Worker Thread Count)
- **Recommended**: 1-2 times CPU core count
- **Impact**: Too many causes context switching overhead, too few underutilizes CPU

### TileCount (Tile Count)
- **Recommended**: 4-8 (generates 16-64 tiles)
- **Impact**: Too many tiles increase boundary processing overhead, too few reduce parallelism

### GridSize (Precision Grid)
- **Recommended**: 0.001-0.0001 (1mm-0.1mm)
- **Impact**: Too large loses detail, too small increases computation overhead

## 🐛 Troubleshooting

### Common Issues

1. **GDAL Library Not Found**
   ```
   Error: cannot find GDAL library
   Solution: Ensure GDAL development libraries are properly installed and environment variables are set
   ```

2. **Out of Memory**
   ```
   Error: out of memory
   Solution: Reduce MaxWorkers or increase TileCount for finer granularity
   ```

3. **Invalid Geometry**
   ```
   Error: invalid geometry
   Solution: Enable precision control or preprocess data to remove invalid geometries
   ```

4. **PostGIS Connection Failed**
   ```
   Error: connection failed
   Solution: Check database connection parameters and ensure PostgreSQL/PostGIS is running
   ```

### Debugging Tips
```go
// Enable verbose logging
config.ProgressCallback = func(progress float64, message string) bool {
    log.Printf("Progress: %.2f%% - %s", progress*100, message)
    return true
}

// Check data validity
if layer.GetFeatureCount() == 0 {
    log.Println("Warning: Layer is empty")
}

// Print layer information
layer.PrintLayerInfo()
```

## 🤝 Contributing

We welcome contributions of all kinds!

### How to Contribute
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Environment Setup
```bash
# Clone repository
git clone https://github.com/yourusername/gogeo.git
cd gogeo

# Install dependencies
go mod tidy

# Run tests
go test ./...

# Build examples
go build ./examples/...
```

### Code Style
- Follow Go conventions and best practices
- Add comprehensive tests for new features
- Update documentation for API changes
- Ensure proper resource cleanup

## 📄 License

This project is licensed under the **GNU Affero General Public License v3.0** - see the [LICENSE](LICENSE) file for details.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

### What this means:
- ✅ **Free for open source projects**: You can use, modify, and distribute this code in open source projects
- ✅ **Educational and research use**: Free for academic and research purposes
- ❌ **No proprietary/commercial use**: You cannot use this code in closed-source commercial products
- 📋 **Share improvements**: Any modifications must be shared under the same license
- 🌐 **Network services**: If you run this as a web service, you must provide the source code

### Commercial Licensing
For commercial use or if you need a different license, please contact us at: your.email@example.com

We offer flexible commercial licenses for businesses that want to use Gogeo in proprietary applications.

### Full License Text
The complete AGPL-3.0 license text is available at: https://www.gnu.org/licenses/agpl-3.0.html


## 🙏 Acknowledgments

- [GDAL/OGR](https://gdal.org/) - Powerful geospatial data processing library
- [GEOS](https://trac.osgeo.org/geos/) - Geometry computation engine
- [PostGIS](https://postgis.net/) - Spatial database extension for PostgreSQL
- All contributors to the Go community

## 📞 Contact

- Project Homepage: https://github.com/fmecool/gogeo
- Issue Tracker: https://github.com/fmecool/gogeo/issues
- Email: 1131698384@qq.com
- qq交流群：767449805
## 🔗 Related Projects

- [GDAL Go Bindings](https://github.com/lukeroth/gdal) - Alternative GDAL bindings for Go
- [PostGIS](https://postgis.net/) - Spatial database extension
- [GEOS](https://trac.osgeo.org/geos/) - Geometry engine

---

⭐ If this project helps you, please give us a star!
