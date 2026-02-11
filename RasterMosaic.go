// RasterMosaic.go
package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"fmt"
	"runtime"
)

// ResampleMethod 重采样方法
type ResampleMethod int

const (
	ResampleNearest     ResampleMethod = 0
	ResampleBilinear    ResampleMethod = 1
	ResampleCubic       ResampleMethod = 2
	ResampleCubicSpline ResampleMethod = 3
	ResampleLanczos     ResampleMethod = 4
)

// MosaicOptions 镶嵌选项
type MosaicOptions struct {
	ForceBandMatch bool           // 强制波段匹配（删除多余波段）
	ResampleMethod ResampleMethod // 重采样方法
	NoDataValue    float64        // 输出NoData值
	HasNoData      bool           // 是否设置NoData
	NumThreads     int            // 并行线程数，0表示自动
}

// DefaultMosaicOptions 默认镶嵌选项
func DefaultMosaicOptions() *MosaicOptions {
	return &MosaicOptions{
		ForceBandMatch: false,
		ResampleMethod: ResampleBilinear,
		NoDataValue:    0,
		HasNoData:      false,
		NumThreads:     0,
	}
}

// MosaicInfo 镶嵌信息
type MosaicInfo struct {
	MinX, MinY, MaxX, MaxY float64
	ResX, ResY             float64
	Width, Height          int
	BandCount              int
	DataType               string
	Projection             string
}

// MosaicDatasets 镶嵌多个栅格数据集
func MosaicDatasets(datasets []*RasterDataset, options *MosaicOptions) (*RasterDataset, error) {
	if len(datasets) == 0 {
		return nil, fmt.Errorf("no datasets provided")
	}

	if len(datasets) == 1 {
		// 单个数据集，直接返回副本
		return cloneDataset(datasets[0])
	}

	if options == nil {
		options = DefaultMosaicOptions()
	}

	// 准备C数据集数组
	cDatasets := make([]C.GDALDatasetH, len(datasets))
	for i, ds := range datasets {
		if ds == nil {
			return nil, fmt.Errorf("dataset at index %d is nil", i)
		}
		cDatasets[i] = ds.GetActiveDataset()
		if cDatasets[i] == nil {
			return nil, fmt.Errorf("dataset at index %d has nil handle", i)
		}
	}

	// 准备选项
	cOptions := C.MosaicOptions{
		forceBandMatch: boolToInt(options.ForceBandMatch),
		resampleMethod: C.int(options.ResampleMethod),
		noDataValue:    C.double(options.NoDataValue),
		hasNoData:      boolToInt(options.HasNoData),
		numThreads:     C.int(options.NumThreads),
	}

	// 错误消息缓冲区
	errorMsg := make([]C.char, 1024)

	// 执行镶嵌
	gdalMutex.Lock()
	resultDS := C.mosaicDatasets(&cDatasets[0], C.int(len(cDatasets)), &cOptions, &errorMsg[0])
	gdalMutex.Unlock()

	if resultDS == nil {
		return nil, fmt.Errorf("mosaic failed: %s", C.GoString(&errorMsg[0]))
	}

	// 获取结果信息
	width := int(C.GDALGetRasterXSize(resultDS))
	height := int(C.GDALGetRasterYSize(resultDS))
	bandCount := int(C.GDALGetRasterCount(resultDS))

	var geoTransform [6]C.double
	C.GDALGetGeoTransform(resultDS, &geoTransform[0])

	projection := C.GoString(C.GDALGetProjectionRef(resultDS))

	minX := float64(geoTransform[0])
	maxY := float64(geoTransform[3])
	maxX := minX + float64(width)*float64(geoTransform[1])
	minY := maxY + float64(height)*float64(geoTransform[5])

	rd := &RasterDataset{
		dataset:       resultDS,
		warpedDS:      nil,
		width:         width,
		height:        height,
		bandCount:     bandCount,
		bounds:        [4]float64{minX, minY, maxX, maxY},
		projection:    projection,
		isReprojected: false,
		hasGeoInfo:    true,
	}

	runtime.SetFinalizer(rd, (*RasterDataset).Close)

	return rd, nil
}

// MosaicFiles 从文件路径镶嵌多个栅格
func MosaicFiles(filePaths []string, options *MosaicOptions) (*RasterDataset, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no file paths provided")
	}

	// 打开所有数据集
	datasets := make([]*RasterDataset, 0, len(filePaths))
	for _, path := range filePaths {
		ds, err := OpenRasterDataset(path, false)
		if err != nil {
			// 关闭已打开的数据集
			for _, openedDS := range datasets {
				openedDS.Close()
			}
			return nil, fmt.Errorf("failed to open %s: %w", path, err)
		}
		datasets = append(datasets, ds)
	}

	// 执行镶嵌
	result, err := MosaicDatasets(datasets, options)

	// 关闭输入数据集
	for _, ds := range datasets {
		ds.Close()
	}

	return result, err
}

// GetMosaicInfo 获取镶嵌预览信息（不执行实际镶嵌）
func GetMosaicInfo(datasets []*RasterDataset, options *MosaicOptions) (*MosaicInfo, error) {
	if len(datasets) == 0 {
		return nil, fmt.Errorf("no datasets provided")
	}

	if options == nil {
		options = DefaultMosaicOptions()
	}

	// 准备C数据集数组
	cDatasets := make([]C.GDALDatasetH, len(datasets))
	for i, ds := range datasets {
		if ds == nil {
			return nil, fmt.Errorf("dataset at index %d is nil", i)
		}
		cDatasets[i] = ds.GetActiveDataset()
	}

	// 准备选项
	cOptions := C.MosaicOptions{
		forceBandMatch: boolToInt(options.ForceBandMatch),
		resampleMethod: C.int(options.ResampleMethod),
		noDataValue:    C.double(options.NoDataValue),
		hasNoData:      boolToInt(options.HasNoData),
		numThreads:     C.int(options.NumThreads),
	}

	errorMsg := make([]C.char, 1024)

	gdalMutex.Lock()
	cInfo := C.calculateMosaicInfo(&cDatasets[0], C.int(len(cDatasets)), &cOptions, &errorMsg[0])
	gdalMutex.Unlock()

	if cInfo == nil {
		return nil, fmt.Errorf("failed to calculate mosaic info: %s", C.GoString(&errorMsg[0]))
	}
	defer C.freeMosaicInfo(cInfo)

	info := &MosaicInfo{
		MinX:       float64(cInfo.minX),
		MinY:       float64(cInfo.minY),
		MaxX:       float64(cInfo.maxX),
		MaxY:       float64(cInfo.maxY),
		ResX:       float64(cInfo.resX),
		ResY:       float64(cInfo.resY),
		Width:      int(cInfo.width),
		Height:     int(cInfo.height),
		BandCount:  int(cInfo.bandCount),
		DataType:   C.GoString(C.GDALGetDataTypeName(cInfo.dataType)),
		Projection: C.GoString(&cInfo.projection[0]),
	}

	return info, nil
}

// MosaicToFile 镶嵌并直接保存到文件
func MosaicToFile(datasets []*RasterDataset, outputPath string, format string, options *MosaicOptions) error {
	result, err := MosaicDatasets(datasets, options)
	if err != nil {
		return err
	}
	defer result.Close()

	return result.ExportToFile(outputPath, format, nil)
}

// MosaicFilesToFile 从文件镶嵌并保存
func MosaicFilesToFile(inputPaths []string, outputPath string, format string, options *MosaicOptions) error {
	result, err := MosaicFiles(inputPaths, options)
	if err != nil {
		return err
	}
	defer result.Close()

	return result.ExportToFile(outputPath, format, nil)
}

// cloneDataset 克隆数据集
func cloneDataset(src *RasterDataset) (*RasterDataset, error) {
	srcDS := src.GetActiveDataset()
	if srcDS == nil {
		return nil, fmt.Errorf("source dataset is nil")
	}

	driver := C.GDALGetDriverByName(C.CString("MEM"))
	if driver == nil {
		return nil, fmt.Errorf("MEM driver not available")
	}

	gdalMutex.Lock()
	newDS := C.GDALCreateCopy(driver, C.CString(""), srcDS, 0, nil, nil, nil)
	gdalMutex.Unlock()

	if newDS == nil {
		return nil, fmt.Errorf("failed to clone dataset")
	}

	rd := &RasterDataset{
		dataset:       newDS,
		warpedDS:      nil,
		width:         src.width,
		height:        src.height,
		bandCount:     src.bandCount,
		bounds:        src.bounds,
		projection:    src.projection,
		isReprojected: src.isReprojected,
		hasGeoInfo:    src.hasGeoInfo,
	}

	runtime.SetFinalizer(rd, (*RasterDataset).Close)
	return rd, nil
}

// boolToInt 布尔转C int
func boolToInt(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// ==================== 高级镶嵌选项 ====================

// MosaicWithBlending 带融合的镶嵌（重叠区域渐变过渡）
type BlendingOptions struct {
	MosaicOptions
	BlendDistance int  // 融合距离（像素）
	BlendMode     int  // 融合模式: 0=线性, 1=余弦
	UseFeathering bool // 是否使用羽化
}

// DefaultBlendingOptions 默认融合选项
func DefaultBlendingOptions() *BlendingOptions {
	return &BlendingOptions{
		MosaicOptions: *DefaultMosaicOptions(),
		BlendDistance: 10,
		BlendMode:     0,
		UseFeathering: true,
	}
}

// MosaicWithPriority 带优先级的镶嵌
type PriorityMosaicOptions struct {
	MosaicOptions
	Priorities []int // 每个数据集的优先级，数值越大优先级越高
}

// MosaicDatasetsWithPriority 按优先级镶嵌
func MosaicDatasetsWithPriority(datasets []*RasterDataset, options *PriorityMosaicOptions) (*RasterDataset, error) {
	if options == nil || len(options.Priorities) != len(datasets) {
		return MosaicDatasets(datasets, nil)
	}

	// 按优先级排序（优先级低的先处理，高的后处理覆盖）
	type indexedDS struct {
		ds       *RasterDataset
		priority int
	}

	indexed := make([]indexedDS, len(datasets))
	for i, ds := range datasets {
		indexed[i] = indexedDS{ds: ds, priority: options.Priorities[i]}
	}

	// 排序
	for i := 0; i < len(indexed)-1; i++ {
		for j := i + 1; j < len(indexed); j++ {
			if indexed[i].priority > indexed[j].priority {
				indexed[i], indexed[j] = indexed[j], indexed[i]
			}
		}
	}

	// 重新排列数据集
	sortedDS := make([]*RasterDataset, len(datasets))
	for i, item := range indexed {
		sortedDS[i] = item.ds
	}

	return MosaicDatasets(sortedDS, &options.MosaicOptions)
}

// ==================== 批量处理 ====================

// MosaicBatch 批量镶嵌结果
type MosaicBatchResult struct {
	OutputPath string
	Error      error
}

// MosaicBatchConfig 批量镶嵌配置
type MosaicBatchConfig struct {
	InputGroups  [][]string     // 输入文件组
	OutputPaths  []string       // 输出路径
	OutputFormat string         // 输出格式
	Options      *MosaicOptions // 镶嵌选项
	MaxParallel  int            // 最大并行数
}

// MosaicBatch 批量执行镶嵌
func MosaicBatch(config *MosaicBatchConfig) []MosaicBatchResult {
	if config == nil || len(config.InputGroups) == 0 {
		return nil
	}

	if len(config.InputGroups) != len(config.OutputPaths) {
		return []MosaicBatchResult{{Error: fmt.Errorf("input groups and output paths count mismatch")}}
	}

	results := make([]MosaicBatchResult, len(config.InputGroups))

	// 简单串行处理（GDAL不是完全线程安全的）
	for i, group := range config.InputGroups {
		results[i].OutputPath = config.OutputPaths[i]
		err := MosaicFilesToFile(group, config.OutputPaths[i], config.OutputFormat, config.Options)
		results[i].Error = err
	}

	return results
}

// ==================== 验证函数 ====================

// ValidateMosaicInputs 验证镶嵌输入
func ValidateMosaicInputs(datasets []*RasterDataset, options *MosaicOptions) error {
	if len(datasets) == 0 {
		return fmt.Errorf("no datasets provided")
	}

	if options == nil {
		options = DefaultMosaicOptions()
	}

	// 获取参考信息
	refDS := datasets[0]
	refBandCount := refDS.GetBandCount()

	refBandInfo, err := refDS.GetBandInfo(1)
	if err != nil {
		return fmt.Errorf("failed to get band info from reference dataset: %w", err)
	}
	refDataType := refBandInfo.DataType

	// 检查所有数据集
	for i, ds := range datasets {
		if ds == nil {
			return fmt.Errorf("dataset at index %d is nil", i)
		}

		// 检查波段数
		bandCount := ds.GetBandCount()
		if !options.ForceBandMatch && bandCount != refBandCount {
			return fmt.Errorf("band count mismatch: dataset %d has %d bands, expected %d (use ForceBandMatch option)", i, bandCount, refBandCount)
		}

		// 检查数据类型
		bandInfo, err := ds.GetBandInfo(1)
		if err != nil {
			return fmt.Errorf("failed to get band info from dataset %d: %w", i, err)
		}

		if bandInfo.DataType != refDataType {
			return fmt.Errorf("data type mismatch: dataset %d has type %s, expected %s", i, bandInfo.DataType.String(), refDataType.String())
		}
	}

	return nil
}

// EstimateMosaicSize 估算镶嵌结果大小（字节）
func EstimateMosaicSize(datasets []*RasterDataset, options *MosaicOptions) (int64, error) {
	info, err := GetMosaicInfo(datasets, options)
	if err != nil {
		return 0, err
	}

	// 获取数据类型大小
	bytesPerPixel := 1 // 默认
	if len(datasets) > 0 {
		bandInfo, err := datasets[0].GetBandInfo(1)
		if err == nil {
			bytesPerPixel = bandInfo.DataType.GetBytesPerPixel()
		}
	}

	size := int64(info.Width) * int64(info.Height) * int64(info.BandCount) * int64(bytesPerPixel)
	return size, nil
}
