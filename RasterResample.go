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

// ==================== 栅格重采样 ====================

// ResampleOptions 重采样选项
type ResampleOptions struct {
	Method       ResampleMethod // 重采样方法
	TargetResX   float64        // 目标X分辨率（0表示使用缩放因子）
	TargetResY   float64        // 目标Y分辨率（0表示使用缩放因子）
	ScaleFactor  float64        // 缩放因子（当TargetRes为0时使用，>1放大，<1缩小）
	TargetWidth  int            // 目标宽度（0表示自动计算）
	TargetHeight int            // 目标高度（0表示自动计算）
	NoDataValue  float64        // NoData值
	HasNoData    bool           // 是否设置NoData
}

// DefaultResampleOptions 默认重采样选项
func DefaultResampleOptions() *ResampleOptions {
	return &ResampleOptions{
		Method:       ResampleBilinear,
		TargetResX:   0,
		TargetResY:   0,
		ScaleFactor:  1.0,
		TargetWidth:  0,
		TargetHeight: 0,
		NoDataValue:  0,
		HasNoData:    false,
	}
}

// ResampleInfo 重采样结果信息
type ResampleInfo struct {
	OriginalWidth  int
	OriginalHeight int
	OriginalResX   float64
	OriginalResY   float64
	TargetWidth    int
	TargetHeight   int
	TargetResX     float64
	TargetResY     float64
	BandCount      int
}

// Resample 对栅格数据集进行重采样
func (rd *RasterDataset) Resample(options *ResampleOptions) (*RasterDataset, error) {
	if rd == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	srcDS := rd.GetActiveDataset()
	if srcDS == nil {
		return nil, fmt.Errorf("source dataset handle is nil")
	}

	if options == nil {
		options = DefaultResampleOptions()
	}

	// 获取源数据集信息
	srcWidth := rd.width
	srcHeight := rd.height
	bandCount := rd.bandCount

	var srcGeoTransform [6]C.double
	gdalMutex.Lock()
	C.GDALGetGeoTransform(srcDS, &srcGeoTransform[0])
	gdalMutex.Unlock()

	srcResX := float64(srcGeoTransform[1])
	srcResY := float64(srcGeoTransform[5]) // 通常为负值

	// 计算目标尺寸和分辨率
	var targetWidth, targetHeight int
	var targetResX, targetResY float64

	if options.TargetWidth > 0 && options.TargetHeight > 0 {
		// 使用指定的目标尺寸
		targetWidth = options.TargetWidth
		targetHeight = options.TargetHeight
		targetResX = (float64(srcWidth) * srcResX) / float64(targetWidth)
		targetResY = (float64(srcHeight) * srcResY) / float64(targetHeight)
	} else if options.TargetResX > 0 && options.TargetResY > 0 {
		// 使用指定的目标分辨率
		targetResX = options.TargetResX
		targetResY = -options.TargetResY // 确保为负值
		if srcResY > 0 {
			targetResY = options.TargetResY
		}
		targetWidth = int(float64(srcWidth) * srcResX / targetResX)
		targetHeight = int(float64(srcHeight) * srcResY / targetResY)
		if targetWidth < 1 {
			targetWidth = 1
		}
		if targetHeight < 1 {
			targetHeight = 1
		}
	} else if options.ScaleFactor > 0 && options.ScaleFactor != 1.0 {
		// 使用缩放因子
		targetWidth = int(float64(srcWidth) * options.ScaleFactor)
		targetHeight = int(float64(srcHeight) * options.ScaleFactor)
		if targetWidth < 1 {
			targetWidth = 1
		}
		if targetHeight < 1 {
			targetHeight = 1
		}
		targetResX = srcResX / options.ScaleFactor
		targetResY = srcResY / options.ScaleFactor
	} else {
		// 无变化，返回克隆
		return cloneDataset(rd)
	}

	// 获取数据类型
	gdalMutex.Lock()
	srcBand := C.GDALGetRasterBand(srcDS, 1)
	dataType := C.GDALGetRasterDataType(srcBand)
	gdalMutex.Unlock()

	// 创建目标数据集
	driver := C.GDALGetDriverByName(C.CString("MEM"))
	if driver == nil {
		return nil, fmt.Errorf("MEM driver not available")
	}

	gdalMutex.Lock()
	dstDS := C.GDALCreate(driver, C.CString(""), C.int(targetWidth), C.int(targetHeight), C.int(bandCount), dataType, nil)
	gdalMutex.Unlock()

	if dstDS == nil {
		return nil, fmt.Errorf("failed to create target dataset")
	}

	// 设置地理变换
	dstGeoTransform := [6]C.double{
		srcGeoTransform[0],   // 左上角X
		C.double(targetResX), // 像素宽度
		srcGeoTransform[2],   // 旋转（通常为0）
		srcGeoTransform[3],   // 左上角Y
		srcGeoTransform[4],   // 旋转（通常为0）
		C.double(targetResY), // 像素高度（负值）
	}

	gdalMutex.Lock()
	C.GDALSetGeoTransform(dstDS, &dstGeoTransform[0])

	// 设置投影
	projection := C.GDALGetProjectionRef(srcDS)
	C.GDALSetProjection(dstDS, projection)
	gdalMutex.Unlock()

	// 设置NoData值
	if options.HasNoData {
		for i := 1; i <= bandCount; i++ {
			gdalMutex.Lock()
			dstBand := C.GDALGetRasterBand(dstDS, C.int(i))
			C.GDALSetRasterNoDataValue(dstBand, C.double(options.NoDataValue))
			gdalMutex.Unlock()
		}
	}

	// 执行重采样
	err := executeResample(srcDS, dstDS, options.Method, bandCount, srcWidth, srcHeight, targetWidth, targetHeight)
	if err != nil {
		C.GDALClose(dstDS)
		return nil, fmt.Errorf("resample failed: %w", err)
	}

	// 计算新边界
	minX := float64(dstGeoTransform[0])
	maxY := float64(dstGeoTransform[3])
	maxX := minX + float64(targetWidth)*float64(dstGeoTransform[1])
	minY := maxY + float64(targetHeight)*float64(dstGeoTransform[5])

	result := &RasterDataset{
		dataset:       dstDS,
		warpedDS:      nil,
		width:         targetWidth,
		height:        targetHeight,
		bandCount:     bandCount,
		bounds:        [4]float64{minX, minY, maxX, maxY},
		projection:    rd.projection,
		isReprojected: rd.isReprojected,
		hasGeoInfo:    rd.hasGeoInfo,
	}

	runtime.SetFinalizer(result, (*RasterDataset).Close)
	return result, nil
}

// executeResample 执行重采样操作
func executeResample(srcDS, dstDS C.GDALDatasetH, method ResampleMethod, bandCount, srcWidth, srcHeight, dstWidth, dstHeight int) error {
	// 获取重采样算法
	var resampleAlg C.GDALRIOResampleAlg
	switch method {
	case ResampleNearest:
		resampleAlg = C.GRIORA_NearestNeighbour
	case ResampleBilinear:
		resampleAlg = C.GRIORA_Bilinear
	case ResampleCubic:
		resampleAlg = C.GRIORA_Cubic
	case ResampleCubicSpline:
		resampleAlg = C.GRIORA_CubicSpline
	case ResampleLanczos:
		resampleAlg = C.GRIORA_Lanczos
	default:
		resampleAlg = C.GRIORA_Bilinear
	}

	// 逐波段处理
	for band := 1; band <= bandCount; band++ {
		gdalMutex.Lock()
		srcBand := C.GDALGetRasterBand(srcDS, C.int(band))
		dstBand := C.GDALGetRasterBand(dstDS, C.int(band))
		dataType := C.GDALGetRasterDataType(srcBand)
		dataSize := C.GDALGetDataTypeSizeBytes(dataType)
		gdalMutex.Unlock()

		// 分配缓冲区
		bufferSize := C.size_t(dstWidth) * C.size_t(dstHeight) * C.size_t(dataSize)
		buffer := C.malloc(bufferSize)
		if buffer == nil {
			return fmt.Errorf("failed to allocate buffer for band %d", band)
		}

		// 设置读取选项
		var rasterIOOptions C.GDALRasterIOExtraArg
		rasterIOOptions.nVersion = 1
		rasterIOOptions.eResampleAlg = resampleAlg
		rasterIOOptions.pfnProgress = nil
		rasterIOOptions.pProgressData = nil
		rasterIOOptions.bFloatingPointWindowValidity = 0

		// 从源波段读取并重采样
		gdalMutex.Lock()
		err := C.GDALRasterIOEx(
			srcBand,
			C.GF_Read,
			0, 0,
			C.int(srcWidth), C.int(srcHeight),
			buffer,
			C.int(dstWidth), C.int(dstHeight),
			dataType,
			0, 0,
			&rasterIOOptions,
		)
		gdalMutex.Unlock()

		if err != C.CE_None {
			C.free(buffer)
			return fmt.Errorf("failed to read and resample band %d", band)
		}

		// 写入目标波段
		gdalMutex.Lock()
		err = C.GDALRasterIO(
			dstBand,
			C.GF_Write,
			0, 0,
			C.int(dstWidth), C.int(dstHeight),
			buffer,
			C.int(dstWidth), C.int(dstHeight),
			dataType,
			0, 0,
		)
		gdalMutex.Unlock()

		C.free(buffer)

		if err != C.CE_None {
			return fmt.Errorf("failed to write band %d", band)
		}
	}

	return nil
}

// ResampleToResolution 重采样到指定分辨率
func (rd *RasterDataset) ResampleToResolution(resX, resY float64, method ResampleMethod) (*RasterDataset, error) {
	options := &ResampleOptions{
		Method:     method,
		TargetResX: resX,
		TargetResY: resY,
	}
	return rd.Resample(options)
}

// ResampleToSize 重采样到指定尺寸
func (rd *RasterDataset) ResampleToSize(width, height int, method ResampleMethod) (*RasterDataset, error) {
	options := &ResampleOptions{
		Method:       method,
		TargetWidth:  width,
		TargetHeight: height,
	}
	return rd.Resample(options)
}

// ResampleByFactor 按缩放因子重采样
func (rd *RasterDataset) ResampleByFactor(factor float64, method ResampleMethod) (*RasterDataset, error) {
	options := &ResampleOptions{
		Method:      method,
		ScaleFactor: factor,
	}
	return rd.Resample(options)
}

// ResampleToFile 重采样并保存到文件
func (rd *RasterDataset) ResampleToFile(outputPath string, format string, options *ResampleOptions) error {
	result, err := rd.Resample(options)
	if err != nil {
		return err
	}
	defer result.Close()

	return result.ExportToFile(outputPath, format, nil)
}

// GetResampleInfo 获取重采样预览信息（不执行实际重采样）
func (rd *RasterDataset) GetResampleInfo(options *ResampleOptions) (*ResampleInfo, error) {
	if rd == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if options == nil {
		options = DefaultResampleOptions()
	}

	srcDS := rd.GetActiveDataset()
	if srcDS == nil {
		return nil, fmt.Errorf("dataset handle is nil")
	}

	// 获取源信息
	srcWidth := rd.width
	srcHeight := rd.height

	var srcGeoTransform [6]C.double
	gdalMutex.Lock()
	C.GDALGetGeoTransform(srcDS, &srcGeoTransform[0])
	gdalMutex.Unlock()

	srcResX := float64(srcGeoTransform[1])
	srcResY := float64(srcGeoTransform[5])

	// 计算目标尺寸
	var targetWidth, targetHeight int
	var targetResX, targetResY float64

	if options.TargetWidth > 0 && options.TargetHeight > 0 {
		targetWidth = options.TargetWidth
		targetHeight = options.TargetHeight
		targetResX = (float64(srcWidth) * srcResX) / float64(targetWidth)
		targetResY = (float64(srcHeight) * srcResY) / float64(targetHeight)
	} else if options.TargetResX > 0 && options.TargetResY > 0 {
		targetResX = options.TargetResX
		targetResY = -options.TargetResY
		if srcResY > 0 {
			targetResY = options.TargetResY
		}
		targetWidth = int(float64(srcWidth) * srcResX / targetResX)
		targetHeight = int(float64(srcHeight) * srcResY / targetResY)
	} else if options.ScaleFactor > 0 {
		targetWidth = int(float64(srcWidth) * options.ScaleFactor)
		targetHeight = int(float64(srcHeight) * options.ScaleFactor)
		targetResX = srcResX / options.ScaleFactor
		targetResY = srcResY / options.ScaleFactor
	} else {
		targetWidth = srcWidth
		targetHeight = srcHeight
		targetResX = srcResX
		targetResY = srcResY
	}

	if targetWidth < 1 {
		targetWidth = 1
	}
	if targetHeight < 1 {
		targetHeight = 1
	}

	return &ResampleInfo{
		OriginalWidth:  srcWidth,
		OriginalHeight: srcHeight,
		OriginalResX:   srcResX,
		OriginalResY:   srcResY,
		TargetWidth:    targetWidth,
		TargetHeight:   targetHeight,
		TargetResX:     targetResX,
		TargetResY:     targetResY,
		BandCount:      rd.bandCount,
	}, nil
}

// EstimateResampleSize 估算重采样结果大小（字节）
func (rd *RasterDataset) EstimateResampleSize(options *ResampleOptions) (int64, error) {
	info, err := rd.GetResampleInfo(options)
	if err != nil {
		return 0, err
	}

	bytesPerPixel := 1
	bandInfo, err := rd.GetBandInfo(1)
	if err == nil {
		bytesPerPixel = bandInfo.DataType.GetBytesPerPixel()
	}

	size := int64(info.TargetWidth) * int64(info.TargetHeight) * int64(info.BandCount) * int64(bytesPerPixel)
	return size, nil
}

// ==================== 批量重采样 ====================

// ResampleBatchConfig 批量重采样配置
type ResampleBatchConfig struct {
	InputPaths   []string         // 输入文件路径
	OutputPaths  []string         // 输出文件路径
	OutputFormat string           // 输出格式
	Options      *ResampleOptions // 重采样选项
}

// ResampleBatchResult 批量重采样结果
type ResampleBatchResult struct {
	InputPath  string
	OutputPath string
	Error      error
}

// ResampleBatch 批量执行重采样
func ResampleBatch(config *ResampleBatchConfig) []ResampleBatchResult {
	if config == nil || len(config.InputPaths) == 0 {
		return nil
	}

	if len(config.InputPaths) != len(config.OutputPaths) {
		return []ResampleBatchResult{{Error: fmt.Errorf("input and output paths count mismatch")}}
	}

	results := make([]ResampleBatchResult, len(config.InputPaths))

	for i, inputPath := range config.InputPaths {
		results[i].InputPath = inputPath
		results[i].OutputPath = config.OutputPaths[i]

		// 打开数据集
		ds, err := OpenRasterDataset(inputPath, false)
		if err != nil {
			results[i].Error = fmt.Errorf("failed to open %s: %w", inputPath, err)
			continue
		}

		// 执行重采样并保存
		err = ds.ResampleToFile(config.OutputPaths[i], config.OutputFormat, config.Options)
		ds.Close()

		if err != nil {
			results[i].Error = fmt.Errorf("failed to resample %s: %w", inputPath, err)
		}
	}

	return results
}

// ResampleFile 从文件重采样并保存
func ResampleFile(inputPath, outputPath, format string, options *ResampleOptions) error {
	ds, err := OpenRasterDataset(inputPath, false)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer ds.Close()

	return ds.ResampleToFile(outputPath, format, options)
}
