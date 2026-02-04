// RasterBandAdvanced.go
package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"fmt"
	"math"
	"unsafe"
)

// ==================== 波段数据读写 ====================

// ReadBandData 读取波段数据为float64数组
func (rd *RasterDataset) ReadBandData(bandIndex int) ([]float64, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return nil, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return nil, fmt.Errorf("failed to get band %d", bandIndex)
	}

	size := rd.width * rd.height
	buffer := make([]float64, size)

	err := C.GDALRasterIO(band, C.GF_Read,
		0, 0, C.int(rd.width), C.int(rd.height),
		unsafe.Pointer(&buffer[0]),
		C.int(rd.width), C.int(rd.height),
		C.GDT_Float64, 0, 0)

	if err != C.CE_None {
		return nil, fmt.Errorf("failed to read band data")
	}

	return buffer, nil
}

// WriteBandData 写入波段数据
// RasterDataset.go - 修复 WriteBandData

func (rd *RasterDataset) WriteBandData(bandIndex int, data []float64) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	expectedSize := rd.width * rd.height
	if len(data) != expectedSize {
		return fmt.Errorf("data size mismatch: expected %d, got %d", expectedSize, len(data))
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}

	// ★★★ 关键修复：使用 C 分配的内存 ★★★
	cData := C.malloc(C.size_t(len(data)) * C.size_t(unsafe.Sizeof(C.double(0))))
	if cData == nil {
		return fmt.Errorf("failed to allocate memory")
	}
	defer C.free(cData)

	// 复制数据到 C 内存
	cSlice := (*[1 << 30]C.double)(cData)[:len(data):len(data)]
	for i, v := range data {
		cSlice[i] = C.double(v)
	}

	err := C.GDALRasterIO(band, C.GF_Write,
		0, 0, C.int(rd.width), C.int(rd.height),
		cData,
		C.int(rd.width), C.int(rd.height),
		C.GDT_Float64, 0, 0)

	if err != C.CE_None {
		return fmt.Errorf("failed to write band data")
	}
	return nil
}

// ReadBandDataRect 读取波段矩形区域数据
func (rd *RasterDataset) ReadBandDataRect(bandIndex, x, y, width, height int) ([]float64, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return nil, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	// 边界检查
	if x < 0 || y < 0 || x+width > rd.width || y+height > rd.height {
		return nil, fmt.Errorf("rectangle out of bounds")
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return nil, fmt.Errorf("failed to get band %d", bandIndex)
	}

	size := width * height
	buffer := make([]float64, size)

	err := C.GDALRasterIO(band, C.GF_Read,
		C.int(x), C.int(y), C.int(width), C.int(height),
		unsafe.Pointer(&buffer[0]),
		C.int(width), C.int(height),
		C.GDT_Float64, 0, 0)

	if err != C.CE_None {
		return nil, fmt.Errorf("failed to read band data")
	}

	return buffer, nil
}

// WriteBandDataRect 写入波段矩形区域数据
func (rd *RasterDataset) WriteBandDataRect(bandIndex, x, y, width, height int, data []float64) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	expectedSize := width * height
	if len(data) != expectedSize {
		return fmt.Errorf("data size mismatch: expected %d, got %d", expectedSize, len(data))
	}
	if x < 0 || y < 0 || x+width > rd.width || y+height > rd.height {
		return fmt.Errorf("rectangle out of bounds")
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}
	err := C.GDALRasterIO(band, C.GF_Write,
		C.int(x), C.int(y), C.int(width), C.int(height),
		unsafe.Pointer(&data[0]),
		C.int(width), C.int(height),
		C.GDT_Float64, 0, 0)
	if err != C.CE_None {
		return fmt.Errorf("failed to write band data")
	}
	return nil
}

// ==================== 波段统计 ====================

// BandStatistics 波段统计信息
type BandStatistics struct {
	Min    float64
	Max    float64
	Mean   float64
	StdDev float64
}

// ComputeBandStatistics 计算波段统计信息
func (rd *RasterDataset) ComputeBandStatistics(bandIndex int, approxOK bool) (*BandStatistics, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return nil, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return nil, fmt.Errorf("failed to get band %d", bandIndex)
	}

	var minVal, maxVal, meanVal, stdDev C.double
	approx := C.int(0)
	if approxOK {
		approx = 1
	}

	err := C.GDALComputeRasterStatistics(band, approx, &minVal, &maxVal, &meanVal, &stdDev, nil, nil)
	if err != C.CE_None {
		return nil, fmt.Errorf("failed to compute statistics")
	}

	return &BandStatistics{
		Min:    float64(minVal),
		Max:    float64(maxVal),
		Mean:   float64(meanVal),
		StdDev: float64(stdDev),
	}, nil
}

// GetBandHistogram 获取波段直方图
func (rd *RasterDataset) GetBandHistogram(bandIndex int, buckets int, min, max float64) ([]uint64, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return nil, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return nil, fmt.Errorf("failed to get band %d", bandIndex)
	}

	histogram := make([]C.GUIntBig, buckets)

	err := C.GDALGetRasterHistogramEx(band,
		C.double(min), C.double(max),
		C.int(buckets), &histogram[0],
		0, 0, nil, nil)

	if err != C.CE_None {
		return nil, fmt.Errorf("failed to get histogram")
	}

	result := make([]uint64, buckets)
	for i := 0; i < buckets; i++ {
		result[i] = uint64(histogram[i])
	}

	return result, nil
}

// ==================== 波段运算 ====================

// BandMath 波段数学运算
type BandMathOp int

const (
	BandMathAdd BandMathOp = iota
	BandMathSubtract
	BandMathMultiply
	BandMathDivide
	BandMathMin
	BandMathMax
	BandMathPow
)

// BandMath 对两个波段进行数学运算
func (rd *RasterDataset) BandMath(band1, band2 int, op BandMathOp) ([]float64, error) {
	data1, err := rd.ReadBandData(band1)
	if err != nil {
		return nil, fmt.Errorf("failed to read band %d: %w", band1, err)
	}

	data2, err := rd.ReadBandData(band2)
	if err != nil {
		return nil, fmt.Errorf("failed to read band %d: %w", band2, err)
	}

	if len(data1) != len(data2) {
		return nil, fmt.Errorf("band size mismatch")
	}

	result := make([]float64, len(data1))

	for i := 0; i < len(data1); i++ {
		switch op {
		case BandMathAdd:
			result[i] = data1[i] + data2[i]
		case BandMathSubtract:
			result[i] = data1[i] - data2[i]
		case BandMathMultiply:
			result[i] = data1[i] * data2[i]
		case BandMathDivide:
			if data2[i] != 0 {
				result[i] = data1[i] / data2[i]
			} else {
				result[i] = math.NaN()
			}
		case BandMathMin:
			result[i] = math.Min(data1[i], data2[i])
		case BandMathMax:
			result[i] = math.Max(data1[i], data2[i])
		case BandMathPow:
			result[i] = math.Pow(data1[i], data2[i])
		}
	}

	return result, nil
}

// BandMathScalar 波段与标量运算
func (rd *RasterDataset) BandMathScalar(bandIndex int, scalar float64, op BandMathOp) ([]float64, error) {
	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to read band %d: %w", bandIndex, err)
	}

	result := make([]float64, len(data))

	for i := 0; i < len(data); i++ {
		switch op {
		case BandMathAdd:
			result[i] = data[i] + scalar
		case BandMathSubtract:
			result[i] = data[i] - scalar
		case BandMathMultiply:
			result[i] = data[i] * scalar
		case BandMathDivide:
			if scalar != 0 {
				result[i] = data[i] / scalar
			} else {
				result[i] = math.NaN()
			}
		case BandMathPow:
			result[i] = math.Pow(data[i], scalar)
		default:
			result[i] = data[i]
		}
	}

	return result, nil
}

// NormalizeBand 归一化波段数据到指定范围
func (rd *RasterDataset) NormalizeBand(bandIndex int, newMin, newMax float64) ([]float64, error) {
	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, err
	}

	// 找出当前最小最大值
	currentMin := data[0]
	currentMax := data[0]
	for _, v := range data {
		if !math.IsNaN(v) {
			if v < currentMin {
				currentMin = v
			}
			if v > currentMax {
				currentMax = v
			}
		}
	}

	// 归一化
	rangeOld := currentMax - currentMin
	rangeNew := newMax - newMin

	result := make([]float64, len(data))
	for i, v := range data {
		if math.IsNaN(v) || rangeOld == 0 {
			result[i] = v
		} else {
			result[i] = ((v-currentMin)/rangeOld)*rangeNew + newMin
		}
	}

	return result, nil
}

// ==================== NDVI等指数计算 ====================

// CalculateNDVI 计算归一化植被指数
// NDVI = (NIR - Red) / (NIR + Red)
func (rd *RasterDataset) CalculateNDVI(nirBand, redBand int) ([]float64, error) {
	nirData, err := rd.ReadBandData(nirBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read NIR band: %w", err)
	}

	redData, err := rd.ReadBandData(redBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read Red band: %w", err)
	}

	result := make([]float64, len(nirData))
	for i := 0; i < len(nirData); i++ {
		sum := nirData[i] + redData[i]
		if sum != 0 {
			result[i] = (nirData[i] - redData[i]) / sum
		} else {
			result[i] = math.NaN()
		}
	}

	return result, nil
}

// CalculateNDWI 计算归一化水体指数
// NDWI = (Green - NIR) / (Green + NIR)
func (rd *RasterDataset) CalculateNDWI(greenBand, nirBand int) ([]float64, error) {
	greenData, err := rd.ReadBandData(greenBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read Green band: %w", err)
	}

	nirData, err := rd.ReadBandData(nirBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read NIR band: %w", err)
	}

	result := make([]float64, len(greenData))
	for i := 0; i < len(greenData); i++ {
		sum := greenData[i] + nirData[i]
		if sum != 0 {
			result[i] = (greenData[i] - nirData[i]) / sum
		} else {
			result[i] = math.NaN()
		}
	}

	return result, nil
}

// CalculateEVI 计算增强植被指数
// EVI = 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)
func (rd *RasterDataset) CalculateEVI(nirBand, redBand, blueBand int) ([]float64, error) {
	nirData, err := rd.ReadBandData(nirBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read NIR band: %w", err)
	}

	redData, err := rd.ReadBandData(redBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read Red band: %w", err)
	}

	blueData, err := rd.ReadBandData(blueBand)
	if err != nil {
		return nil, fmt.Errorf("failed to read Blue band: %w", err)
	}

	result := make([]float64, len(nirData))
	for i := 0; i < len(nirData); i++ {
		denominator := nirData[i] + 6*redData[i] - 7.5*blueData[i] + 1
		if denominator != 0 {
			result[i] = 2.5 * (nirData[i] - redData[i]) / denominator
		} else {
			result[i] = math.NaN()
		}
	}

	return result, nil
}

// ==================== 波段合并与拆分 ====================

// MergeBandsToNewDataset 合并多个波段到新数据集
func (rd *RasterDataset) MergeBandsToNewDataset(bandIndices []int) (*RasterDataset, error) {
	if len(bandIndices) == 0 {
		return nil, fmt.Errorf("band indices cannot be empty")
	}

	// 验证波段索引
	for _, idx := range bandIndices {
		if idx < 1 || idx > rd.bandCount {
			return nil, fmt.Errorf("invalid band index: %d", idx)
		}
	}

	// 使用reorderBands创建新数据集
	cBandOrder := make([]C.int, len(bandIndices))
	for i, v := range bandIndices {
		cBandOrder[i] = C.int(v)
	}

	activeDS := rd.GetActiveDataset()
	newDS := C.reorderBands(activeDS, &cBandOrder[0], C.int(len(bandIndices)))
	if newDS == nil {
		return nil, fmt.Errorf("failed to merge bands")
	}

	newRD := &RasterDataset{
		dataset:       newDS,
		warpedDS:      nil,
		width:         rd.width,
		height:        rd.height,
		bandCount:     len(bandIndices),
		bounds:        rd.bounds,
		projection:    rd.projection,
		isReprojected: false,
		hasGeoInfo:    rd.hasGeoInfo,
	}

	return newRD, nil
}

// SplitBands 将多波段数据集拆分为单波段数据集数组
func (rd *RasterDataset) SplitBands() ([]*RasterDataset, error) {
	if rd.bandCount == 0 {
		return nil, fmt.Errorf("no bands to split")
	}

	results := make([]*RasterDataset, rd.bandCount)

	for i := 1; i <= rd.bandCount; i++ {
		newRD, err := rd.MergeBandsToNewDataset([]int{i})
		if err != nil {
			// 清理已创建的数据集
			for j := 0; j < i-1; j++ {
				if results[j] != nil {
					results[j].Close()
				}
			}
			return nil, fmt.Errorf("failed to split band %d: %w", i, err)
		}
		results[i-1] = newRD
	}

	return results, nil
}

// ==================== 波段掩膜操作 ====================

// ApplyMask 应用掩膜到波段
func (rd *RasterDataset) ApplyMask(bandIndex int, mask []bool, noDataValue float64) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	if len(mask) != rd.width*rd.height {
		return fmt.Errorf("mask size mismatch: expected %d, got %d", rd.width*rd.height, len(mask))
	}
	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return err
	}
	for i := 0; i < len(data); i++ {
		if !mask[i] {
			data[i] = noDataValue
		}
	}
	return rd.WriteBandData(bandIndex, data)
}

// CreateMaskFromNoData 从NoData值创建掩膜
func (rd *RasterDataset) CreateMaskFromNoData(bandIndex int) ([]bool, error) {
	info, err := rd.GetBandInfo(bandIndex)
	if err != nil {
		return nil, err
	}

	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, err
	}

	mask := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		if info.HasNoData {
			mask[i] = data[i] != info.NoDataValue && !math.IsNaN(data[i])
		} else {
			mask[i] = !math.IsNaN(data[i])
		}
	}

	return mask, nil
}

// CreateMaskFromThreshold 从阈值创建掩膜
func (rd *RasterDataset) CreateMaskFromThreshold(bandIndex int, minVal, maxVal float64) ([]bool, error) {
	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, err
	}

	mask := make([]bool, len(data))
	for i := 0; i < len(data); i++ {
		mask[i] = data[i] >= minVal && data[i] <= maxVal
	}

	return mask, nil
}

// ==================== 波段滤波操作 ====================

// FilterType 滤波类型
type FilterType int

const (
	FilterMean     FilterType = iota // 均值滤波
	FilterMedian                     // 中值滤波
	FilterGaussian                   // 高斯滤波
	FilterSobel                      // Sobel边缘检测
	FilterLaplace                    // 拉普拉斯滤波
	FilterMin                        // 最小值滤波
	FilterMax                        // 最大值滤波
)

// ApplyFilter 应用滤波器到波段
func (rd *RasterDataset) ApplyFilter(bandIndex int, filterType FilterType, kernelSize int) ([]float64, error) {
	if kernelSize%2 == 0 {
		return nil, fmt.Errorf("kernel size must be odd")
	}

	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, err
	}

	result := make([]float64, len(data))
	copy(result, data)

	halfKernel := kernelSize / 2

	for y := halfKernel; y < rd.height-halfKernel; y++ {
		for x := halfKernel; x < rd.width-halfKernel; x++ {
			idx := y*rd.width + x

			// 收集邻域值
			neighbors := make([]float64, 0, kernelSize*kernelSize)
			for ky := -halfKernel; ky <= halfKernel; ky++ {
				for kx := -halfKernel; kx <= halfKernel; kx++ {
					nIdx := (y+ky)*rd.width + (x + kx)
					neighbors = append(neighbors, data[nIdx])
				}
			}

			switch filterType {
			case FilterMean:
				result[idx] = calculateMean(neighbors)
			case FilterMedian:
				result[idx] = calculateMedian(neighbors)
			case FilterMin:
				result[idx] = calculateMin(neighbors)
			case FilterMax:
				result[idx] = calculateMax(neighbors)
			case FilterGaussian:
				result[idx] = applyGaussianKernel(data, rd.width, x, y, kernelSize)
			case FilterSobel:
				result[idx] = applySobelKernel(data, rd.width, x, y)
			case FilterLaplace:
				result[idx] = applyLaplaceKernel(data, rd.width, x, y)
			}
		}
	}

	return result, nil
}

// 辅助函数：计算均值
func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	count := 0
	for _, v := range values {
		if !math.IsNaN(v) {
			sum += v
			count++
		}
	}
	if count == 0 {
		return math.NaN()
	}
	return sum / float64(count)
}

// 辅助函数：计算中值
func calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// 过滤NaN值
	valid := make([]float64, 0, len(values))
	for _, v := range values {
		if !math.IsNaN(v) {
			valid = append(valid, v)
		}
	}

	if len(valid) == 0 {
		return math.NaN()
	}

	// 简单排序
	for i := 0; i < len(valid)-1; i++ {
		for j := i + 1; j < len(valid); j++ {
			if valid[j] < valid[i] {
				valid[i], valid[j] = valid[j], valid[i]
			}
		}
	}

	mid := len(valid) / 2
	if len(valid)%2 == 0 {
		return (valid[mid-1] + valid[mid]) / 2
	}
	return valid[mid]
}

// 辅助函数：计算最小值
func calculateMin(values []float64) float64 {
	minVal := math.MaxFloat64
	for _, v := range values {
		if !math.IsNaN(v) && v < minVal {
			minVal = v
		}
	}
	if minVal == math.MaxFloat64 {
		return math.NaN()
	}
	return minVal
}

// 辅助函数：计算最大值
func calculateMax(values []float64) float64 {
	maxVal := -math.MaxFloat64
	for _, v := range values {
		if !math.IsNaN(v) && v > maxVal {
			maxVal = v
		}
	}
	if maxVal == -math.MaxFloat64 {
		return math.NaN()
	}
	return maxVal
}

// 高斯核滤波
func applyGaussianKernel(data []float64, width, x, y, kernelSize int) float64 {
	sigma := float64(kernelSize) / 6.0
	halfKernel := kernelSize / 2

	sum := 0.0
	weightSum := 0.0

	for ky := -halfKernel; ky <= halfKernel; ky++ {
		for kx := -halfKernel; kx <= halfKernel; kx++ {
			idx := (y+ky)*width + (x + kx)
			if idx >= 0 && idx < len(data) && !math.IsNaN(data[idx]) {
				weight := math.Exp(-float64(kx*kx+ky*ky) / (2 * sigma * sigma))
				sum += data[idx] * weight
				weightSum += weight
			}
		}
	}

	if weightSum == 0 {
		return math.NaN()
	}
	return sum / weightSum
}

// Sobel边缘检测
func applySobelKernel(data []float64, width, x, y int) float64 {
	// Sobel X核
	sobelX := [][]float64{
		{-1, 0, 1},
		{-2, 0, 2},
		{-1, 0, 1},
	}

	// Sobel Y核
	sobelY := [][]float64{
		{-1, -2, -1},
		{0, 0, 0},
		{1, 2, 1},
	}

	gx := 0.0
	gy := 0.0

	for ky := -1; ky <= 1; ky++ {
		for kx := -1; kx <= 1; kx++ {
			idx := (y+ky)*width + (x + kx)
			if idx >= 0 && idx < len(data) && !math.IsNaN(data[idx]) {
				gx += data[idx] * sobelX[ky+1][kx+1]
				gy += data[idx] * sobelY[ky+1][kx+1]
			}
		}
	}

	return math.Sqrt(gx*gx + gy*gy)
}

// 拉普拉斯滤波
func applyLaplaceKernel(data []float64, width, x, y int) float64 {
	// 拉普拉斯核
	laplace := [][]float64{
		{0, 1, 0},
		{1, -4, 1},
		{0, 1, 0},
	}

	sum := 0.0
	for ky := -1; ky <= 1; ky++ {
		for kx := -1; kx <= 1; kx++ {
			idx := (y+ky)*width + (x + kx)
			if idx >= 0 && idx < len(data) && !math.IsNaN(data[idx]) {
				sum += data[idx] * laplace[ky+1][kx+1]
			}
		}
	}

	return sum
}

// ==================== 波段重分类 ====================

// ReclassifyRule 重分类规则
type ReclassifyRule struct {
	MinValue float64 // 最小值（包含）
	MaxValue float64 // 最大值（不包含）
	NewValue float64 // 新值
}

// ReclassifyBand 重分类波段
func (rd *RasterDataset) ReclassifyBand(bandIndex int, rules []ReclassifyRule, defaultValue float64) ([]float64, error) {
	data, err := rd.ReadBandData(bandIndex)
	if err != nil {
		return nil, err
	}

	result := make([]float64, len(data))

	for i, v := range data {
		if math.IsNaN(v) {
			result[i] = v
			continue
		}

		matched := false
		for _, rule := range rules {
			if v >= rule.MinValue && v < rule.MaxValue {
				result[i] = rule.NewValue
				matched = true
				break
			}
		}

		if !matched {
			result[i] = defaultValue
		}
	}

	return result, nil
}

// ==================== 波段导出 ====================

// ExportBandToFile 导出波段到文件
func (rd *RasterDataset) ExportBandToFile(bandIndex int, outputPath, format string) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}

	// 创建单波段数据集
	singleBandDS, err := rd.MergeBandsToNewDataset([]int{bandIndex})
	if err != nil {
		return fmt.Errorf("failed to extract band: %w", err)
	}
	defer singleBandDS.Close()

	// 获取驱动
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	driver := C.GDALGetDriverByName(cFormat)
	if driver == nil {
		return fmt.Errorf("unsupported format: %s", format)
	}

	// 创建输出文件
	cOutputPath := C.CString(outputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	outputDS := C.GDALCreateCopy(driver, cOutputPath, singleBandDS.GetActiveDataset(), 0, nil, nil, nil)
	if outputDS == nil {
		return fmt.Errorf("failed to create output file")
	}
	C.GDALClose(outputDS)

	return nil
}

// 辅助函数：颜色解释转字符串
func colorInterpToString(interp int) string {
	names := map[int]string{
		0:  "Undefined",
		1:  "Gray",
		2:  "Palette",
		3:  "Red",
		4:  "Green",
		5:  "Blue",
		6:  "Alpha",
		7:  "Hue",
		8:  "Saturation",
		9:  "Lightness",
		10: "Cyan",
		11: "Magenta",
		12: "Yellow",
		13: "Black",
	}
	if name, ok := names[interp]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", interp)
}

// ==================== 波段元数据操作 ====================

// SetBandMetadata 设置波段元数据
func (rd *RasterDataset) SetBandMetadata(bandIndex int, key, value string) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	err := C.GDALSetMetadataItem(C.GDALMajorObjectH(band), cKey, cValue, nil)
	if err != C.CE_None {
		return fmt.Errorf("failed to set metadata")
	}
	return nil
}

// GetBandMetadata 获取波段元数据
func (rd *RasterDataset) GetBandMetadata(bandIndex int, key string) (string, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return "", fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return "", fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return "", fmt.Errorf("failed to get band %d", bandIndex)
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	value := C.GDALGetMetadataItem(C.GDALMajorObjectH(band), cKey, nil)
	if value == nil {
		return "", fmt.Errorf("metadata key not found: %s", key)
	}

	return C.GoString(value), nil
}

// GetAllBandMetadata 获取波段所有元数据
func (rd *RasterDataset) GetAllBandMetadata(bandIndex int) (map[string]string, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return nil, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return nil, fmt.Errorf("failed to get band %d", bandIndex)
	}

	metadata := C.GDALGetMetadata(C.GDALMajorObjectH(band), nil)
	if metadata == nil {
		return make(map[string]string), nil
	}

	result := make(map[string]string)
	for i := 0; ; i++ {
		item := C.CSLGetField(metadata, C.int(i))
		if item == nil || C.GoString(item) == "" {
			break
		}

		itemStr := C.GoString(item)
		// 解析 "KEY=VALUE" 格式
		for j := 0; j < len(itemStr); j++ {
			if itemStr[j] == '=' {
				result[itemStr[:j]] = itemStr[j+1:]
				break
			}
		}
	}

	return result, nil
}

// ==================== 波段缩放与偏移 ====================

// SetBandScale 设置波段缩放因子
func (rd *RasterDataset) SetBandScale(bandIndex int, scale float64) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}

	err := C.GDALSetRasterScale(band, C.double(scale))
	if err != C.CE_None {
		return fmt.Errorf("failed to set scale")
	}

	return nil
}

// SetBandOffset 设置波段偏移量
func (rd *RasterDataset) SetBandOffset(bandIndex int, offset float64) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}
	err := C.GDALSetRasterOffset(band, C.double(offset))
	if err != C.CE_None {
		return fmt.Errorf("failed to set offset")
	}
	return nil
}

// GetBandScale 获取波段缩放因子
func (rd *RasterDataset) GetBandScale(bandIndex int) (float64, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return 0, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return 0, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return 0, fmt.Errorf("failed to get band %d", bandIndex)
	}

	var success C.int
	scale := C.GDALGetRasterScale(band, &success)

	return float64(scale), nil
}

// GetBandOffset 获取波段偏移量
func (rd *RasterDataset) GetBandOffset(bandIndex int) (float64, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return 0, fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return 0, fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return 0, fmt.Errorf("failed to get band %d", bandIndex)
	}

	var success C.int
	offset := C.GDALGetRasterOffset(band, &success)

	return float64(offset), nil
}

// ==================== 波段单位与描述 ====================

// SetBandUnitType 设置波段单位类型
func (rd *RasterDataset) SetBandUnitType(bandIndex int, unitType string) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}
	cUnitType := C.CString(unitType)
	defer C.free(unsafe.Pointer(cUnitType))
	err := C.GDALSetRasterUnitType(band, cUnitType)
	if err != C.CE_None {
		return fmt.Errorf("failed to set unit type")
	}
	return nil
}

// GetBandUnitType 获取波段单位类型
func (rd *RasterDataset) GetBandUnitType(bandIndex int) (string, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return "", fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return "", fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return "", fmt.Errorf("failed to get band %d", bandIndex)
	}

	unitType := C.GDALGetRasterUnitType(band)
	if unitType == nil {
		return "", nil
	}

	return C.GoString(unitType), nil
}

// SetBandDescription 设置波段描述
func (rd *RasterDataset) SetBandDescription(bandIndex int, description string) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}
	cDesc := C.CString(description)
	defer C.free(unsafe.Pointer(cDesc))
	C.GDALSetDescription(C.GDALMajorObjectH(band), cDesc)
	return nil
}

// GetBandDescription 获取波段描述
func (rd *RasterDataset) GetBandDescription(bandIndex int) (string, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return "", fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return "", fmt.Errorf("invalid band index: %d", bandIndex)
	}

	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return "", fmt.Errorf("failed to get band %d", bandIndex)
	}

	desc := C.GDALGetDescription(C.GDALMajorObjectH(band))
	if desc == nil {
		return "", nil
	}

	return C.GoString(desc), nil
}
