// RasterBandCalculator.go
package Gogeo

/*
#include "osgeo_band_calc.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"math"
	"unsafe"
)

// BandCalculator 高性能波段计算器
type BandCalculator struct {
	rd *RasterDataset
}

// NewBandCalculator 创建波段计算器
func (rd *RasterDataset) NewBandCalculator() *BandCalculator {
	return &BandCalculator{rd: rd}
}

// Calculate 执行表达式计算
// 支持格式: "b1", "b2", "B1", "band1" 表示波段引用
// 支持运算符: +, -, *, /, ^ (幂运算)
// 支持函数: sqrt, abs, sin, cos, tan, log, log10, exp, floor, ceil, round, min, max, pow
// 支持比较: >, >=, <, <=, ==, !=
// 支持逻辑: &&, ||
// 示例: "(b1 - b2) / (b1 + b2)", "sqrt(b1^2 + b2^2)", "max(b1, b2) * 0.5"
func (bc *BandCalculator) Calculate(expression string) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}
	cExpr := C.CString(expression)
	defer C.free(unsafe.Pointer(cExpr))
	var outSize C.int
	result := C.calculateBandExpression(activeDS, cExpr, &outSize)
	if result == nil {
		return nil, fmt.Errorf("failed to calculate expression: %s", expression)
	}
	defer C.freeBandCalcResult(result)
	size := int(outSize)
	if size <= 0 {
		return nil, fmt.Errorf("invalid result size: %d", size)
	}
	goResult := make([]float64, size)
	// ★★★ 使用 unsafe.Slice（Go 1.17+）更安全 ★★★
	cSlice := unsafe.Slice((*C.double)(result), size)
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}
	return goResult, nil
}

// CalculateWithCondition 带条件的计算
// expression: 计算表达式
// condition: 条件表达式（满足条件才计算，否则返回noDataValue）
// 示例: expression="(b1-b2)/(b1+b2)", condition="b1 > 0 && b2 > 0"
func (bc *BandCalculator) CalculateWithCondition(expression, condition string, noDataValue float64) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cExpr := C.CString(expression)
	defer C.free(unsafe.Pointer(cExpr))

	var cCond *C.char
	if condition != "" {
		cCond = C.CString(condition)
		defer C.free(unsafe.Pointer(cCond))
	}

	var outSize C.int
	result := C.calculateBandExpressionWithCondition(activeDS, cExpr, cCond,
		C.double(noDataValue), &outSize)
	if result == nil {
		return nil, fmt.Errorf("failed to calculate expression with condition")
	}
	defer C.freeBandCalcResult(result)

	size := int(outSize)
	goResult := make([]float64, size)

	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}

	return goResult, nil
}

// ReplaceCondition 替换条件
type ReplaceCondition struct {
	MinValue   float64
	MaxValue   float64
	NewValue   float64
	IncludeMin bool
	IncludeMax bool
}

// ConditionalReplaceMulti 多条件替换
func (bc *BandCalculator) ConditionalReplaceMulti(bandIndex int, conditions []ReplaceCondition) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if len(conditions) == 0 {
		return nil, fmt.Errorf("conditions cannot be empty")
	}

	count := len(conditions)
	minValues := make([]C.double, count)
	maxValues := make([]C.double, count)
	newValues := make([]C.double, count)
	includeMin := make([]C.int, count)
	includeMax := make([]C.int, count)

	for i, cond := range conditions {
		minValues[i] = C.double(cond.MinValue)
		maxValues[i] = C.double(cond.MaxValue)
		newValues[i] = C.double(cond.NewValue)
		if cond.IncludeMin {
			includeMin[i] = 1
		}
		if cond.IncludeMax {
			includeMax[i] = 1
		}
	}

	var outSize C.int
	result := C.conditionalReplace(activeDS, C.int(bandIndex),
		&minValues[0], &maxValues[0], &newValues[0],
		&includeMin[0], &includeMax[0], C.int(count), &outSize)

	if result == nil {
		return nil, fmt.Errorf("failed to perform conditional replace")
	}
	defer C.freeBandCalcResult(result)

	size := int(outSize)
	goResult := make([]float64, size)

	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}

	return goResult, nil
}

// ConditionalReplace 简单条件替换
func (bc *BandCalculator) ConditionalReplace(bandIndex int, minVal, maxVal, newValue float64) ([]float64, error) {
	return bc.ConditionalReplaceMulti(bandIndex, []ReplaceCondition{
		{MinValue: minVal, MaxValue: maxVal, NewValue: newValue, IncludeMin: true, IncludeMax: false},
	})
}

// CalculateAndWrite 计算并写入到指定波段
func (bc *BandCalculator) CalculateAndWrite(expression string, targetBand int) error {
	// 验证目标波段
	bandCount := bc.rd.GetBandCount()
	if targetBand < 1 || targetBand > bandCount {
		return fmt.Errorf("invalid target band: %d (valid: 1-%d)", targetBand, bandCount)
	}
	result, err := bc.Calculate(expression)
	if err != nil {
		return fmt.Errorf("calculation failed: %w", err)
	}
	// 验证结果大小
	expectedSize := bc.rd.GetWidth() * bc.rd.GetHeight()
	if len(result) != expectedSize {
		return fmt.Errorf("result size mismatch: got %d, expected %d", len(result), expectedSize)
	}
	return bc.rd.WriteBandData(targetBand, result)
}

// ==================== 预定义指数计算 ====================

// CalculateNDVI 计算归一化植被指数 (NIR - Red) / (NIR + Red)
func (bc *BandCalculator) CalculateNDVI(nirBand, redBand int) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var outSize C.int
	result := C.calculateNDVI(activeDS, C.int(nirBand), C.int(redBand), &outSize)
	if result == nil {
		return nil, fmt.Errorf("failed to calculate NDVI")
	}
	defer C.freeBandCalcResult(result)

	size := int(outSize)
	goResult := make([]float64, size)

	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}

	return goResult, nil
}

// CalculateNDWI 计算归一化水体指数 (Green - NIR) / (Green + NIR)
func (bc *BandCalculator) CalculateNDWI(greenBand, nirBand int) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var outSize C.int
	result := C.calculateNDWI(activeDS, C.int(greenBand), C.int(nirBand), &outSize)
	if result == nil {
		return nil, fmt.Errorf("failed to calculate NDWI")
	}
	defer C.freeBandCalcResult(result)

	size := int(outSize)
	goResult := make([]float64, size)

	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}

	return goResult, nil
}

// CalculateEVI 计算增强植被指数
// EVI = 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)
func (bc *BandCalculator) CalculateEVI(nirBand, redBand, blueBand int) ([]float64, error) {
	if err := bc.rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := bc.rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var outSize C.int
	result := C.calculateEVI(activeDS, C.int(nirBand), C.int(redBand), C.int(blueBand), &outSize)
	if result == nil {
		return nil, fmt.Errorf("failed to calculate EVI")
	}
	defer C.freeBandCalcResult(result)

	size := int(outSize)
	goResult := make([]float64, size)

	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}

	return goResult, nil
}

// ==================== 分块计算器（用于超大影像） ====================

// BlockCalculator 分块计算器
type BlockCalculator struct {
	handle      *C.BlockCalculator // ★ 改为指针类型
	rd          *RasterDataset
	blockWidth  int
	blockHeight int
	numBlocksX  int
	numBlocksY  int
}

// NewBlockCalculator 创建分块计算器
func (rd *RasterDataset) NewBlockCalculator(expression string, blockWidth, blockHeight int) (*BlockCalculator, error) {
	if err := rd.ensureMemoryCopy(); err != nil {
		return nil, err
	}
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}
	cExpr := C.CString(expression)
	defer C.free(unsafe.Pointer(cExpr))
	handle := C.createBlockCalculator(activeDS, cExpr, C.int(blockWidth), C.int(blockHeight))
	if handle == nil {
		return nil, fmt.Errorf("failed to create block calculator")
	}
	numBlocksX := (rd.width + blockWidth - 1) / blockWidth
	numBlocksY := (rd.height + blockHeight - 1) / blockHeight
	return &BlockCalculator{
		handle:      handle, // 直接赋值指针
		rd:          rd,
		blockWidth:  blockWidth,
		blockHeight: blockHeight,
		numBlocksX:  numBlocksX,
		numBlocksY:  numBlocksY,
	}, nil
}
func (bc *BlockCalculator) Close() {
	if bc.handle != nil {
		C.freeBlockCalculator(bc.handle)
		bc.handle = nil
	}
}

// GetBlockCount 获取块数量
func (bc *BlockCalculator) GetBlockCount() (x, y int) {
	return bc.numBlocksX, bc.numBlocksY
}

// CalculateBlock 计算指定块
func (bc *BlockCalculator) CalculateBlock(blockX, blockY int) ([]float64, int, int, error) {
	if bc.handle == nil {
		return nil, 0, 0, fmt.Errorf("block calculator is nil")
	}
	var outWidth, outHeight C.int
	result := C.calculateBlock(bc.handle, C.int(blockX), C.int(blockY), &outWidth, &outHeight)
	if result == nil {
		return nil, 0, 0, fmt.Errorf("failed to calculate block (%d, %d)", blockX, blockY)
	}
	defer C.freeBandCalcResult(result)
	width := int(outWidth)
	height := int(outHeight)
	size := width * height
	goResult := make([]float64, size)
	cSlice := (*[1 << 30]C.double)(unsafe.Pointer(result))[:size:size]
	for i := 0; i < size; i++ {
		goResult[i] = float64(cSlice[i])
	}
	return goResult, width, height, nil
}

// CalculateAllBlocks 计算所有块并合并结果
func (bc *BlockCalculator) CalculateAllBlocks() ([]float64, error) {
	if bc.handle == nil {
		return nil, fmt.Errorf("block calculator is nil")
	}

	totalSize := bc.rd.width * bc.rd.height
	result := make([]float64, totalSize)

	for by := 0; by < bc.numBlocksY; by++ {
		for bx := 0; bx < bc.numBlocksX; bx++ {
			blockData, blockWidth, blockHeight, err := bc.CalculateBlock(bx, by)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate block (%d, %d): %w", bx, by, err)
			}

			// 将块数据复制到结果数组
			xOff := bx * bc.blockWidth
			yOff := by * bc.blockHeight

			for y := 0; y < blockHeight; y++ {
				for x := 0; x < blockWidth; x++ {
					srcIdx := y*blockWidth + x
					dstIdx := (yOff+y)*bc.rd.width + (xOff + x)
					result[dstIdx] = blockData[srcIdx]
				}
			}
		}
	}

	return result, nil
}

// ==================== 批量表达式计算 ====================

// BatchExpressionResult 批量表达式计算结果
type BatchExpressionResult struct {
	Expression string
	Data       []float64
	Error      error
}

// CalculateBatch 批量计算多个表达式
func (bc *BandCalculator) CalculateBatch(expressions []string) []BatchExpressionResult {
	results := make([]BatchExpressionResult, len(expressions))

	for i, expr := range expressions {
		data, err := bc.Calculate(expr)
		results[i] = BatchExpressionResult{
			Expression: expr,
			Data:       data,
			Error:      err,
		}
	}

	return results
}

// ==================== 常用遥感指数快捷方法 ====================

// CalculateSAVI 计算土壤调节植被指数
// SAVI = ((NIR - Red) / (NIR + Red + L)) * (1 + L), L通常取0.5
func (bc *BandCalculator) CalculateSAVI(nirBand, redBand int, L float64) ([]float64, error) {
	expr := fmt.Sprintf("((b%d - b%d) / (b%d + b%d + %.6f)) * (1 + %.6f)",
		nirBand, redBand, nirBand, redBand, L, L)
	return bc.Calculate(expr)
}

// CalculateMNDWI 计算改进的归一化水体指数
// MNDWI = (Green - SWIR) / (Green + SWIR)
func (bc *BandCalculator) CalculateMNDWI(greenBand, swirBand int) ([]float64, error) {
	expr := fmt.Sprintf("(b%d - b%d) / (b%d + b%d)", greenBand, swirBand, greenBand, swirBand)
	return bc.Calculate(expr)
}

// CalculateNDBI 计算归一化建筑指数
// NDBI = (SWIR - NIR) / (SWIR + NIR)
func (bc *BandCalculator) CalculateNDBI(swirBand, nirBand int) ([]float64, error) {
	expr := fmt.Sprintf("(b%d - b%d) / (b%d + b%d)", swirBand, nirBand, swirBand, nirBand)
	return bc.Calculate(expr)
}

// CalculateNDSI 计算归一化雪指数
// NDSI = (Green - SWIR) / (Green + SWIR)
func (bc *BandCalculator) CalculateNDSI(greenBand, swirBand int) ([]float64, error) {
	expr := fmt.Sprintf("(b%d - b%d) / (b%d + b%d)", greenBand, swirBand, greenBand, swirBand)
	return bc.Calculate(expr)
}

// CalculateLAI 计算叶面积指数（基于NDVI的经验公式）
// LAI = -ln((0.69 - NDVI) / 0.59) / 0.91
func (bc *BandCalculator) CalculateLAI(nirBand, redBand int) ([]float64, error) {
	// 先计算NDVI
	ndviExpr := fmt.Sprintf("(b%d - b%d) / (b%d + b%d)", nirBand, redBand, nirBand, redBand)
	ndvi, err := bc.Calculate(ndviExpr)
	if err != nil {
		return nil, err
	}

	// 计算LAI
	result := make([]float64, len(ndvi))
	for i, v := range ndvi {
		if v >= 0.69 {
			result[i] = 6.0 // 最大值限制
		} else if v <= 0.1 {
			result[i] = 0.0 // 最小值限制
		} else {
			val := (0.69 - v) / 0.59
			if val > 0 {
				result[i] = -math.Log(val) / 0.91
			} else {
				result[i] = 6.0
			}
		}
	}

	return result, nil
}

// ==================== 表达式验证 ====================

// ValidateExpression 验证表达式是否合法
func (bc *BandCalculator) ValidateExpression(expression string) error {
	cExpr := C.CString(expression)
	defer C.free(unsafe.Pointer(cExpr))

	ce := C.compileExpression(cExpr)
	if ce == nil {
		return fmt.Errorf("invalid expression: %s", expression)
	}
	C.freeCompiledExpression(ce)
	return nil
}

// CreateSingleBandDataset 从计算结果创建单波段数据集
func (rd *RasterDataset) CreateSingleBandDataset(data []float64, dataType BandDataType) (*RasterDataset, error) {
	width := rd.GetWidth()
	height := rd.GetHeight()
	expectedSize := width * height

	if len(data) != expectedSize {
		return nil, fmt.Errorf("data size mismatch: got %d, expected %d", len(data), expectedSize)
	}

	// 获取MEM驱动
	cDriverName := C.CString("MEM")
	defer C.free(unsafe.Pointer(cDriverName))
	driver := C.GDALGetDriverByName(cDriverName)
	if driver == nil {
		return nil, fmt.Errorf("failed to get MEM driver")
	}

	// 创建内存数据集
	cEmpty := C.CString("")
	defer C.free(unsafe.Pointer(cEmpty))
	newDS := C.GDALCreate(driver, cEmpty, C.int(width), C.int(height), 1, C.GDALDataType(dataType), nil)
	if newDS == nil {
		return nil, fmt.Errorf("failed to create dataset")
	}

	// 复制地理变换
	var geoTransform [6]C.double
	activeDS := rd.dataset
	if rd.warpedDS != nil {
		activeDS = rd.warpedDS
	}

	hasGeoInfo := false
	if C.GDALGetGeoTransform(activeDS, &geoTransform[0]) == C.CE_None {
		C.GDALSetGeoTransform(newDS, &geoTransform[0])
		hasGeoInfo = true
	}

	// 复制投影
	proj := C.GDALGetProjectionRef(activeDS)
	projection := ""
	if proj != nil {
		projection = C.GoString(proj)
		if projection != "" {
			cProj := C.CString(projection)
			defer C.free(unsafe.Pointer(cProj))
			C.GDALSetProjection(newDS, cProj)
		}
	}

	// 获取波段并写入数据
	band := C.GDALGetRasterBand(newDS, 1)
	if band == nil {
		C.GDALClose(newDS)
		return nil, fmt.Errorf("failed to get raster band")
	}

	// 转换数据
	cData := make([]C.double, len(data))
	for i, v := range data {
		cData[i] = C.double(v)
	}

	// 写入数据
	err := C.GDALRasterIO(band, C.GF_Write, 0, 0, C.int(width), C.int(height),
		unsafe.Pointer(&cData[0]), C.int(width), C.int(height), C.GDT_Float64, 0, 0)
	if err != C.CE_None {
		C.GDALClose(newDS)
		return nil, fmt.Errorf("failed to write raster data")
	}

	// 计算bounds
	var bounds [4]float64
	if hasGeoInfo {
		bounds[0] = float64(geoTransform[0])                                            // minX
		bounds[1] = float64(geoTransform[3]) + float64(height)*float64(geoTransform[5]) // minY
		bounds[2] = float64(geoTransform[0]) + float64(width)*float64(geoTransform[1])  // maxX
		bounds[3] = float64(geoTransform[3])                                            // maxY
	}

	// 创建新的 RasterDataset
	newRD := &RasterDataset{
		dataset:       newDS,
		warpedDS:      nil,
		filePath:      "",
		width:         width,
		height:        height,
		bandCount:     1,
		bounds:        bounds,
		projection:    projection,
		isReprojected: rd.isReprojected,
		hasGeoInfo:    hasGeoInfo,
	}

	return newRD, nil
}
