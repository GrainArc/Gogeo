// RasterColor.go
package Gogeo

/*
#include "osgeo_color.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// ColorAdjustParams 调色参数
type ColorAdjustParams struct {
	Brightness float64 // 亮度调整 [-1.0, 1.0]
	Contrast   float64 // 对比度调整 [-1.0, 1.0]
	Saturation float64 // 饱和度调整 [-1.0, 1.0]
	Gamma      float64 // Gamma校正 [0.1, 10.0]
	Hue        float64 // 色相调整 [-180, 180]
}

// LevelsParams 色阶调整参数
type LevelsParams struct {
	InputMin  float64 // 输入最小值
	InputMax  float64 // 输入最大值
	OutputMin float64 // 输出最小值
	OutputMax float64 // 输出最大值
	Midtone   float64 // 中间调 [0.1, 9.9], 1.0为不变
}

// CurvePoint 曲线控制点
type CurvePoint struct {
	Input  float64 // 输入值 [0, 255]
	Output float64 // 输出值 [0, 255]
}

// CurveParams 曲线调整参数
type CurveParams struct {
	Points  []CurvePoint // 控制点数组
	Channel int          // 通道: 0=全部, 1=R, 2=G, 3=B
}

// ReferenceRegion 参考区域
type ReferenceRegion struct {
	X      int
	Y      int
	Width  int
	Height int
}

// ColorStatistics 颜色统计信息
type ColorStatistics struct {
	MeanR, MeanG, MeanB float64
	StdR, StdG, StdB    float64
	MinR, MinG, MinB    float64
	MaxR, MaxG, MaxB    float64
}

// BandMetaStatistics 波段统计信息
type BandMetaStatistics struct {
	Min       float64
	Max       float64
	Mean      float64
	Stddev    float64
	Histogram []int
}

// ColorBalanceMethod 匀色方法
type ColorBalanceMethod int

const (
	BalanceHistogramMatch   ColorBalanceMethod = C.BALANCE_HISTOGRAM_MATCH
	BalanceMeanStd          ColorBalanceMethod = C.BALANCE_MEAN_STD
	BalanceWallis           ColorBalanceMethod = C.BALANCE_WALLIS
	BalanceMomentMatch      ColorBalanceMethod = C.BALANCE_MOMENT_MATCH
	BalanceLinearRegression ColorBalanceMethod = C.BALANCE_LINEAR_REGRESSION
	BalanceDodging          ColorBalanceMethod = C.BALANCE_DODGING
)

// ColorBalanceParams 匀色参数
type ColorBalanceParams struct {
	Method        ColorBalanceMethod
	Strength      float64          // 匀色强度 [0, 1]
	OverlapRegion *ReferenceRegion // 重叠区域
	WallisC       float64          // Wallis对比度参数 [0, 1]
	WallisB       float64          // Wallis亮度参数 [0, 1]
	TargetMean    float64          // 目标均值
	TargetStd     float64          // 目标标准差
}

// ==================== 调色函数 ====================

// AdjustColors 综合调色
func (rd *RasterDataset) AdjustColors(params *ColorAdjustParams) (*RasterDataset, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cParams := C.ColorAdjustParams{
		brightness: C.double(params.Brightness),
		contrast:   C.double(params.Contrast),
		saturation: C.double(params.Saturation),
		gamma:      C.double(params.Gamma),
		hue:        C.double(params.Hue),
	}

	newDS := C.adjustColors(activeDS, &cParams)
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust colors")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustBrightness 调整亮度
func (rd *RasterDataset) AdjustBrightness(brightness float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.adjustBrightness(activeDS, C.double(brightness))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust brightness")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustContrast 调整对比度
func (rd *RasterDataset) AdjustContrast(contrast float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.adjustContrast(activeDS, C.double(contrast))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust contrast")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustSaturation 调整饱和度
func (rd *RasterDataset) AdjustSaturation(saturation float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.adjustSaturation(activeDS, C.double(saturation))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust saturation")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustGamma Gamma校正
func (rd *RasterDataset) AdjustGamma(gamma float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if gamma <= 0 {
		return nil, fmt.Errorf("gamma must be positive")
	}

	newDS := C.adjustGamma(activeDS, C.double(gamma))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust gamma")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustHue 调整色相
func (rd *RasterDataset) AdjustHue(hue float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.adjustHue(activeDS, C.double(hue))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust hue")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustLevels 色阶调整
func (rd *RasterDataset) AdjustLevels(params *LevelsParams, bandIndex int) (*RasterDataset, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cParams := C.LevelsParams{
		inputMin:  C.double(params.InputMin),
		inputMax:  C.double(params.InputMax),
		outputMin: C.double(params.OutputMin),
		outputMax: C.double(params.OutputMax),
		midtone:   C.double(params.Midtone),
	}

	newDS := C.adjustLevels(activeDS, &cParams, C.int(bandIndex))
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust levels")
	}

	return rd.createNewDataset(newDS), nil
}

// AdjustCurves 曲线调整
func (rd *RasterDataset) AdjustCurves(params *CurveParams) (*RasterDataset, error) {
	if params == nil || len(params.Points) < 2 {
		return nil, fmt.Errorf("at least 2 curve points required")
	}

	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	// 转换控制点
	cPoints := make([]C.CurvePoint, len(params.Points))
	for i, p := range params.Points {
		cPoints[i] = C.CurvePoint{
			input:  C.double(p.Input),
			output: C.double(p.Output),
		}
	}

	cParams := C.CurveParams{
		points:     &cPoints[0],
		pointCount: C.int(len(params.Points)),
		channel:    C.int(params.Channel),
	}

	newDS := C.adjustCurves(activeDS, &cParams)
	if newDS == nil {
		return nil, fmt.Errorf("failed to adjust curves")
	}

	return rd.createNewDataset(newDS), nil
}

// AutoLevels 自动色阶
func (rd *RasterDataset) AutoLevels(clipPercent float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.autoLevels(activeDS, C.double(clipPercent))
	if newDS == nil {
		return nil, fmt.Errorf("failed to auto levels")
	}

	return rd.createNewDataset(newDS), nil
}

// AutoContrast 自动对比度
func (rd *RasterDataset) AutoContrast() (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.autoContrast(activeDS)
	if newDS == nil {
		return nil, fmt.Errorf("failed to auto contrast")
	}

	return rd.createNewDataset(newDS), nil
}

// AutoWhiteBalance 自动白平衡
func (rd *RasterDataset) AutoWhiteBalance() (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.autoWhiteBalance(activeDS)
	if newDS == nil {
		return nil, fmt.Errorf("failed to auto white balance")
	}

	return rd.createNewDataset(newDS), nil
}

// HistogramEqualization 直方图均衡化
func (rd *RasterDataset) HistogramEqualization(bandIndex int) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.histogramEqualization(activeDS, C.int(bandIndex))
	if newDS == nil {
		return nil, fmt.Errorf("failed to histogram equalization")
	}

	return rd.createNewDataset(newDS), nil
}

// CLAHEEqualization CLAHE均衡化
func (rd *RasterDataset) CLAHEEqualization(tileSize int, clipLimit float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.claheEqualization(activeDS, C.int(tileSize), C.double(clipLimit))
	if newDS == nil {
		return nil, fmt.Errorf("failed to CLAHE equalization")
	}

	return rd.createNewDataset(newDS), nil
}

// ==================== 匀色函数 ====================

// GetColorStatistics 获取颜色统计信息
func (rd *RasterDataset) GetColorStatistics(region *ReferenceRegion) (*ColorStatistics, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var cRegion *C.ReferenceRegion
	if region != nil {
		cRegion = &C.ReferenceRegion{
			x:      C.int(region.X),
			y:      C.int(region.Y),
			width:  C.int(region.Width),
			height: C.int(region.Height),
		}
	}

	cStats := C.getColorStatistics(activeDS, cRegion)
	if cStats == nil {
		return nil, fmt.Errorf("failed to get color statistics")
	}
	defer C.freeColorStatistics(cStats)

	stats := &ColorStatistics{
		MeanR: float64(cStats.meanR),
		MeanG: float64(cStats.meanG),
		MeanB: float64(cStats.meanB),
		StdR:  float64(cStats.stdR),
		StdG:  float64(cStats.stdG),
		StdB:  float64(cStats.stdB),
		MinR:  float64(cStats.minR),
		MinG:  float64(cStats.minG),
		MinB:  float64(cStats.minB),
		MaxR:  float64(cStats.maxR),
		MaxG:  float64(cStats.maxG),
		MaxB:  float64(cStats.maxB),
	}

	return stats, nil
}

// GetBandStatistics 获取波段统计信息
func (rd *RasterDataset) GetBandStatistics(bandIndex int, region *ReferenceRegion) (*BandMetaStatistics, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var cRegion *C.ReferenceRegion
	if region != nil {
		cRegion = &C.ReferenceRegion{
			x:      C.int(region.X),
			y:      C.int(region.Y),
			width:  C.int(region.Width),
			height: C.int(region.Height),
		}
	}

	cStats := C.getBandStatistics(activeDS, C.int(bandIndex), cRegion)
	if cStats == nil {
		return nil, fmt.Errorf("failed to get band statistics")
	}
	defer C.freeBandStatistics(cStats)

	stats := &BandMetaStatistics{
		Min:    float64(cStats.min),
		Max:    float64(cStats.max),
		Mean:   float64(cStats.mean),
		Stddev: float64(cStats.stddev),
	}

	if cStats.histogram != nil && cStats.histogramSize > 0 {
		stats.Histogram = make([]int, int(cStats.histogramSize))
		cHistSlice := (*[256]C.int)(unsafe.Pointer(cStats.histogram))[:256:256]
		for i := 0; i < 256; i++ {
			stats.Histogram[i] = int(cHistSlice[i])
		}
	}

	return stats, nil
}

// HistogramMatch 直方图匹配
func (rd *RasterDataset) HistogramMatch(refDS *RasterDataset, srcRegion, refRegion *ReferenceRegion) (*RasterDataset, error) {
	srcActiveDS := rd.GetActiveDataset()
	refActiveDS := refDS.GetActiveDataset()

	if srcActiveDS == nil || refActiveDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var cSrcRegion, cRefRegion *C.ReferenceRegion
	if srcRegion != nil {
		cSrcRegion = &C.ReferenceRegion{
			x:      C.int(srcRegion.X),
			y:      C.int(srcRegion.Y),
			width:  C.int(srcRegion.Width),
			height: C.int(srcRegion.Height),
		}
	}
	if refRegion != nil {
		cRefRegion = &C.ReferenceRegion{
			x:      C.int(refRegion.X),
			y:      C.int(refRegion.Y),
			width:  C.int(refRegion.Width),
			height: C.int(refRegion.Height),
		}
	}

	newDS := C.histogramMatch(srcActiveDS, refActiveDS, cSrcRegion, cRefRegion)
	if newDS == nil {
		return nil, fmt.Errorf("failed to histogram match")
	}

	return rd.createNewDataset(newDS), nil
}

// MeanStdMatch 均值-标准差匹配
func (rd *RasterDataset) MeanStdMatch(targetStats *ColorStatistics, region *ReferenceRegion, strength float64) (*RasterDataset, error) {
	if targetStats == nil {
		return nil, fmt.Errorf("target stats cannot be nil")
	}

	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cTargetStats := C.ColorStatistics{
		meanR: C.double(targetStats.MeanR),
		meanG: C.double(targetStats.MeanG),
		meanB: C.double(targetStats.MeanB),
		stdR:  C.double(targetStats.StdR),
		stdG:  C.double(targetStats.StdG),
		stdB:  C.double(targetStats.StdB),
	}

	var cRegion *C.ReferenceRegion
	if region != nil {
		cRegion = &C.ReferenceRegion{
			x:      C.int(region.X),
			y:      C.int(region.Y),
			width:  C.int(region.Width),
			height: C.int(region.Height),
		}
	}

	newDS := C.meanStdMatch(activeDS, &cTargetStats, cRegion, C.double(strength))
	if newDS == nil {
		return nil, fmt.Errorf("failed to mean-std match")
	}

	return rd.createNewDataset(newDS), nil
}

// WallisFilter Wallis滤波匀色
func (rd *RasterDataset) WallisFilter(targetMean, targetStd, c, b float64, windowSize int) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.wallisFilter(activeDS, C.double(targetMean), C.double(targetStd),
		C.double(c), C.double(b), C.int(windowSize))
	if newDS == nil {
		return nil, fmt.Errorf("failed to wallis filter")
	}

	return rd.createNewDataset(newDS), nil
}

// MomentMatch 矩匹配
func (rd *RasterDataset) MomentMatch(refDS *RasterDataset, srcRegion, refRegion *ReferenceRegion) (*RasterDataset, error) {
	srcActiveDS := rd.GetActiveDataset()
	refActiveDS := refDS.GetActiveDataset()

	if srcActiveDS == nil || refActiveDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	// RasterColor.go (续)

	var cSrcRegion, cRefRegion *C.ReferenceRegion
	if srcRegion != nil {
		cSrcRegion = &C.ReferenceRegion{
			x:      C.int(srcRegion.X),
			y:      C.int(srcRegion.Y),
			width:  C.int(srcRegion.Width),
			height: C.int(srcRegion.Height),
		}
	}
	if refRegion != nil {
		cRefRegion = &C.ReferenceRegion{
			x:      C.int(refRegion.X),
			y:      C.int(refRegion.Y),
			width:  C.int(refRegion.Width),
			height: C.int(refRegion.Height),
		}
	}

	newDS := C.momentMatch(srcActiveDS, refActiveDS, cSrcRegion, cRefRegion)
	if newDS == nil {
		return nil, fmt.Errorf("failed to moment match")
	}

	return rd.createNewDataset(newDS), nil
}

// LinearRegressionBalance 线性回归匀色
func (rd *RasterDataset) LinearRegressionBalance(refDS *RasterDataset, overlapRegion *ReferenceRegion) (*RasterDataset, error) {
	if overlapRegion == nil {
		return nil, fmt.Errorf("overlap region cannot be nil")
	}

	srcActiveDS := rd.GetActiveDataset()
	refActiveDS := refDS.GetActiveDataset()

	if srcActiveDS == nil || refActiveDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cRegion := C.ReferenceRegion{
		x:      C.int(overlapRegion.X),
		y:      C.int(overlapRegion.Y),
		width:  C.int(overlapRegion.Width),
		height: C.int(overlapRegion.Height),
	}

	newDS := C.linearRegressionBalance(srcActiveDS, refActiveDS, &cRegion)
	if newDS == nil {
		return nil, fmt.Errorf("failed to linear regression balance")
	}

	return rd.createNewDataset(newDS), nil
}

// DodgingBalance Dodging匀光
func (rd *RasterDataset) DodgingBalance(blockSize int, strength float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.dodgingBalance(activeDS, C.int(blockSize), C.double(strength))
	if newDS == nil {
		return nil, fmt.Errorf("failed to dodging balance")
	}

	return rd.createNewDataset(newDS), nil
}

// GradientBlend 渐变融合
func (rd *RasterDataset) GradientBlend(ds2 *RasterDataset, overlapRegion *ReferenceRegion, blendWidth int) (*RasterDataset, error) {
	if overlapRegion == nil {
		return nil, fmt.Errorf("overlap region cannot be nil")
	}

	activeDS1 := rd.GetActiveDataset()
	activeDS2 := ds2.GetActiveDataset()

	if activeDS1 == nil || activeDS2 == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cRegion := C.ReferenceRegion{
		x:      C.int(overlapRegion.X),
		y:      C.int(overlapRegion.Y),
		width:  C.int(overlapRegion.Width),
		height: C.int(overlapRegion.Height),
	}

	newDS := C.gradientBlend(activeDS1, activeDS2, &cRegion, C.int(blendWidth))
	if newDS == nil {
		return nil, fmt.Errorf("failed to gradient blend")
	}

	return rd.createNewDataset(newDS), nil
}

// ColorBalance 通用匀色接口
func (rd *RasterDataset) ColorBalance(refDS *RasterDataset, params *ColorBalanceParams) (*RasterDataset, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	switch params.Method {
	case BalanceHistogramMatch:
		return rd.HistogramMatch(refDS, params.OverlapRegion, params.OverlapRegion)

	case BalanceMeanStd:
		refStats, err := refDS.GetColorStatistics(params.OverlapRegion)
		if err != nil {
			return nil, err
		}
		return rd.MeanStdMatch(refStats, params.OverlapRegion, params.Strength)

	case BalanceWallis:
		return rd.WallisFilter(params.TargetMean, params.TargetStd, params.WallisC, params.WallisB, 31)

	case BalanceMomentMatch:
		return rd.MomentMatch(refDS, params.OverlapRegion, params.OverlapRegion)

	case BalanceLinearRegression:
		return rd.LinearRegressionBalance(refDS, params.OverlapRegion)

	case BalanceDodging:
		return rd.DodgingBalance(128, params.Strength)

	default:
		return nil, fmt.Errorf("unknown balance method: %d", params.Method)
	}
}

// ==================== 批量处理函数 ====================

// BatchColorBalance 批量匀色
func BatchColorBalance(datasets []*RasterDataset, refDS *RasterDataset, params *ColorBalanceParams) ([]*RasterDataset, error) {
	if len(datasets) == 0 {
		return nil, fmt.Errorf("no datasets provided")
	}
	if refDS == nil {
		return nil, fmt.Errorf("reference dataset cannot be nil")
	}
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	results := make([]*RasterDataset, len(datasets))

	for i, ds := range datasets {
		if ds == nil {
			results[i] = nil
			continue
		}

		result, err := ds.ColorBalance(refDS, params)
		if err != nil {
			// 记录错误但继续处理
			results[i] = nil
			continue
		}
		results[i] = result
	}

	return results, nil
}

// ==================== 辅助函数 ====================

// createNewDataset 从C数据集创建新的RasterDataset
func (rd *RasterDataset) createNewDataset(cDS C.GDALDatasetH) *RasterDataset {
	newRD := &RasterDataset{
		dataset:       cDS,
		warpedDS:      nil,
		width:         int(C.GDALGetRasterXSize(cDS)),
		height:        int(C.GDALGetRasterYSize(cDS)),
		bandCount:     int(C.GDALGetRasterCount(cDS)),
		bounds:        rd.bounds,
		projection:    rd.projection,
		isReprojected: false,
		hasGeoInfo:    rd.hasGeoInfo,
	}
	return newRD
}

// ==================== 预设调色方案 ====================

// PresetVivid 鲜艳预设
func (rd *RasterDataset) PresetVivid() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0.05,
		Contrast:   0.15,
		Saturation: 0.3,
		Gamma:      1.1,
		Hue:        0,
	}
	return rd.AdjustColors(params)
}

// PresetSoft 柔和预设
func (rd *RasterDataset) PresetSoft() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0.02,
		Contrast:   -0.1,
		Saturation: -0.15,
		Gamma:      1.05,
		Hue:        0,
	}
	return rd.AdjustColors(params)
}

// PresetHighContrast 高对比度预设
func (rd *RasterDataset) PresetHighContrast() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0,
		Contrast:   0.4,
		Saturation: 0.1,
		Gamma:      1.0,
		Hue:        0,
	}
	return rd.AdjustColors(params)
}

// PresetWarm 暖色调预设
func (rd *RasterDataset) PresetWarm() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0.03,
		Contrast:   0.05,
		Saturation: 0.1,
		Gamma:      1.0,
		Hue:        15,
	}
	return rd.AdjustColors(params)
}

// PresetCool 冷色调预设
func (rd *RasterDataset) PresetCool() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0,
		Contrast:   0.05,
		Saturation: 0.05,
		Gamma:      1.0,
		Hue:        -15,
	}
	return rd.AdjustColors(params)
}

// PresetBlackWhite 黑白预设
func (rd *RasterDataset) PresetBlackWhite() (*RasterDataset, error) {
	params := &ColorAdjustParams{
		Brightness: 0,
		Contrast:   0.1,
		Saturation: -1.0,
		Gamma:      1.0,
		Hue:        0,
	}
	return rd.AdjustColors(params)
}

// PresetSepia 复古棕褐色预设
func (rd *RasterDataset) PresetSepia() (*RasterDataset, error) {
	// 先转黑白
	bw, err := rd.PresetBlackWhite()
	if err != nil {
		return nil, err
	}

	// 再调整色相到棕褐色
	params := &ColorAdjustParams{
		Brightness: 0.05,
		Contrast:   0,
		Saturation: 0.3,
		Gamma:      1.0,
		Hue:        30,
	}
	return bw.AdjustColors(params)
}

// ==================== S曲线调整 ====================

// SCurveContrast S曲线对比度增强
func (rd *RasterDataset) SCurveContrast(strength float64) (*RasterDataset, error) {
	// 创建S曲线控制点
	points := []CurvePoint{
		{Input: 0, Output: 0},
		{Input: 64, Output: 64 - 32*strength},
		{Input: 128, Output: 128},
		{Input: 192, Output: 192 + 32*strength},
		{Input: 255, Output: 255},
	}

	// 限制输出范围
	for i := range points {
		if points[i].Output < 0 {
			points[i].Output = 0
		}
		if points[i].Output > 255 {
			points[i].Output = 255
		}
	}

	params := &CurveParams{
		Points:  points,
		Channel: 0, // 应用到所有通道
	}

	return rd.AdjustCurves(params)
}

// ==================== 色彩空间转换辅助 ====================

// RGBToHSL RGB转HSL
func RGBToHSL(r, g, b float64) (h, s, l float64) {
	var ch, cs, cl C.double
	C.rgbToHsl(C.double(r), C.double(g), C.double(b), &ch, &cs, &cl)
	return float64(ch), float64(cs), float64(cl)
}

// HSLToRGB HSL转RGB
func HSLToRGB(h, s, l float64) (r, g, b float64) {
	var cr, cg, cb C.double
	C.hslToRgb(C.double(h), C.double(s), C.double(l), &cr, &cg, &cb)
	return float64(cr), float64(cg), float64(cb)
}

// RGBToHSV RGB转HSV
func RGBToHSV(r, g, b float64) (h, s, v float64) {
	var ch, cs, cv C.double
	C.rgbToHsv(C.double(r), C.double(g), C.double(b), &ch, &cs, &cv)
	return float64(ch), float64(cs), float64(cv)
}

// HSVToRGB HSV转RGB
func HSVToRGB(h, s, v float64) (r, g, b float64) {
	var cr, cg, cb C.double
	C.hsvToRgb(C.double(h), C.double(s), C.double(v), &cr, &cg, &cb)
	return float64(cr), float64(cg), float64(cb)
}

// ==================== 高级匀色功能 ====================

// AutoColorBalance 自动匀色（基于参考图像）
func (rd *RasterDataset) AutoColorBalance(refDS *RasterDataset) (*RasterDataset, error) {
	// 获取参考图像统计信息
	refStats, err := refDS.GetColorStatistics(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get reference statistics: %w", err)
	}

	// 使用均值-标准差匹配，强度0.8
	return rd.MeanStdMatch(refStats, nil, 0.8)
}

// SmartColorBalance 智能匀色（自动选择最佳方法）
func (rd *RasterDataset) SmartColorBalance(refDS *RasterDataset, overlapRegion *ReferenceRegion) (*RasterDataset, error) {
	// 获取源图像和参考图像的统计信息
	srcStats, err := rd.GetColorStatistics(overlapRegion)
	if err != nil {
		return nil, err
	}

	refStats, err := refDS.GetColorStatistics(overlapRegion)
	if err != nil {
		return nil, err
	}

	// 计算颜色差异
	meanDiff := (absFloat(srcStats.MeanR-refStats.MeanR) +
		absFloat(srcStats.MeanG-refStats.MeanG) +
		absFloat(srcStats.MeanB-refStats.MeanB)) / 3.0

	stdDiff := (absFloat(srcStats.StdR-refStats.StdR) +
		absFloat(srcStats.StdG-refStats.StdG) +
		absFloat(srcStats.StdB-refStats.StdB)) / 3.0

	// 根据差异选择方法
	if meanDiff > 50 || stdDiff > 30 {
		// 差异较大，使用直方图匹配
		return rd.HistogramMatch(refDS, overlapRegion, overlapRegion)
	} else if overlapRegion != nil && overlapRegion.Width > 100 && overlapRegion.Height > 100 {
		// 有足够大的重叠区域，使用线性回归
		return rd.LinearRegressionBalance(refDS, overlapRegion)
	} else {
		// 默认使用均值-标准差匹配
		return rd.MeanStdMatch(refStats, overlapRegion, 0.9)
	}
}

// absFloat 浮点数绝对值
func absFloat(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ==================== 局部调整功能 ====================

// LocalContrastEnhancement 局部对比度增强
func (rd *RasterDataset) LocalContrastEnhancement(tileSize int, clipLimit float64) (*RasterDataset, error) {
	return rd.CLAHEEqualization(tileSize, clipLimit)
}

// UnsharpMask USM锐化（通过对比度增强模拟）
func (rd *RasterDataset) UnsharpMask(amount float64) (*RasterDataset, error) {
	// 使用对比度增强模拟USM效果
	params := &ColorAdjustParams{
		Brightness: 0,
		Contrast:   amount * 0.3,
		Saturation: amount * 0.1,
		Gamma:      1.0,
		Hue:        0,
	}
	return rd.AdjustColors(params)
}

// ==================== 色彩校正功能 ====================

// ColorCorrection 色彩校正（基于灰点）
func (rd *RasterDataset) ColorCorrection(grayPointR, grayPointG, grayPointB float64) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	// 计算校正系数
	avgGray := (grayPointR + grayPointG + grayPointB) / 3.0

	scaleR := avgGray / grayPointR
	scaleG := avgGray / grayPointG
	scaleB := avgGray / grayPointB

	// 创建目标统计信息
	srcStats, err := rd.GetColorStatistics(nil)
	if err != nil {
		return nil, err
	}

	targetStats := &ColorStatistics{
		MeanR: srcStats.MeanR * scaleR,
		MeanG: srcStats.MeanG * scaleG,
		MeanB: srcStats.MeanB * scaleB,
		StdR:  srcStats.StdR,
		StdG:  srcStats.StdG,
		StdB:  srcStats.StdB,
	}

	return rd.MeanStdMatch(targetStats, nil, 1.0)
}

// ==================== 导出功能 ====================

// ExportWithColorAdjust 导出时应用调色
func (rd *RasterDataset) ExportWithColorAdjust(outputPath string, format string, params *ColorAdjustParams) error {
	// 先应用调色
	adjusted, err := rd.AdjustColors(params)
	if err != nil {
		return err
	}
	defer adjusted.Close()

	// 导出
	return adjusted.ExportToFile(outputPath, format, nil)
}

// ==================== 链式调用支持 ====================

// ColorPipeline 调色管道
type ColorPipeline struct {
	dataset *RasterDataset
	err     error
}

// NewColorPipeline 创建调色管道
func (rd *RasterDataset) NewColorPipeline() *ColorPipeline {
	return &ColorPipeline{
		dataset: rd,
		err:     nil,
	}
}

// Brightness 调整亮度
func (cp *ColorPipeline) Brightness(value float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AdjustBrightness(value)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// Contrast 调整对比度
func (cp *ColorPipeline) Contrast(value float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AdjustContrast(value)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// Saturation 调整饱和度
func (cp *ColorPipeline) Saturation(value float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AdjustSaturation(value)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// Gamma Gamma校正
func (cp *ColorPipeline) Gamma(value float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AdjustGamma(value)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// Hue 调整色相
func (cp *ColorPipeline) Hue(value float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AdjustHue(value)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// AutoLevels 自动色阶
func (cp *ColorPipeline) AutoLevels(clipPercent float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AutoLevels(clipPercent)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// AutoWhiteBalance 自动白平衡
func (cp *ColorPipeline) AutoWhiteBalance() *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.AutoWhiteBalance()
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// CLAHE CLAHE均衡化
func (cp *ColorPipeline) CLAHE(tileSize int, clipLimit float64) *ColorPipeline {
	if cp.err != nil {
		return cp
	}
	newDS, err := cp.dataset.CLAHEEqualization(tileSize, clipLimit)
	if err != nil {
		cp.err = err
		return cp
	}
	cp.dataset = newDS
	return cp
}

// Result 获取结果
func (cp *ColorPipeline) Result() (*RasterDataset, error) {
	return cp.dataset, cp.err
}

// Export 导出结果
func (cp *ColorPipeline) Export(outputPath string, format string) error {
	if cp.err != nil {
		return cp.err
	}
	return cp.dataset.ExportToFile(outputPath, format, nil)
}
