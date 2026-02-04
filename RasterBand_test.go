package Gogeo

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"runtime"
)

func AddBandSample(rd *RasterDataset) {
	// 添加Alpha波段
	err := rd.AddBand(BandAlpha8, ColorAlpha, 255)
	if err != nil {
		log.Printf("添加波段失败: %v", err)
	}
}

func SetBandColorSample(rd *RasterDataset) {
	err := rd.SetBandColorInterpretation(2, ColorGray)
	if err != nil {
		log.Printf("修改颜色解释失败: %v", err)
	}
}

func SetBandNoDataSample(rd *RasterDataset) {
	err := rd.SetBandNoDataValue(1, 0)
	if err != nil {
		log.Printf("设置NoData失败: %v", err)
	}
}

func ReorderBandsSample(rd *RasterDataset) {
	// 重排波段顺序 (BGR -> RGB)
	err := rd.ReorderBands([]int{3, 2, 1})
	if err != nil {
		log.Printf("重排波段失败: %v", err)
	}
}

func ConvertBandDataTypeSample(rd *RasterDataset) {
	// 转换波段数据类型
	err := rd.ConvertBandDataType(1, BandUInt16)
	if err != nil {
		log.Printf("转换数据类型失败: %v", err)
	}
}

func RemoveBandSample(rd *RasterDataset) {
	// 删除波段
	err := rd.RemoveBand(5)
	if err != nil {
		log.Printf("删除波段失败: %v", err)
		return
	}
}

func GetPaletteInfoSample(rd *RasterDataset) {
	// 获取调色板信息
	paletteInfo, err := rd.GetPaletteInfo(1)
	if err != nil {
		fmt.Println("该波段没有调色板")
	} else {
		fmt.Printf("调色板条目数: %d\n", paletteInfo.EntryCount)
		for i, entry := range paletteInfo.Entries[:10] { // 只打印前10个
			fmt.Printf("  [%d] R=%d G=%d B=%d A=%d\n",
				i, entry.C1, entry.C2, entry.C3, entry.C4)
		}
	}
}

func CreateAndSetPaletteSample(rd *RasterDataset) {
	// 创建并设置新调色板
	ct := NewColorTable(PaletteRGB)
	defer ct.Destroy()

	// 添加调色板条目
	for i := 0; i < 256; i++ {
		ct.AddRGBEntry(i, int16(i), int16(255-i), int16(i/2))
	}

	// 设置到波段
	err := rd.SetBandColorTable(1, ct)
	if err != nil {
		log.Printf("设置调色板失败: %v", err)
	}
}

func ModifyPaletteEntrySample(rd *RasterDataset) {
	// 修改单个调色板条目
	err := rd.ModifyPaletteEntry(1, 0, 255, 0, 0, 255) // 将索引0设为红色
	if err != nil {
		log.Printf("修改调色板条目失败: %v", err)
	}
}

func DeletePaletteSample(rd *RasterDataset) {
	// 删除调色板
	err := rd.DeleteBandColorTable(1)
	if err != nil {
		log.Printf("删除调色板失败: %v", err)
	}
}

func PresetPaletteSample(rd *RasterDataset) {
	// 灰度调色板
	grayCT := CreateGrayscalePalette()
	defer grayCT.Destroy()
	fmt.Println("灰度调色板已创建")

	// 彩虹调色板
	rainbowCT := CreateRainbowPalette()
	defer rainbowCT.Destroy()
	fmt.Println("彩虹调色板已创建")

	// 热力图调色板
	heatmapCT := CreateHeatmapPalette()
	defer heatmapCT.Destroy()
	fmt.Println("热力图调色板已创建")

	// 自定义调色板
	customColors := []PaletteEntry{
		{C1: 0, C2: 0, C3: 255, C4: 255},   // 蓝色
		{C1: 0, C2: 255, C3: 0, C4: 255},   // 绿色
		{C1: 255, C2: 255, C3: 0, C4: 255}, // 黄色
		{C1: 255, C2: 128, C3: 0, C4: 255}, // 橙色
		{C1: 255, C2: 0, C3: 0, C4: 255},   // 红色
	}
	customCT := CreateCustomPalette(customColors)
	defer customCT.Destroy()
	fmt.Println("自定义调色板已创建")
}

func PaletteToRGBSample(rd *RasterDataset) {
	// 调色板图像转RGB
	rgbDataset, err := rd.PaletteToRGB()
	if err != nil {
		log.Printf("调色板转RGB失败: %v", err)
	} else {
		defer rgbDataset.Close()
		fmt.Printf("调色板转RGB成功，转换后波段数: %d\n", rgbDataset.GetBandCount())
	}
}

func RGBToPaletteSample(rd *RasterDataset) {
	// RGB图像转调色板（256色）
	paletteDataset, err := rd.RGBToPalette(256)
	if err != nil {
		log.Printf("RGB转调色板失败: %v", err)
	} else {
		defer paletteDataset.Close()
		fmt.Printf("RGB转调色板成功，转换后波段数: %d\n", paletteDataset.GetBandCount())
	}
}

func ApplyBandOperationsSample(rd *RasterDataset) {
	// 批量操作
	operations := []BandOperation{
		{
			Type:        "add",
			DataType:    BandAlpha8,
			ColorInterp: ColorAlpha,
			NoDataValue: 0,
		},
		{
			Type:        "modify",
			BandIndex:   1,
			ColorInterp: ColorRed,
			NoDataValue: -9999,
		},
	}
	err := rd.ApplyBandOperations(operations)
	if err != nil {
		log.Printf("批量操作失败: %v", err)
	} else {
		fmt.Println("批量操作完成")
	}
}

func GetAllBandsInfoSample(rd *RasterDataset) {
	bandsInfo, err := rd.GetAllBandsInfo()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== 波段信息 ===")
	for _, info := range bandsInfo {
		fmt.Printf("波段 %d: 类型=%s, 颜色解释=%s, NoData=%v (有效:%v)\n",
			info.BandIndex, info.DataType, info.ColorInterp,
			info.NoDataValue, info.HasNoData)
	}
}

func ReadBandDataSample(rd *RasterDataset) {
	fmt.Println("========== 读取波段数据 ==========")
	data, err := rd.ReadBandData(1)
	if err != nil {
		log.Printf("读取波段数据失败: %v", err)
		return
	}
	fmt.Printf("波段1数据大小: %d 像素\n", len(data))
	fmt.Printf("前10个像素值: %v\n", data[:min(10, len(data))])
}
func WriteBandDataSample(rd *RasterDataset) {
	fmt.Println("========== 写入波段数据 ==========")
	// 先读取数据
	data, err := rd.ReadBandData(1)
	if err != nil {
		log.Printf("读取波段数据失败: %v", err)
		return
	}
	// 修改数据（例如：所有值加10）
	for i := range data {
		data[i] += 10
	}
	// 写回
	err = rd.WriteBandData(1, data)
	if err != nil {
		log.Printf("写入波段数据失败: %v", err)
		return
	}
	fmt.Println("波段数据写入成功")
}
func ReadBandDataRectSample(rd *RasterDataset) {
	fmt.Println("========== 读取矩形区域数据 ==========")
	// 读取左上角100x100区域
	data, err := rd.ReadBandDataRect(1, 0, 0, 100, 100)
	if err != nil {
		log.Printf("读取矩形区域失败: %v", err)
		return
	}
	fmt.Printf("矩形区域数据大小: %d 像素\n", len(data))
	fmt.Printf("前10个像素值: %v\n", data[:min(10, len(data))])
}
func WriteBandDataRectSample(rd *RasterDataset) {
	fmt.Println("========== 写入矩形区域数据 ==========")
	// 创建100x100的测试数据
	width, height := 100, 100
	data := make([]float64, width*height)
	for i := range data {
		data[i] = float64(i % 256)
	}
	err := rd.WriteBandDataRect(1, 0, 0, width, height, data)
	if err != nil {
		log.Printf("写入矩形区域失败: %v", err)
		return
	}
	fmt.Println("矩形区域数据写入成功")
}

// ==================== 波段统计测试 ====================
func ComputeBandStatisticsSample(rd *RasterDataset) {
	fmt.Println("========== 计算波段统计信息 ==========")
	stats, err := rd.ComputeBandStatistics(1, false)
	if err != nil {
		log.Printf("计算统计信息失败: %v", err)
		return
	}
	fmt.Printf("波段1统计信息:\n")
	fmt.Printf("  最小值: %.4f\n", stats.Min)
	fmt.Printf("  最大值: %.4f\n", stats.Max)
	fmt.Printf("  均值: %.4f\n", stats.Mean)
	fmt.Printf("  标准差: %.4f\n", stats.StdDev)
}
func GetBandHistogramSample(rd *RasterDataset) {
	fmt.Println("========== 获取波段直方图 ==========")
	// 先获取统计信息确定范围
	stats, err := rd.ComputeBandStatistics(1, true)
	if err != nil {
		log.Printf("获取统计信息失败: %v", err)
		return
	}
	histogram, err := rd.GetBandHistogram(1, 256, stats.Min, stats.Max)
	if err != nil {
		log.Printf("获取直方图失败: %v", err)
		return
	}
	fmt.Printf("直方图桶数: %d\n", len(histogram))
	// 找出最大值的桶
	maxCount := uint64(0)
	maxIdx := 0
	for i, count := range histogram {
		if count > maxCount {
			maxCount = count
			maxIdx = i
		}
	}
	fmt.Printf("最大频率桶: 索引=%d, 计数=%d\n", maxIdx, maxCount)
}

// ==================== 波段运算测试 ====================
func BandMathSample(rd *RasterDataset) {
	fmt.Println("========== 波段数学运算 ==========")
	if rd.GetBandCount() < 2 {
		fmt.Println("需要至少2个波段进行运算")
		return
	}
	// 波段1 + 波段2
	result, err := rd.BandMath(1, 2, BandMathAdd)
	if err != nil {
		log.Printf("波段加法失败: %v", err)
		return
	}
	fmt.Printf("波段1+波段2 结果大小: %d\n", len(result))
	fmt.Printf("前5个结果值: %v\n", result[:min(5, len(result))])
	// 波段1 - 波段2
	result, err = rd.BandMath(1, 2, BandMathSubtract)
	if err != nil {
		log.Printf("波段减法失败: %v", err)
		return
	}
	fmt.Printf("波段1-波段2 前5个结果值: %v\n", result[:min(5, len(result))])
	// 波段1 * 波段2
	result, err = rd.BandMath(1, 2, BandMathMultiply)
	if err != nil {
		log.Printf("波段乘法失败: %v", err)
		return
	}
	fmt.Printf("波段1*波段2 前5个结果值: %v\n", result[:min(5, len(result))])
	// 波段1 / 波段2
	result, err = rd.BandMath(1, 2, BandMathDivide)
	if err != nil {
		log.Printf("波段除法失败: %v", err)
		return
	}
	fmt.Printf("波段1/波段2 前5个结果值: %v\n", result[:min(5, len(result))])
}
func BandMathScalarSample(rd *RasterDataset) {
	fmt.Println("========== 波段与标量运算 ==========")
	// 波段1 * 2
	result, err := rd.BandMathScalar(1, 2.0, BandMathMultiply)
	if err != nil {
		log.Printf("标量乘法失败: %v", err)
		return
	}
	fmt.Printf("波段1 * 2 前5个结果值: %v\n", result[:min(5, len(result))])
	// 波段1 + 100
	result, err = rd.BandMathScalar(1, 100.0, BandMathAdd)
	if err != nil {
		log.Printf("标量加法失败: %v", err)
		return
	}
	fmt.Printf("波段1 + 100 前5个结果值: %v\n", result[:min(5, len(result))])
	// 波段1 ^ 0.5 (开方)
	result, err = rd.BandMathScalar(1, 0.5, BandMathPow)
	if err != nil {
		log.Printf("标量幂运算失败: %v", err)
		return
	}
	fmt.Printf("波段1 ^ 0.5 前5个结果值: %v\n", result[:min(5, len(result))])
}
func NormalizeBandSample(rd *RasterDataset) {
	fmt.Println("========== 波段归一化 ==========")
	// 归一化到0-1范围
	result, err := rd.NormalizeBand(1, 0, 1)
	if err != nil {
		log.Printf("归一化失败: %v", err)
		return
	}
	// 验证结果范围
	minVal, maxVal := result[0], result[0]
	for _, v := range result {
		if !math.IsNaN(v) {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	fmt.Printf("归一化后范围: [%.4f, %.4f]\n", minVal, maxVal)
	// 归一化到0-255范围
	result, err = rd.NormalizeBand(1, 0, 255)
	if err != nil {
		log.Printf("归一化失败: %v", err)
		return
	}
	fmt.Printf("归一化到0-255 前5个结果值: %v\n", result[:min(5, len(result))])
}

// ==================== 植被指数计算测试 ====================
func CalculateNDVISample(rd *RasterDataset) {
	fmt.Println("========== 计算NDVI ==========")
	if rd.GetBandCount() < 4 {
		fmt.Println("需要至少4个波段（假设波段4为NIR，波段3为Red）")
		return
	}
	// 假设波段4为NIR，波段3为Red（根据实际数据调整）
	ndvi, err := rd.CalculateNDVI(4, 3)
	if err != nil {
		log.Printf("计算NDVI失败: %v", err)
		return
	}
	// 统计NDVI范围
	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	validCount := 0
	for _, v := range ndvi {
		if !math.IsNaN(v) {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
			validCount++
		}
	}
	fmt.Printf("NDVI计算完成:\n")
	fmt.Printf("  有效像素数: %d\n", validCount)
	fmt.Printf("  NDVI范围: [%.4f, %.4f]\n", minVal, maxVal)
}
func CalculateNDWISample(rd *RasterDataset) {
	fmt.Println("========== 计算NDWI ==========")
	if rd.GetBandCount() < 4 {
		fmt.Println("需要至少4个波段（假设波段2为Green，波段4为NIR）")
		return
	}
	// 假设波段2为Green，波段4为NIR
	ndwi, err := rd.CalculateNDWI(2, 4)
	if err != nil {
		log.Printf("计算NDWI失败: %v", err)
		return
	}
	// 统计NDWI范围
	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	for _, v := range ndwi {
		if !math.IsNaN(v) {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	fmt.Printf("NDWI范围: [%.4f, %.4f]\n", minVal, maxVal)
}
func CalculateEVISample(rd *RasterDataset) {
	fmt.Println("========== 计算EVI ==========")
	if rd.GetBandCount() < 4 {
		fmt.Println("需要至少4个波段（假设波段4为NIR，波段3为Red，波段1为Blue）")
		return
	}
	// 假设波段4为NIR，波段3为Red，波段1为Blue
	evi, err := rd.CalculateEVI(4, 3, 1)
	if err != nil {
		log.Printf("计算EVI失败: %v", err)
		return
	}
	// 统计EVI范围
	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	for _, v := range evi {
		if !math.IsNaN(v) {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	fmt.Printf("EVI范围: [%.4f, %.4f]\n", minVal, maxVal)
}

// ==================== 波段合并与拆分测试 ====================
func MergeBandsSample(rd *RasterDataset) {
	fmt.Println("========== 合并波段 ==========")
	if rd.GetBandCount() < 3 {
		fmt.Println("需要至少3个波段")
		return
	}
	// 合并波段1、2、3
	mergedDS, err := rd.MergeBandsToNewDataset([]int{1, 2, 3})
	if err != nil {
		log.Printf("合并波段失败: %v", err)
		return
	}
	defer mergedDS.Close()
	fmt.Printf("合并后数据集波段数: %d\n", mergedDS.GetBandCount())
}
func SplitBandsSample(rd *RasterDataset) {
	fmt.Println("========== 拆分波段 ==========")
	splitDS, err := rd.SplitBands()
	if err != nil {
		log.Printf("拆分波段失败: %v", err)
		return
	}
	fmt.Printf("拆分为 %d 个单波段数据集\n", len(splitDS))
	for i, ds := range splitDS {
		fmt.Printf("  数据集%d: 波段数=%d\n", i+1, ds.GetBandCount())
		ds.Close()
	}
}

// ==================== 波段掩膜操作测试 ====================
func CreateMaskFromNoDataSample(rd *RasterDataset) {
	fmt.Println("========== 从NoData创建掩膜 ==========")
	mask, err := rd.CreateMaskFromNoData(1)
	if err != nil {
		log.Printf("创建掩膜失败: %v", err)
		return
	}
	validCount := 0
	for _, v := range mask {
		if v {
			validCount++
		}
	}
	fmt.Printf("掩膜大小: %d\n", len(mask))
	fmt.Printf("有效像素数: %d (%.2f%%)\n", validCount, float64(validCount)/float64(len(mask))*100)
}
func CreateMaskFromThresholdSample(rd *RasterDataset) {
	fmt.Println("========== 从阈值创建掩膜 ==========")
	// 获取统计信息确定阈值范围
	stats, err := rd.ComputeBandStatistics(1, true)
	if err != nil {
		log.Printf("获取统计信息失败: %v", err)
		return
	}
	// 创建均值±标准差范围的掩膜
	minVal := stats.Mean - stats.StdDev
	maxVal := stats.Mean + stats.StdDev
	mask, err := rd.CreateMaskFromThreshold(1, minVal, maxVal)
	if err != nil {
		log.Printf("创建阈值掩膜失败: %v", err)
		return
	}
	validCount := 0
	for _, v := range mask {
		if v {
			validCount++
		}
	}
	fmt.Printf("阈值范围: [%.2f, %.2f]\n", minVal, maxVal)
	fmt.Printf("符合条件像素数: %d (%.2f%%)\n", validCount, float64(validCount)/float64(len(mask))*100)
}
func ApplyMaskSample(rd *RasterDataset) {
	fmt.Println("========== 应用掩膜 ==========")
	// 创建简单掩膜（保留前半部分）
	size := rd.GetWidth() * rd.GetHeight()
	mask := make([]bool, size)
	for i := 0; i < size/2; i++ {
		mask[i] = true
	}
	err := rd.ApplyMask(1, mask, -9999)
	if err != nil {
		log.Printf("应用掩膜失败: %v", err)
		return
	}
	fmt.Println("掩膜应用成功")
}

// ==================== 波段滤波操作测试 ====================
func ApplyFilterSample(rd *RasterDataset) {
	fmt.Println("========== 应用滤波器 ==========")
	// 均值滤波
	result, err := rd.ApplyFilter(1, FilterMean, 3)
	if err != nil {
		log.Printf("均值滤波失败: %v", err)
	} else {
		fmt.Printf("均值滤波完成，结果大小: %d\n", len(result))
	}
	// 中值滤波
	result, err = rd.ApplyFilter(1, FilterMedian, 3)
	if err != nil {
		log.Printf("中值滤波失败: %v", err)
	} else {
		fmt.Printf("中值滤波完成，结果大小: %d\n", len(result))
	}
	// 高斯滤波
	result, err = rd.ApplyFilter(1, FilterGaussian, 5)
	if err != nil {
		log.Printf("高斯滤波失败: %v", err)
	} else {
		fmt.Printf("高斯滤波完成，结果大小: %d\n", len(result))
	}
	// Sobel边缘检测
	result, err = rd.ApplyFilter(1, FilterSobel, 3)
	if err != nil {
		log.Printf("Sobel边缘检测失败: %v", err)
	} else {
		fmt.Printf("Sobel边缘检测完成，结果大小: %d\n", len(result))
	}
	// 拉普拉斯滤波
	result, err = rd.ApplyFilter(1, FilterLaplace, 3)
	if err != nil {
		log.Printf("拉普拉斯滤波失败: %v", err)
	} else {
		fmt.Printf("拉普拉斯滤波完成，结果大小: %d\n", len(result))
	}
	// 最小值滤波
	result, err = rd.ApplyFilter(1, FilterMin, 3)
	if err != nil {
		log.Printf("最小值滤波失败: %v", err)
	} else {
		fmt.Printf("最小值滤波完成，结果大小: %d\n", len(result))
	}
	// 最大值滤波
	result, err = rd.ApplyFilter(1, FilterMax, 3)
	if err != nil {
		log.Printf("最大值滤波失败: %v", err)
	} else {
		fmt.Printf("最大值滤波完成，结果大小: %d\n", len(result))
	}
}

// ==================== 波段重分类测试 ====================
func ReclassifyBandSample(rd *RasterDataset) {
	fmt.Println("========== 波段重分类 ==========")
	// 获取统计信息确定分类范围
	stats, err := rd.ComputeBandStatistics(1, true)
	if err != nil {
		log.Printf("获取统计信息失败: %v", err)
		return
	}
	// 定义重分类规则（分为5类）
	range_ := stats.Max - stats.Min
	step := range_ / 5
	rules := []ReclassifyRule{
		{MinValue: stats.Min, MaxValue: stats.Min + step, NewValue: 1},
		{MinValue: stats.Min + step, MaxValue: stats.Min + 2*step, NewValue: 2},
		{MinValue: stats.Min + 2*step, MaxValue: stats.Min + 3*step, NewValue: 3},
		{MinValue: stats.Min + 3*step, MaxValue: stats.Min + 4*step, NewValue: 4},
		{MinValue: stats.Min + 4*step, MaxValue: stats.Max + 1, NewValue: 5},
	}
	result, err := rd.ReclassifyBand(1, rules, 0)
	if err != nil {
		log.Printf("重分类失败: %v", err)
		return
	}
	// 统计各类别数量
	classCounts := make(map[float64]int)
	for _, v := range result {
		if !math.IsNaN(v) {
			classCounts[v]++
		}
	}
	fmt.Println("重分类结果统计:")
	for class, count := range classCounts {
		fmt.Printf("  类别 %.0f: %d 像素\n", class, count)
	}
}

// ==================== 波段导出测试 ====================
func ExportBandToFileSample(rd *RasterDataset) {
	fmt.Println("========== 导出单波段 ==========")
	err := rd.ExportBandToFile(1, "band1_output.tif", "GTiff")
	if err != nil {
		log.Printf("导出波段失败: %v", err)
		return
	}
	fmt.Println("波段1导出成功: band1_output.tif")
}

// ==================== 波段元数据操作测试 ====================
func BandMetadataSample(rd *RasterDataset) {
	fmt.Println("========== 波段元数据操作 ==========")
	// 设置元数据
	err := rd.SetBandMetadata(1, "DESCRIPTION", "Test Band")
	if err != nil {
		log.Printf("设置元数据失败: %v", err)
	} else {
		fmt.Println("元数据设置成功")
	}
	// 获取元数据
	value, err := rd.GetBandMetadata(1, "DESCRIPTION")
	if err != nil {
		log.Printf("获取元数据失败: %v", err)
	} else {
		fmt.Printf("DESCRIPTION: %s\n", value)
	}
	// 获取所有元数据
	allMeta, err := rd.GetAllBandMetadata(1)
	if err != nil {
		log.Printf("获取所有元数据失败: %v", err)
	} else {
		fmt.Printf("波段1所有元数据: %v\n", allMeta)
	}
}

// ==================== 波段缩放与偏移测试 ====================
func BandScaleOffsetSample(rd *RasterDataset) {
	fmt.Println("========== 波段缩放与偏移 ==========")
	// 设置缩放因子
	err := rd.SetBandScale(1, 0.1)
	if err != nil {
		log.Printf("设置缩放因子失败: %v", err)
	} else {
		fmt.Println("缩放因子设置成功")
	}
	// 设置偏移量
	err = rd.SetBandOffset(1, 100)
	if err != nil {
		log.Printf("设置偏移量失败: %v", err)
	} else {
		fmt.Println("偏移量设置成功")
	}
	// 获取缩放因子
	scale, err := rd.GetBandScale(1)
	if err != nil {
		log.Printf("获取缩放因子失败: %v", err)
	} else {
		fmt.Printf("缩放因子: %.4f\n", scale)
	}
	// 获取偏移量
	offset, err := rd.GetBandOffset(1)
	if err != nil {
		log.Printf("获取偏移量失败: %v", err)
	} else {
		fmt.Printf("偏移量: %.4f\n", offset)
	}
}

// ==================== 波段单位与描述测试 ====================
func BandUnitDescriptionSample(rd *RasterDataset) {
	fmt.Println("========== 波段单位与描述 ==========")
	// 设置单位类型
	err := rd.SetBandUnitType(1, "meters")
	if err != nil {
		log.Printf("设置单位类型失败: %v", err)
	} else {
		fmt.Println("单位类型设置成功")
	}
	// 获取单位类型
	unitType, err := rd.GetBandUnitType(1)
	if err != nil {
		log.Printf("获取单位类型失败: %v", err)
	} else {
		fmt.Printf("单位类型: %s\n", unitType)
	}
	// 设置描述
	err = rd.SetBandDescription(1, "Elevation Data")
	if err != nil {
		log.Printf("设置描述失败: %v", err)
	} else {
		fmt.Println("描述设置成功")
	}
	// 获取描述
	desc, err := rd.GetBandDescription(1)
	if err != nil {
		log.Printf("获取描述失败: %v", err)
	} else {
		fmt.Printf("描述: %s\n", desc)
	}
}

// ==================== 综合测试 ====================
func ComprehensiveIndexCalculationSample(rd *RasterDataset) {
	fmt.Println("========== 综合指数计算与导出 ==========")
	if rd.GetBandCount() < 4 {
		fmt.Println("需要至少4个波段进行综合测试")
		return
	}
	// 计算NDVI
	ndvi, err := rd.CalculateNDVI(4, 3)
	if err != nil {
		log.Printf("计算NDVI失败: %v", err)
		return
	}
	// 归一化NDVI到0-255
	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	for _, v := range ndvi {
		if !math.IsNaN(v) {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	rangeVal := maxVal - minVal
	normalizedNDVI := make([]float64, len(ndvi))
	for i, v := range ndvi {
		if math.IsNaN(v) || rangeVal == 0 {
			normalizedNDVI[i] = 0
		} else {
			normalizedNDVI[i] = ((v - minVal) / rangeVal) * 255
		}
	}
	fmt.Printf("NDVI原始范围: [%.4f, %.4f]\n", minVal, maxVal)
	fmt.Printf("归一化后范围: [0, 255]\n")
	fmt.Println("NDVI计算与归一化完成")
}

func BandCalculatorSample(rd *RasterDataset) {
	calc := rd.NewBandCalculator()

	err := calc.CalculateAndWrite("(b1 + b2) / 4", 3)
	if err != nil {
		return
	}
}

func MosaicWithOptions() {
	ds1, _ := OpenRasterDataset("E:\\影像数据\\00影像\\2025年1月耕林园影像\\510183邛崃市0102\\510183邛崃市1.img", false)
	defer ds1.Close()
	ds2, _ := OpenRasterDataset("E:\\影像数据\\00影像\\2025年1月耕林园影像\\510183邛崃市0102\\510183邛崃市2.img", false)
	defer ds2.Close()
	// 自定义选项
	options := &MosaicOptions{
		ForceBandMatch: true,          // 强制波段匹配
		ResampleMethod: ResampleCubic, // 三次卷积重采样
		NoDataValue:    -9999,         // NoData值
		HasNoData:      true,
		NumThreads:     runtime.NumCPU() / 2, // 使用4个线程
	}
	result, err := MosaicDatasets([]*RasterDataset{ds1, ds2}, options)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()
	fmt.Printf("镶嵌结果: %dx%d, %d波段\n", result.GetWidth(), result.GetHeight(), result.GetBandCount())

}

// ==================== 基础调色示例 ====================

// 综合调色示例
func AdjustColorsSample(ds *RasterDataset) {
	fmt.Println("执行综合调色...")
	params := &ColorAdjustParams{
		Brightness: 0.1,
		Contrast:   0.2,
		Saturation: 0.15,
		Gamma:      1.1,
		Hue:        0,
	}
	adjusted, err := ds.AdjustColors(params)
	if err != nil {
		log.Fatal(err)
	}
	adjusted.ExportToFile("output_adjusted.tif", "GTiff", nil)
	adjusted.Close()
	fmt.Println("综合调色完成: output_adjusted.tif")
}

// 自动色阶示例
func AutoLevelsSample(ds *RasterDataset) {
	fmt.Println("执行自动色阶...")
	autoLeveled, err := ds.AutoLevels(0.5)
	if err != nil {
		log.Fatal(err)
	}
	autoLeveled.ExportToFile("output_autolevels.tif", "GTiff", nil)
	autoLeveled.Close()
	fmt.Println("自动色阶完成: output_autolevels.tif")
}

// 自动白平衡示例
func AutoWhiteBalanceSample(ds *RasterDataset) {
	fmt.Println("执行自动白平衡...")
	whiteBalanced, err := ds.AutoWhiteBalance()
	if err != nil {
		log.Fatal(err)
	}
	whiteBalanced.ExportToFile("output_wb.tif", "GTiff", nil)
	whiteBalanced.Close()
	fmt.Println("自动白平衡完成: output_wb.tif")
}

// CLAHE均衡化示例
func CLAHEEqualizationSample(ds *RasterDataset) {
	fmt.Println("执行CLAHE均衡化...")
	clahe, err := ds.CLAHEEqualization(64, 2.0)
	if err != nil {
		log.Fatal(err)
	}
	clahe.ExportToFile("output_clahe.tif", "GTiff", nil)
	clahe.Close()
	fmt.Println("CLAHE均衡化完成: output_clahe.tif")
}

// 预设鲜艳风格示例
func PresetVividSample(ds *RasterDataset) {
	fmt.Println("执行预设鲜艳风格...")
	vivid, err := ds.PresetVivid()
	if err != nil {
		log.Fatal(err)
	}
	vivid.ExportToFile("output_vivid.tif", "GTiff", nil)
	vivid.Close()
	fmt.Println("预设鲜艳风格完成: output_vivid.tif")
}

// ==================== 匀色示例 ====================

// 直方图匹配示例
func HistogramMatchSample(ds *RasterDataset, refDS *RasterDataset) {
	fmt.Println("执行直方图匹配...")
	histMatched, err := ds.HistogramMatch(refDS, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	histMatched.ExportToFile("output_histmatch.tif", "GTiff", nil)
	histMatched.Close()
	fmt.Println("直方图匹配完成: output_histmatch.tif")
}

// Wallis滤波匀色示例
func WallisFilterSample(ds *RasterDataset) {
	fmt.Println("执行Wallis滤波匀色...")
	wallis, err := ds.WallisFilter(128, 50, 0.8, 0.9, 31)
	if err != nil {
		log.Fatal(err)
	}
	wallis.ExportToFile("output_wallis.tif", "GTiff", nil)
	wallis.Close()
	fmt.Println("Wallis滤波匀色完成: output_wallis.tif")
}

// Dodging匀光示例
func DodgingBalanceSample(ds *RasterDataset) {
	fmt.Println("执行Dodging匀光...")
	dodging, err := ds.DodgingBalance(128, 0.8)
	if err != nil {
		log.Fatal(err)
	}
	dodging.ExportToFile("output_dodging.tif", "GTiff", nil)
	dodging.Close()
	fmt.Println("Dodging匀光完成: output_dodging.tif")
}

// 智能匀色示例
func SmartColorBalanceSample(ds *RasterDataset, refDS *RasterDataset) {
	fmt.Println("执行智能匀色...")
	smart, err := ds.SmartColorBalance(refDS, nil)
	if err != nil {
		log.Fatal(err)
	}
	smart.ExportToFile("output_smart.tif", "GTiff", nil)
	smart.Close()
	fmt.Println("智能匀色完成: output_smart.tif")
}

// ==================== 链式调用示例 ====================

// 色彩处理管道示例
func ColorPipelineSample(ds *RasterDataset) {
	fmt.Println("执行色彩处理管道...")
	result, err := ds.NewColorPipeline().
		AutoLevels(0.5).
		Brightness(0.05).
		Contrast(0.1).
		Saturation(0.1).
		CLAHE(64, 2.0).
		Result()
	if err != nil {
		log.Fatal(err)
	}
	result.ExportToFile("output_pipeline.tif", "GTiff", nil)
	result.Close()
	fmt.Println("色彩处理管道完成: output_pipeline.tif")
}

// 示例1：为没有坐标系的栅格定义投影
func exampleDefineProjection() {
	fmt.Println("=== 示例1：定义投影 ===")

	// 打开没有坐标系的栅格数据
	rd, err := OpenRasterDataset("path/to/no_projection_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	fmt.Printf("原始数据集信息：\n")
	fmt.Printf("  宽度: %d\n", rd.GetWidth())
	fmt.Printf("  高度: %d\n", rd.GetHeight())
	fmt.Printf("  投影: %s\n", rd.projection)

	// 定义为WGS84投影（EPSG:4326）
	newRD, err := rd.DefineProjectionToMemory(4326)
	if err != nil {
		log.Fatalf("Failed to define projection: %v", err)
	}
	defer newRD.Close()

	fmt.Printf("定义投影后的数据集信息：\n")
	fmt.Printf("  投影: %s\n", newRD.projection)
	fmt.Printf("  有地理信息: %v\n", newRD.hasGeoInfo)

	// 导出到文件
	outputPath := "path/to/output_with_projection.tif"
	err = newRD.ExportToFile(outputPath, "GTiff", nil)
	if err != nil {
		log.Fatalf("Failed to export: %v", err)
	}

	fmt.Printf("✓ 已导出到: %s\n\n", outputPath)
}

// 示例2：重投影到指定EPSG
func exampleReprojectToEPSG() {
	fmt.Println("=== 示例2：重投影到EPSG ===")

	// 打开栅格数据
	rd, err := OpenRasterDataset("path/to/source_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	// 源坐标系：UTM Zone 50N (EPSG:32650)
	// 目标坐标系：WGS84 (EPSG:4326)
	srcEPSG := 32650
	dstEPSG := 4326
	outputPath := "path/to/reprojected_to_wgs84.tif"

	fmt.Printf("正在重投影...\n")
	fmt.Printf("  源坐标系: EPSG:%d\n", srcEPSG)
	fmt.Printf("  目标坐标系: EPSG:%d\n", dstEPSG)
	fmt.Printf("  输出路径: %s\n", outputPath)

	// 使用双线性插值重采样
	err = rd.ReprojectToEPSG(srcEPSG, dstEPSG, outputPath, "GTiff", ResampleBilinear)
	if err != nil {
		log.Fatalf("Failed to reproject: %v", err)
	}

	fmt.Printf("✓ 重投影完成\n\n")
}

// 示例3：使用自定义WKT进行重投影
func exampleReprojectWithCustomWKT() {
	fmt.Println("=== 示例3：使用自定义WKT重投影 ===")

	rd, err := OpenRasterDataset("path/to/source_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	// 自定义WKT投影定义（例如：自定义高斯投影）
	customWKT := `PROJCS["Custom_Projection",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",120],PARAMETER["scale_factor",1],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1]]`

	srcEPSG := 4326
	outputPath := "path/to/reprojected_custom.tif"

	fmt.Printf("正在使用自定义WKT重投影...\n")
	fmt.Printf("  源坐标系: EPSG:%d\n", srcEPSG)
	fmt.Printf("  输出路径: %s\n", outputPath)

	err = rd.ReprojectWithCustomWKT(srcEPSG, customWKT, outputPath, "GTiff", ResampleCubic)
	if err != nil {
		log.Fatalf("Failed to reproject with custom WKT: %v", err)
	}

	fmt.Printf("✓ 自定义WKT重投影完成\n\n")
}

// 示例4：使用四参数进行重投影
func exampleReprojectWithAffineParams() {
	fmt.Println("=== 示例4：使用四参数重投影 ===")

	rd, err := OpenRasterDataset("path/to/source_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	// 四参数变换：dx, dy, scale, angle
	params := &AffineParams{
		Dx:     10.5,   // X平移 10.5 米
		Dy:     20.3,   // Y平移 20.3 米
		DScale: 1.0001, // 缩放因子
		Angle:  0.5,    // 旋转 0.5 度
	}

	srcEPSG := 4326
	outputPath := "path/to/reprojected_4param.tif"

	fmt.Printf("正在使用四参数重投影...\n")
	fmt.Printf("  源坐标系: EPSG:%d\n", srcEPSG)
	fmt.Printf("  参数: dx=%.2f, dy=%.2f, scale=%.6f, angle=%.2f\n",
		params.Dx, params.Dy, params.DScale, params.Angle)
	fmt.Printf("  输出路径: %s\n", outputPath)

	err = rd.ReprojectWithAffineParams(srcEPSG, params, "4param", outputPath, "GTiff", ResampleBilinear)
	if err != nil {
		log.Fatalf("Failed to reproject with affine params: %v", err)
	}

	fmt.Printf("✓ 四参数重投影完成\n\n")
}

// 示例5：使用七参数进行重投影
func exampleReprojectWith7Params() {
	fmt.Println("=== 示例5：使用七参数重投影 ===")

	rd, err := OpenRasterDataset("path/to/source_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	// 七参数变换：tx, ty, tz, rx, ry, rz, scale
	params := &AffineParams{
		Tx:    10.5,   // X平移 10.5 米
		Ty:    20.3,   // Y平移 20.3 米
		Tz:    5.1,    // Z平移 5.1 米
		Rx:    0.1,    // X旋转 0.1 度
		Ry:    0.2,    // Y旋转 0.2 度
		Rz:    0.3,    // Z旋转 0.3 度
		Scale: 1.0001, // 缩放因子
	}

	srcEPSG := 4326
	outputPath := "path/to/reprojected_7param.tif"

	fmt.Printf("正在使用七参数重投影...\n")
	fmt.Printf("  源坐标系: EPSG:%d\n", srcEPSG)
	fmt.Printf("  参数: tx=%.2f, ty=%.2f, tz=%.2f, rx=%.2f, ry=%.2f, rz=%.2f, scale=%.6f\n",
		params.Tx, params.Ty, params.Tz, params.Rx, params.Ry, params.Rz, params.Scale)
	fmt.Printf("  输出路径: %s\n", outputPath)

	err = rd.ReprojectWithAffineParams(srcEPSG, params, "7param", outputPath, "GTiff", ResampleLanczos)
	if err != nil {
		log.Fatalf("Failed to reproject with 7 params: %v", err)
	}

	fmt.Printf("✓ 七参数重投影完成\n\n")
}

// 示例6：获取投影WKT
func exampleGetProjectionWKT() {
	fmt.Println("=== 示例6：获取投影WKT ===")

	// 获取WGS84的WKT定义
	wkt, err := GetProjectionWKT(4326)
	if err != nil {
		log.Fatalf("Failed to get projection WKT: %v", err)
	}

	fmt.Printf("EPSG:4326 (WGS84) 的WKT定义：\n")
	fmt.Printf("%s\n\n", wkt)

	// 验证WKT
	isValid := ValidateProjectionWKT(wkt)
	fmt.Printf("WKT有效性: %v\n\n", isValid)
}

// 示例7：批量处理多个栅格文件
func exampleBatchReproject() {
	fmt.Println("=== 示例7：批量重投影 ===")

	sourceDir := "path/to/source/images"
	outputDir := "path/to/output/images"

	files := []string{
		"image1.tif",
		"image2.tif",
		"image3.tif",
	}

	srcEPSG := 32650 // UTM Zone 50N
	dstEPSG := 4326  // WGS84

	for i, file := range files {
		sourcePath := filepath.Join(sourceDir, file)
		outputPath := filepath.Join(outputDir, fmt.Sprintf("reprojected_%s", file))

		fmt.Printf("[%d/%d] 正在处理: %s\n", i+1, len(files), file)

		rd, err := OpenRasterDataset(sourcePath, false)
		if err != nil {
			fmt.Printf("  ✗ 打开失败: %v\n", err)
			continue
		}

		err = rd.ReprojectToEPSG(srcEPSG, dstEPSG, outputPath, "GTiff", ResampleBilinear)
		rd.Close()

		if err != nil {
			fmt.Printf("  ✗ 重投影失败: %v\n", err)
		} else {
			fmt.Printf("  ✓ 完成，输出: %s\n", outputPath)
		}
	}

	fmt.Printf("批量处理完成\n\n")
}

// 示例8：组合操作 - 定义投影后再重投影
func exampleCombinedOperation() {
	fmt.Println("=== 示例8：组合操作 ===")

	// 第一步：打开没有坐标系的栅格
	rd, err := OpenRasterDataset("path/to/no_projection_image.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	fmt.Printf("第一步：定义投影为UTM Zone 50N (EPSG:32650)...\n")

	// 定义投影
	rdWithProj, err := rd.DefineProjectionToMemory(32650)
	if err != nil {
		log.Fatalf("Failed to define projection: %v", err)
	}
	defer rdWithProj.Close()

	fmt.Printf("✓ 投影已定义\n")

	// 第二步：重投影到WGS84
	fmt.Printf("第二步：重投影到WGS84 (EPSG:4326)...\n")

	outputPath := "path/to/final_output.tif"
	err = rdWithProj.ReprojectToEPSG(32650, 4326, outputPath, "GTiff", ResampleCubic)
	if err != nil {
		log.Fatalf("Failed to reproject: %v", err)
	}

	fmt.Printf("✓ 已输出到: %s\n\n", outputPath)
}
func exampleDefineProjectionInPlace() {
	fmt.Println("=== 示例1：直接定义投影 ===")

	// 打开没有坐标系的栅格数据（以更新模式打开）
	rd, err := OpenRasterDataset("C:\\Users\\Administrator\\Desktop\\新建文件夹 (2)\\output.tif", false)
	if err != nil {
		log.Fatalf("Failed to open raster: %v", err)
	}
	defer rd.Close()

	fmt.Printf("原始投影: %s\n", rd.GetProjection())

	// 直接定义为WGS84投影
	err = rd.DefineProjection(3857)
	if err != nil {
		log.Fatalf("Failed to define projection: %v", err)
	}

	fmt.Printf("定义后投影: %s\n", rd.GetProjection())
	fmt.Printf("✓ 投影已直接写入原文件\n\n")
}
