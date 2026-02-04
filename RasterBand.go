// RasterBand.go
package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// BandDataType 波段数据类型
type BandDataType int

const (
	BandGray8   BandDataType = C.BAND_Gray8
	BandGray16  BandDataType = C.BAND_Gray16
	BandRed8    BandDataType = C.BAND_Red8
	BandRed16   BandDataType = C.BAND_Red16
	BandGreen8  BandDataType = C.BAND_Green8
	BandGreen16 BandDataType = C.BAND_Green16
	BandBlue8   BandDataType = C.BAND_Blue8
	BandBlue16  BandDataType = C.BAND_Blue16
	BandAlpha8  BandDataType = C.BAND_Alpha8
	BandAlpha16 BandDataType = C.BAND_Alpha16
	BandInt8    BandDataType = C.BAND_Int8
	BandInt16   BandDataType = C.BAND_Int16
	BandInt32   BandDataType = C.BAND_Int32
	BandInt64   BandDataType = C.BAND_Int64
	BandUInt8   BandDataType = C.BAND_UInt8
	BandUInt16  BandDataType = C.BAND_UInt16
	BandUInt32  BandDataType = C.BAND_UInt32
	BandUInt64  BandDataType = C.BAND_UInt64
	BandReal32  BandDataType = C.BAND_Real32
	BandReal64  BandDataType = C.BAND_Real64
)

// ColorInterpretation 颜色解释
type ColorInterpretation int

const (
	ColorUndefined  ColorInterpretation = C.COLOR_Undefined
	ColorGray       ColorInterpretation = C.COLOR_Gray
	ColorPalette    ColorInterpretation = C.COLOR_Palette
	ColorRed        ColorInterpretation = C.COLOR_Red
	ColorGreen      ColorInterpretation = C.COLOR_Green
	ColorBlue       ColorInterpretation = C.COLOR_Blue
	ColorAlpha      ColorInterpretation = C.COLOR_Alpha
	ColorHue        ColorInterpretation = C.COLOR_Hue
	ColorSaturation ColorInterpretation = C.COLOR_Saturation
	ColorLightness  ColorInterpretation = C.COLOR_Lightness
	ColorCyan       ColorInterpretation = C.COLOR_Cyan
	ColorMagenta    ColorInterpretation = C.COLOR_Magenta
	ColorYellow     ColorInterpretation = C.COLOR_Yellow
	ColorBlack      ColorInterpretation = C.COLOR_Black
)

// PaletteInterpretation 调色板解释类型
type PaletteInterpretation int

const (
	PaletteGray PaletteInterpretation = C.GPI_Gray
	PaletteRGB  PaletteInterpretation = C.GPI_RGB
	PaletteCMYK PaletteInterpretation = C.GPI_CMYK
	PaletteHLS  PaletteInterpretation = C.GPI_HLS
)

// BandInfo 波段信息
type BandInfo struct {
	BandIndex   int
	DataType    BandDataType
	ColorInterp ColorInterpretation
	NoDataValue float64
	HasNoData   bool
	MinValue    float64
	MaxValue    float64
	HasStats    bool
}

// PaletteEntry 调色板条目
type PaletteEntry struct {
	C1 int16 // Red or Gray
	C2 int16 // Green
	C3 int16 // Blue
	C4 int16 // Alpha
}

// PaletteInfo 调色板信息
type PaletteInfo struct {
	EntryCount int
	InterpType PaletteInterpretation
	Entries    []PaletteEntry
}

// ColorTable 调色板句柄
type ColorTable struct {
	handle C.GDALColorTableH
}

// ==================== 波段信息获取 ====================

// GetBandInfo 获取指定波段信息
func (rd *RasterDataset) GetBandInfo(bandIndex int) (*BandInfo, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cInfo := C.getBandInfo(activeDS, C.int(bandIndex))
	if cInfo == nil {
		return nil, fmt.Errorf("failed to get band info for band %d", bandIndex)
	}
	defer C.freeBandInfo(cInfo)

	info := &BandInfo{
		BandIndex:   int(cInfo.bandIndex),
		DataType:    BandDataType(C.gdalToBandDataType(cInfo.dataType)),
		ColorInterp: ColorInterpretation(C.gdalToColorInterp(cInfo.colorInterp)),
		NoDataValue: float64(cInfo.noDataValue),
		HasNoData:   cInfo.hasNoData != 0,
		MinValue:    float64(cInfo.minValue),
		MaxValue:    float64(cInfo.maxValue),
		HasStats:    cInfo.hasStats != 0,
	}

	return info, nil
}

// GetAllBandsInfo 获取所有波段信息
func (rd *RasterDataset) GetAllBandsInfo() ([]BandInfo, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	var bandCount C.int
	cInfos := C.getAllBandsInfo(activeDS, &bandCount)
	if cInfos == nil {
		return nil, fmt.Errorf("failed to get bands info")
	}
	defer C.freeBandInfo(cInfos)

	count := int(bandCount)
	infos := make([]BandInfo, count)

	// 将C数组转换为Go切片
	cInfoSlice := (*[1 << 20]C.BandInfo)(unsafe.Pointer(cInfos))[:count:count]

	// 检查是否全部未定义
	allUndefined := true
	for i := 0; i < count; i++ {
		infos[i] = BandInfo{
			BandIndex:   int(cInfoSlice[i].bandIndex),
			DataType:    BandDataType(C.gdalToBandDataType(cInfoSlice[i].dataType)),
			ColorInterp: ColorInterpretation(C.gdalToColorInterp(cInfoSlice[i].colorInterp)),
			NoDataValue: float64(cInfoSlice[i].noDataValue),
			HasNoData:   cInfoSlice[i].hasNoData != 0,
			MinValue:    float64(cInfoSlice[i].minValue),
			MaxValue:    float64(cInfoSlice[i].maxValue),
			HasStats:    cInfoSlice[i].hasStats != 0,
		}
		if infos[i].ColorInterp != ColorUndefined {
			allUndefined = false
		}
	}

	// 智能推断：针对颜色解释全部未定义的情况
	if allUndefined && rd.needsColorInterpInference() {
		rd.inferColorInterpretation(infos)
	}

	return infos, nil
}

// needsColorInterpInference 判断是否需要智能推断颜色解释
func (rd *RasterDataset) needsColorInterpInference() bool {
	// 获取驱动名称
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return false
	}

	driver := C.GDALGetDatasetDriver(activeDS)
	if driver == nil {
		return false
	}

	driverName := C.GoString(C.GDALGetDriverShortName(driver))

	// 需要推断的格式列表
	inferFormats := map[string]bool{
		"HFA":      true, // ERDAS IMAGINE (.img)
		"ENVI":     true, // ENVI格式
		"EHdr":     true, // ESRI BIL/BIP/BSQ
		"BMP":      true, // BMP格式
		"PCRaster": true,
	}

	return inferFormats[driverName]
}

// inferColorInterpretation 智能推断颜色解释
func (rd *RasterDataset) inferColorInterpretation(infos []BandInfo) {
	bandCount := len(infos)

	switch bandCount {
	case 1:
		// 单波段：检查是否有调色板
		palette, _ := rd.GetPaletteInfo(1)
		if palette != nil && palette.EntryCount > 0 {
			infos[0].ColorInterp = ColorPalette
		} else {
			infos[0].ColorInterp = ColorGray
		}
	case 2:
		// 双波段：灰度 + Alpha
		infos[0].ColorInterp = ColorGray
		infos[1].ColorInterp = ColorAlpha
	case 3:
		// 三波段：RGB
		infos[0].ColorInterp = ColorRed
		infos[1].ColorInterp = ColorGreen
		infos[2].ColorInterp = ColorBlue
	case 4:
		// 四波段：RGBA
		infos[0].ColorInterp = ColorRed
		infos[1].ColorInterp = ColorGreen
		infos[2].ColorInterp = ColorBlue
		infos[3].ColorInterp = ColorAlpha
	default:
		// 多光谱影像，保持Undefined
	}
}

// GetBandCount 获取波段数量
func (rd *RasterDataset) GetBandCount() int {
	return rd.bandCount
}

// ==================== 波段操作 ====================

// AddBand 添加新波段
func (rd *RasterDataset) AddBand(dataType BandDataType, colorInterp ColorInterpretation, noDataValue float64) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	newDS := C.addBandToDataset(activeDS, C.BandDataType(dataType),
		C.ColorInterpretation(colorInterp), C.double(noDataValue))
	if newDS == nil {
		return fmt.Errorf("failed to add band")
	}

	// 更新数据集引用
	rd.replaceDataset(newDS)
	rd.bandCount++

	return nil
}

// RemoveBand 删除指定波段
func (rd *RasterDataset) RemoveBand(bandIndex int) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}

	if rd.bandCount <= 1 {
		return fmt.Errorf("cannot remove the last band")
	}

	newDS := C.removeBandFromDataset(activeDS, C.int(bandIndex))
	if newDS == nil {
		return fmt.Errorf("failed to remove band %d", bandIndex)
	}

	rd.replaceDataset(newDS)
	rd.bandCount--

	return nil
}

// SetBandColorInterpretation 设置波段颜色解释
// RasterBand.go

// SetBandColorInterpretation 设置波段颜色解释
func (rd *RasterDataset) SetBandColorInterpretation(bandIndex int, colorInterp ColorInterpretation) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	if bandIndex < 1 || bandIndex > rd.bandCount {
		return fmt.Errorf("invalid band index: %d", bandIndex)
	}
	band := C.GDALGetRasterBand(activeDS, C.int(bandIndex))
	if band == nil {
		return fmt.Errorf("failed to get band %d", bandIndex)
	}

	C.GDALSetRasterColorInterpretation(band, C.colorInterpToGDAL(C.ColorInterpretation(colorInterp)))
	return nil
}

// SetBandNoDataValue 设置波段NoData值
func (rd *RasterDataset) SetBandNoDataValue(bandIndex int, noDataValue float64) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	result := C.setBandNoDataValue(activeDS, C.int(bandIndex), C.double(noDataValue))
	if result == 0 {
		return fmt.Errorf("failed to set nodata value for band %d", bandIndex)
	}
	return nil
}

// DeleteBandNoDataValue 删除波段NoData值
func (rd *RasterDataset) DeleteBandNoDataValue(bandIndex int) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	result := C.deleteBandNoDataValue(activeDS, C.int(bandIndex))
	if result == 0 {
		return fmt.Errorf("failed to delete nodata value for band %d", bandIndex)
	}
	return nil
}

// ReorderBands 重排波段顺序
func (rd *RasterDataset) ReorderBands(bandOrder []int) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	if len(bandOrder) == 0 {
		return fmt.Errorf("band order cannot be empty")
	}

	// 转换为C数组
	cBandOrder := make([]C.int, len(bandOrder))
	for i, v := range bandOrder {
		cBandOrder[i] = C.int(v)
	}

	newDS := C.reorderBands(activeDS, &cBandOrder[0], C.int(len(bandOrder)))
	if newDS == nil {
		return fmt.Errorf("failed to reorder bands")
	}

	rd.replaceDataset(newDS)
	rd.bandCount = len(bandOrder)

	return nil
}

// ConvertBandDataType 转换波段数据类型
func (rd *RasterDataset) ConvertBandDataType(bandIndex int, newType BandDataType) error {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	newDS := C.convertBandDataType(activeDS, C.int(bandIndex), C.BandDataType(newType))
	if newDS == nil {
		return fmt.Errorf("failed to convert band %d data type", bandIndex)
	}

	rd.replaceDataset(newDS)

	return nil
}

// CopyBandData 复制波段数据到另一个数据集
func (rd *RasterDataset) CopyBandData(srcBandIndex int, dstDataset *RasterDataset, dstBandIndex int) error {
	srcDS := rd.GetActiveDataset()
	dstDS := dstDataset.GetActiveDataset()

	if srcDS == nil || dstDS == nil {
		return fmt.Errorf("dataset is nil")
	}

	result := C.copyBandData(srcDS, C.int(srcBandIndex), dstDS, C.int(dstBandIndex))
	if result == 0 {
		return fmt.Errorf("failed to copy band data")
	}

	return nil
}

// ==================== 调色板操作 ====================

// GetPaletteInfo 获取调色板信息
func (rd *RasterDataset) GetPaletteInfo(bandIndex int) (*PaletteInfo, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	cInfo := C.getPaletteInfo(activeDS, C.int(bandIndex))
	if cInfo == nil {
		return nil, fmt.Errorf("no palette found for band %d", bandIndex)
	}
	defer C.freePaletteInfo(cInfo)

	info := &PaletteInfo{
		EntryCount: int(cInfo.entryCount),
		InterpType: PaletteInterpretation(cInfo.interpType),
		Entries:    make([]PaletteEntry, int(cInfo.entryCount)),
	}

	if cInfo.entryCount > 0 && cInfo.entries != nil {
		cEntries := (*[1 << 20]C.PaletteEntry)(unsafe.Pointer(cInfo.entries))[:info.EntryCount:info.EntryCount]
		for i := 0; i < info.EntryCount; i++ {
			info.Entries[i] = PaletteEntry{
				C1: int16(cEntries[i].c1),
				C2: int16(cEntries[i].c2),
				C3: int16(cEntries[i].c3),
				C4: int16(cEntries[i].c4),
			}
		}
	}

	return info, nil
}

// NewColorTable 创建新调色板
func NewColorTable(interpType PaletteInterpretation) *ColorTable {
	handle := C.createColorTable(C.GDALPaletteInterp(interpType))
	if handle == nil {
		return nil
	}
	return &ColorTable{handle: handle}
}

// AddEntry 添加调色板条目
func (ct *ColorTable) AddEntry(index int, r, g, b, a int16) error {
	if ct.handle == nil {
		return fmt.Errorf("color table is nil")
	}

	result := C.addPaletteEntry(ct.handle, C.int(index), C.short(r), C.short(g), C.short(b), C.short(a))
	if result == 0 {
		return fmt.Errorf("failed to add palette entry at index %d", index)
	}

	return nil
}

// AddRGBEntry 添加RGB调色板条目（Alpha默认255）
func (ct *ColorTable) AddRGBEntry(index int, r, g, b int16) error {
	return ct.AddEntry(index, r, g, b, 255)
}

// Destroy 销毁调色板
func (ct *ColorTable) Destroy() {
	if ct.handle != nil {
		C.GDALDestroyColorTable(ct.handle)
		ct.handle = nil
	}
}

// SetBandColorTable 设置波段调色板
func (rd *RasterDataset) SetBandColorTable(bandIndex int, colorTable *ColorTable) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	if colorTable == nil || colorTable.handle == nil {
		return fmt.Errorf("color table is nil")
	}
	result := C.setBandColorTable(activeDS, C.int(bandIndex), colorTable.handle)
	if result == 0 {
		return fmt.Errorf("failed to set color table for band %d", bandIndex)
	}
	return nil
}

// DeleteBandColorTable 删除波段调色板
func (rd *RasterDataset) DeleteBandColorTable(bandIndex int) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	result := C.deleteBandColorTable(activeDS, C.int(bandIndex))
	if result == 0 {
		return fmt.Errorf("failed to delete color table for band %d", bandIndex)
	}
	return nil
}

// ModifyPaletteEntry 修改调色板条目
func (rd *RasterDataset) ModifyPaletteEntry(bandIndex, entryIndex int, r, g, b, a int16) error {
	if err := rd.ensureMemoryCopy(); err != nil {
		return err
	}

	activeDS := rd.GetActiveDataset()
	result := C.modifyPaletteEntry(activeDS, C.int(bandIndex), C.int(entryIndex),
		C.short(r), C.short(g), C.short(b), C.short(a))
	if result == 0 {
		return fmt.Errorf("failed to modify palette entry %d for band %d", entryIndex, bandIndex)
	}
	return nil
}

// PaletteToRGB 将调色板图像转换为RGB图像
func (rd *RasterDataset) PaletteToRGB() (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	newDS := C.paletteToRGB(activeDS)
	if newDS == nil {
		return nil, fmt.Errorf("failed to convert palette to RGB")
	}

	// 创建新的RasterDataset
	newRD := &RasterDataset{
		dataset:       newDS,
		warpedDS:      nil,
		width:         rd.width,
		height:        rd.height,
		bandCount:     4, // RGBA
		bounds:        rd.bounds,
		projection:    rd.projection,
		isReprojected: rd.isReprojected,
		hasGeoInfo:    rd.hasGeoInfo,
	}

	return newRD, nil
}

// RGBToPalette 将RGB图像转换为调色板图像
func (rd *RasterDataset) RGBToPalette(colorCount int) (*RasterDataset, error) {
	activeDS := rd.GetActiveDataset()
	if activeDS == nil {
		return nil, fmt.Errorf("dataset is nil")
	}

	if colorCount <= 0 || colorCount > 256 {
		return nil, fmt.Errorf("color count must be between 1 and 256")
	}

	newDS := C.rgbToPalette(activeDS, C.int(colorCount))
	if newDS == nil {
		return nil, fmt.Errorf("failed to convert RGB to palette")
	}

	newRD := &RasterDataset{
		dataset:       newDS,
		warpedDS:      nil,
		width:         rd.width,
		height:        rd.height,
		bandCount:     1,
		bounds:        rd.bounds,
		projection:    rd.projection,
		isReprojected: rd.isReprojected,
		hasGeoInfo:    rd.hasGeoInfo,
	}

	return newRD, nil
}

// ==================== 辅助方法 ====================

// replaceDataset 替换当前数据集
func (rd *RasterDataset) replaceDataset(newDS C.GDALDatasetH) {
	// 关闭旧的warped数据集
	if rd.warpedDS != nil {
		C.GDALClose(rd.warpedDS)
		rd.warpedDS = nil
	}

	// 如果原来是重投影的，新数据集也作为warped
	if rd.isReprojected {
		rd.warpedDS = newDS
	} else {
		// 关闭旧的原始数据集
		if rd.dataset != nil {
			C.GDALClose(rd.dataset)
		}
		rd.dataset = newDS
	}

	// 更新尺寸信息
	rd.width = int(C.GDALGetRasterXSize(newDS))
	rd.height = int(C.GDALGetRasterYSize(newDS))
}

// ==================== 批量操作 ====================

// BandOperation 波段操作配置
type BandOperation struct {
	Type        string              // "add", "remove", "modify"
	BandIndex   int                 // 目标波段索引
	DataType    BandDataType        // 新波段数据类型
	ColorInterp ColorInterpretation // 颜色解释
	NoDataValue float64             // NoData值
}

// ApplyBandOperations 批量应用波段操作
func (rd *RasterDataset) ApplyBandOperations(operations []BandOperation) error {
	for i, op := range operations {
		var err error
		switch op.Type {
		case "add":
			err = rd.AddBand(op.DataType, op.ColorInterp, op.NoDataValue)
		case "remove":
			err = rd.RemoveBand(op.BandIndex)
		case "modify":
			err = rd.SetBandColorInterpretation(op.BandIndex, op.ColorInterp)
			if err == nil && op.NoDataValue != 0 {
				err = rd.SetBandNoDataValue(op.BandIndex, op.NoDataValue)
			}
		default:
			err = fmt.Errorf("unknown operation type: %s", op.Type)
		}

		if err != nil {
			return fmt.Errorf("operation %d failed: %w", i, err)
		}
	}

	return nil
}

// CreateGrayscalePalette 创建灰度调色板
func CreateGrayscalePalette() *ColorTable {
	ct := NewColorTable(PaletteGray)
	if ct == nil {
		return nil
	}

	for i := 0; i < 256; i++ {
		ct.AddEntry(i, int16(i), int16(i), int16(i), 255)
	}

	return ct
}

// CreateRainbowPalette 创建彩虹调色板
func CreateRainbowPalette() *ColorTable {
	ct := NewColorTable(PaletteRGB)
	if ct == nil {
		return nil
	}

	for i := 0; i < 256; i++ {
		var r, g, b int16
		if i < 43 {
			r = 255
			g = int16(i * 6)
			b = 0
		} else if i < 85 {
			r = int16(255 - (i-43)*6)
			g = 255
			b = 0
		} else if i < 128 {
			r = 0
			g = 255
			b = int16((i - 85) * 6)
		} else if i < 170 {
			r = 0
			g = int16(255 - (i-128)*6)
			b = 255
		} else if i < 213 {
			r = int16((i - 170) * 6)
			g = 0
			b = 255
		} else {
			r = 255
			g = 0
			b = int16(255 - (i-213)*6)
		}
		ct.AddEntry(i, r, g, b, 255)
	}

	return ct
}

// CreateHeatmapPalette 创建热力图调色板
func CreateHeatmapPalette() *ColorTable {
	ct := NewColorTable(PaletteRGB)
	if ct == nil {
		return nil
	}

	for i := 0; i < 256; i++ {
		var r, g, b int16
		if i < 64 {
			r = 0
			g = 0
			b = int16(128 + i*2)
		} else if i < 128 {
			r = 0
			g = int16((i - 64) * 4)
			b = 255
		} else if i < 192 {
			r = int16((i - 128) * 4)
			g = 255
			b = int16(255 - (i-128)*4)
		} else {
			r = 255
			g = int16(255 - (i-192)*4)
			b = 0
		}
		ct.AddEntry(i, r, g, b, 255)
	}

	return ct
}

// CreateCustomPalette 从颜色数组创建自定义调色板
func CreateCustomPalette(colors []PaletteEntry) *ColorTable {
	ct := NewColorTable(PaletteRGB)
	if ct == nil {
		return nil
	}

	for i, color := range colors {
		if i >= 256 {
			break
		}
		ct.AddEntry(i, color.C1, color.C2, color.C3, color.C4)
	}

	return ct
}

// ==================== 数据类型辅助函数 ====================

// GetDataTypeName 获取数据类型名称
func (dt BandDataType) String() string {
	switch dt {
	case BandGray8:
		return "Gray8"
	case BandGray16:
		return "Gray16"
	case BandRed8:
		return "Red8"
	case BandRed16:
		return "Red16"
	case BandGreen8:
		return "Green8"
	case BandGreen16:
		return "Green16"
	case BandBlue8:
		return "Blue8"
	case BandBlue16:
		return "Blue16"
	case BandAlpha8:
		return "Alpha8"
	case BandAlpha16:
		return "Alpha16"
	case BandInt8:
		return "Int8"
	case BandInt16:
		return "Int16"
	case BandInt32:
		return "Int32"
	case BandInt64:
		return "Int64"
	case BandUInt8:
		return "UInt8"
	case BandUInt16:
		return "UInt16"
	case BandUInt32:
		return "UInt32"
	case BandUInt64:
		return "UInt64"
	case BandReal32:
		return "Real32"
	case BandReal64:
		return "Real64"
	default:
		return "Unknown"
	}
}

// GetColorInterpName 获取颜色解释名称
func (ci ColorInterpretation) String() string {
	switch ci {
	case ColorUndefined:
		return "Undefined"
	case ColorGray:
		return "Gray"
	case ColorPalette:
		return "Palette"
	case ColorRed:
		return "Red"
	case ColorGreen:
		return "Green"
	case ColorBlue:
		return "Blue"
	case ColorAlpha:
		return "Alpha"
	case ColorHue:
		return "Hue"
	case ColorSaturation:
		return "Saturation"
	case ColorLightness:
		return "Lightness"
	case ColorCyan:
		return "Cyan"
	case ColorMagenta:
		return "Magenta"
	case ColorYellow:
		return "Yellow"
	case ColorBlack:
		return "Black"
	default:
		return "Unknown"
	}
}

// GetBytesPerPixel 获取每像素字节数
func (dt BandDataType) GetBytesPerPixel() int {
	switch dt {
	case BandGray8, BandRed8, BandGreen8, BandBlue8, BandAlpha8, BandInt8, BandUInt8:
		return 1
	case BandGray16, BandRed16, BandGreen16, BandBlue16, BandAlpha16, BandInt16, BandUInt16:
		return 2
	case BandInt32, BandUInt32, BandReal32:
		return 4
	case BandInt64, BandUInt64, BandReal64:
		return 8
	default:
		return 1
	}
}
