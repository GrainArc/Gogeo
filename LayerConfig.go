/*
Copyright (C) 2025 [GrainArc]

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// 修改分块结果结构，标记边界要素
type TileResult struct {
	TileIndex        int
	InteriorFeatures []C.OGRFeatureH // 完全在分块内部的要素
	BorderFeatures   []C.OGRFeatureH // 可能跨越边界的要素
	Error            error
	ProcessTime      time.Duration
}

// ParallelGeosConfig 并行相交分析配置
type ParallelGeosConfig struct {
	TileCount  int // 分块数量 (N*N)
	MaxWorkers int // 最大工作协程数

	IsMergeTile      bool                     // 是否合并瓦片
	ProgressCallback ProgressCallback         // 进度回调
	PrecisionConfig  *GeometryPrecisionConfig // 几何精度配置
}

// ProgressCallback 进度回调函数类型
// 返回值：true继续执行，false取消执行
type ProgressCallback func(complete float64, message string) bool

// ProgressData 进度数据结构，用于在C和Go之间传递信息
type ProgressData struct {
	callback  ProgressCallback
	cancelled bool
	mutex     sync.RWMutex
}

// 全局进度数据映射，用于在C回调中找到对应的Go回调
var (
	progressDataMap   = make(map[uintptr]*ProgressData)
	progressDataMutex sync.RWMutex
	progressIDCounter int64
)

// handleProgressUpdate 处理来自C的进度更新
//
//export handleProgressUpdate
func handleProgressUpdate(complete C.double, message *C.char, progressArg unsafe.Pointer) C.int {
	// 线程安全地获取进度数据
	progressDataMutex.RLock()
	data, exists := progressDataMap[uintptr(progressArg)]
	progressDataMutex.RUnlock()

	if !exists || data == nil {
		return 1 // 继续执行
	}

	// 检查是否已被取消
	data.mutex.RLock()
	if data.cancelled {
		data.mutex.RUnlock()
		return 0 // 取消执行
	}
	callback := data.callback
	data.mutex.RUnlock()

	if callback != nil {
		// 转换消息字符串
		msg := ""
		if message != nil {
			msg = C.GoString(message)
		}

		// 调用Go回调函数
		shouldContinue := callback(float64(complete), msg)
		if !shouldContinue {
			// 用户取消操作
			data.mutex.Lock()
			data.cancelled = true
			data.mutex.Unlock()
			return 0 // 取消执行
		}
	}

	return 1 // 继续执行
}

// GeosAnalysisResult 相交分析结果
type GeosAnalysisResult struct {
	OutputLayer *GDALLayer
	ResultCount int
}

// FieldsInfo 字段信息结构
type FieldsInfo struct {
	Name      string
	Type      C.OGRFieldType
	FromTable string // 标记字段来源表
}

// FieldMergeStrategy 字段合并策略枚举
type FieldMergeStrategy int

const (
	// UseTable1Fields 只使用第一个表的字段
	UseTable1Fields FieldMergeStrategy = iota
	// UseTable2Fields 只使用第二个表的字段
	UseTable2Fields
	// MergePreferTable1 合并字段，冲突时优先使用table1
	MergePreferTable1
	// MergePreferTable2 合并字段，冲突时优先使用table2
	MergePreferTable2
	// MergeWithPrefix 合并字段，使用前缀区分来源
	MergeWithPrefix
)

func (s FieldMergeStrategy) String() string {
	switch s {
	case UseTable1Fields:
		return "只使用表1字段"
	case UseTable2Fields:
		return "只使用表2字段"
	case MergePreferTable1:
		return "合并字段(优先表1)"
	case MergePreferTable2:
		return "合并字段(优先表2)"
	case MergeWithPrefix:
		return "合并字段(使用前缀区分)"
	default:
		return "未知策略"
	}
}

// 获取要素几何体的标准化WKT表示，用于去重比较
func getFeatureGeometryWKT(feature C.OGRFeatureH) string {
	if feature == nil {
		return ""
	}

	geom := C.OGR_F_GetGeometryRef(feature)
	if geom == nil {
		return ""
	}

	// 创建几何体副本进行标准化处理
	geomClone := C.OGR_G_Clone(geom)
	if geomClone == nil {
		return ""
	}
	defer C.OGR_G_DestroyGeometry(geomClone)

	// 标准化几何体 - 这会统一坐标顺序和格式
	C.OGR_G_FlattenTo2D(geomClone) // 转为2D，去除Z坐标差异

	// 使用更精确的WKT导出，设置精度
	var wkt *C.char
	err := C.OGR_G_ExportToWkt(geomClone, &wkt)
	if err != C.OGRERR_NONE || wkt == nil {
		return ""
	}

	result := C.GoString(wkt)
	C.CPLFree(unsafe.Pointer(wkt))
	return result
}

// 添加去重后的边界要素
func addDeduplicatedBorderFeatures(borderFeaturesMap map[string]*BorderFeatureInfo, resultLayer *GDALLayer, progressCallback ProgressCallback) error {
	totalFeatures := len(borderFeaturesMap)
	addedCount := 0

	i := 0
	for _, info := range borderFeaturesMap {
		if info.Feature != nil {
			err := C.OGR_L_CreateFeature(resultLayer.layer, info.Feature)
			if err == C.OGRERR_NONE {
				addedCount++
			}
			C.OGR_F_Destroy(info.Feature)
			info.Feature = nil
		}

		i++

		// 更新进度
		if progressCallback != nil && i%100 == 0 {
			progress := 0.9 + 0.1*float64(i)/float64(totalFeatures)
			message := fmt.Sprintf("正在添加去重后的边界要素 %d/%d", i, totalFeatures)
			if !progressCallback(progress, message) {
				return fmt.Errorf("操作被用户取消")
			}
		}
	}

	fmt.Printf("成功添加边界要素 %d 个\n", addedCount)
	return nil
}

// 清理资源的辅助函数
func cleanupTileResult(result *TileResult) {
	for _, feature := range result.InteriorFeatures {
		if feature != nil {
			C.OGR_F_Destroy(feature)
		}
	}
	for _, feature := range result.BorderFeatures {
		if feature != nil {
			C.OGR_F_Destroy(feature)
		}
	}
}

func cleanupBorderFeaturesMap(borderFeaturesMap map[string]*BorderFeatureInfo) {
	for _, info := range borderFeaturesMap {
		if info.Feature != nil {
			C.OGR_F_Destroy(info.Feature)
		}
	}
}

// 添加图层字段到结果图层
func addLayerFields(resultLayer, sourceLayer *GDALLayer, prefix string) error {
	sourceDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer.layer)
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		fieldName := C.OGR_Fld_GetNameRef(fieldDefn)
		fieldType := C.OGR_Fld_GetType(fieldDefn)

		originalFieldName := C.GoString(fieldName)

		// 构建字段名（可能带前缀）
		var newFieldName string
		if prefix != "" {

			newFieldName = prefix + originalFieldName

		} else {

			newFieldName = originalFieldName
		}

		// 检查字段是否已存在
		newFieldNameC := C.CString(newFieldName)
		fieldIndex := C.OGR_FD_GetFieldIndex(resultDefn, newFieldNameC)

		// 如果字段不存在（返回-1），则添加字段
		if fieldIndex == -1 {
			// 创建新字段
			newFieldDefn := C.OGR_Fld_Create(newFieldNameC, fieldType)

			// 复制字段属性
			C.OGR_Fld_SetWidth(newFieldDefn, C.OGR_Fld_GetWidth(fieldDefn))
			C.OGR_Fld_SetPrecision(newFieldDefn, C.OGR_Fld_GetPrecision(fieldDefn))

			// 添加字段到结果图层
			err := C.OGR_L_CreateField(resultLayer.layer, newFieldDefn, 1)

			// 清理字段定义资源
			C.OGR_Fld_Destroy(newFieldDefn)

			if err != C.OGRERR_NONE {
				C.free(unsafe.Pointer(newFieldNameC))
				return fmt.Errorf("创建字段 %s 失败，错误代码: %d", newFieldName, int(err))
			}
		}

		// 清理字段名资源
		C.free(unsafe.Pointer(newFieldNameC))
	}

	return nil
}

// 获取精度标志位
func (config *GeometryPrecisionConfig) getFlags() C.int {
	var flags C.int = 0

	return flags
}

// BorderFeatureInfo 边界要素信息
type BorderFeatureInfo struct {
	Feature     C.OGRFeatureH
	TileIndices []int  // 该要素出现在哪些分块中
	GeometryWKT string // 几何体的WKT表示，用于去重比较
}

// addUniqueIdentifierFieldForErase 为输入图层添加唯一标识字段
func addIdentifierField(inputLayer *GDALLayer, AttName string) error {
	// 为输入图层创建带标识字段的副本
	newLayer, err := createLayerWithIdentifierField(inputLayer, AttName)
	if err != nil {
		return fmt.Errorf("为图层 %s 创建带标识字段的副本失败: %v", AttName, err)
	}

	// 替换原始图层
	inputLayer.Close()
	*inputLayer = *newLayer

	return nil
}

// createLayerWithIdentifierFieldForErase 创建一个带有标识字段的新图层（用于擦除操作）
func createLayerWithIdentifierField(sourceLayer *GDALLayer, identifierFieldName string) (*GDALLayer, error) {
	// 创建内存数据源
	driverName := C.CString("MEM")
	driver := C.OGRGetDriverByName(driverName)
	C.free(unsafe.Pointer(driverName))

	if driver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	dataSourceName := C.CString("")
	dataSource := C.OGR_Dr_CreateDataSource(driver, dataSourceName, nil)
	C.free(unsafe.Pointer(dataSourceName))

	if dataSource == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层的空间参考系统
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)

	// 创建新图层
	layerName := C.CString("temp_layer")
	geomType := C.OGR_FD_GetGeomType(sourceLayerDefn)
	newLayer := C.OGR_DS_CreateLayer(dataSource, layerName, sourceSRS, geomType, nil)
	C.free(unsafe.Pointer(layerName))

	if newLayer == nil {
		C.OGR_DS_Destroy(dataSource)
		return nil, fmt.Errorf("无法创建新图层")
	}

	// 复制原有字段定义
	fieldCount := C.OGR_FD_GetFieldCount(sourceLayerDefn)
	for i := C.int(0); i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, i)
		if C.OGR_L_CreateField(newLayer, fieldDefn, 1) != C.OGRERR_NONE {
			C.OGR_DS_Destroy(dataSource)
			return nil, fmt.Errorf("复制字段定义失败")
		}
	}

	// 添加标识字段
	identifierFieldNameC := C.CString(identifierFieldName)
	identifierFieldDefn := C.OGR_Fld_Create(identifierFieldNameC, C.OFTInteger64)
	defer C.OGR_Fld_Destroy(identifierFieldDefn)
	defer C.free(unsafe.Pointer(identifierFieldNameC))

	if C.OGR_L_CreateField(newLayer, identifierFieldDefn, 1) != C.OGRERR_NONE {
		C.OGR_DS_Destroy(dataSource)
		return nil, fmt.Errorf("创建标识字段失败")
	}

	// 获取标识字段索引
	newLayerDefn := C.OGR_L_GetLayerDefn(newLayer)
	identifierFieldIndex := C.OGR_FD_GetFieldIndex(newLayerDefn, identifierFieldNameC)

	// 复制要素并添加标识字段值
	sourceLayer.ResetReading()
	var featureID int64 = 1

	sourceLayer.IterateFeatures(func(sourceFeature C.OGRFeatureH) {
		// 创建新要素
		newFeature := C.OGR_F_Create(newLayerDefn)
		if newFeature == nil {
			return
		}
		defer C.OGR_F_Destroy(newFeature)

		// 复制几何体
		geom := C.OGR_F_GetGeometryRef(sourceFeature)
		if geom != nil {
			geomClone := C.OGR_G_Clone(geom)
			C.OGR_F_SetGeometry(newFeature, geomClone)
			C.OGR_G_DestroyGeometry(geomClone)
		}

		// 复制原有字段值
		for i := C.int(0); i < fieldCount; i++ {
			if C.OGR_F_IsFieldSet(sourceFeature, i) != 0 {
				sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, i)
				fieldType := C.OGR_Fld_GetType(sourceFieldDefn)

				switch fieldType {
				case C.OFTInteger:
					value := C.OGR_F_GetFieldAsInteger(sourceFeature, i)
					C.OGR_F_SetFieldInteger(newFeature, i, value)
				case C.OFTInteger64:
					value := C.OGR_F_GetFieldAsInteger64(sourceFeature, i)
					C.OGR_F_SetFieldInteger64(newFeature, i, value)
				case C.OFTReal:
					value := C.OGR_F_GetFieldAsDouble(sourceFeature, i)
					C.OGR_F_SetFieldDouble(newFeature, i, value)
				case C.OFTString:
					value := C.OGR_F_GetFieldAsString(sourceFeature, i)
					C.OGR_F_SetFieldString(newFeature, i, value)
				case C.OFTDate, C.OFTTime, C.OFTDateTime:
					var year, month, day, hour, minute, second, tzflag C.int
					C.OGR_F_GetFieldAsDateTime(sourceFeature, i, &year, &month, &day, &hour, &minute, &second, &tzflag)
					C.OGR_F_SetFieldDateTime(newFeature, i, year, month, day, hour, minute, second, tzflag)
				default:
					value := C.OGR_F_GetFieldAsString(sourceFeature, i)
					C.OGR_F_SetFieldString(newFeature, i, value)
				}
			}
		}

		// 设置标识字段值
		C.OGR_F_SetFieldInteger64(newFeature, identifierFieldIndex, C.longlong(featureID))
		featureID++

		// 添加要素到新图层
		C.OGR_L_CreateFeature(newLayer, newFeature)
	})

	// 创建新的GDALLayer包装器
	result := &GDALLayer{
		layer:   newLayer,
		dataset: dataSource,
	}

	fmt.Printf("成功创建带标识字段的图层，共处理 %d 个要素\n", featureID-1)
	return result, nil
}

func createMemoryLayerCopy(sourceLayer *GDALLayer, layerName string) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层无效")
	}

	layerNameC := C.CString(layerName)
	defer C.free(unsafe.Pointer(layerNameC))

	// 使用更安全的方式创建内存图层
	// 创建内存数据源
	driverName := C.CString("MEM")
	driver := C.OGRGetDriverByName(driverName)
	C.free(unsafe.Pointer(driverName))

	if driver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	dataSourceName := C.CString("")
	dataSource := C.OGR_Dr_CreateDataSource(driver, dataSourceName, nil)
	C.free(unsafe.Pointer(dataSourceName))

	if dataSource == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	if sourceLayerDefn == nil {
		C.OGR_DS_Destroy(dataSource)
		return nil, fmt.Errorf("无法获取源图层定义")
	}

	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)
	geomType := C.OGR_FD_GetGeomType(sourceLayerDefn)

	// 创建新图层
	memLayerPtr := C.OGR_DS_CreateLayer(dataSource, layerNameC, sourceSRS, geomType, nil)
	if memLayerPtr == nil {
		C.OGR_DS_Destroy(dataSource)
		return nil, fmt.Errorf("创建内存图层失败")
	}

	// 创建GDALLayer包装器
	memLayer := &GDALLayer{
		layer:   memLayerPtr,
		dataset: dataSource,
	}
	runtime.SetFinalizer(memLayer, (*GDALLayer).cleanup)

	// 复制字段定义

	err := addLayerFields(memLayer, sourceLayer, "")
	if err != nil {
		memLayer.Close()
		return nil, fmt.Errorf("复制字段定义失败: %v", err)
	}

	// 复制所有要素
	sourceLayer.ResetReading()
	var count int64 = 0

	sourceLayer.IterateFeatures(func(sourceFeature C.OGRFeatureH) {
		if sourceFeature == nil {
			return
		}

		// 创建新要素
		memLayerDefn := C.OGR_L_GetLayerDefn(memLayerPtr)
		newFeature := C.OGR_F_Create(memLayerDefn)
		if newFeature == nil {
			return
		}
		defer C.OGR_F_Destroy(newFeature)

		// 复制几何体
		geom := C.OGR_F_GetGeometryRef(sourceFeature)
		if geom != nil {
			geomClone := C.OGR_G_Clone(geom)
			if geomClone != nil {
				C.OGR_F_SetGeometry(newFeature, geomClone)
				C.OGR_G_DestroyGeometry(geomClone)
			}
		}

		// 复制字段值
		fieldCount := C.OGR_F_GetFieldCount(sourceFeature)
		for i := C.int(0); i < fieldCount; i++ {
			if C.OGR_F_IsFieldSet(sourceFeature, i) != 0 {
				sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, i)
				if sourceFieldDefn == nil {
					continue
				}

				fieldType := C.OGR_Fld_GetType(sourceFieldDefn)

				switch fieldType {
				case C.OFTInteger:
					value := C.OGR_F_GetFieldAsInteger(sourceFeature, i)
					C.OGR_F_SetFieldInteger(newFeature, i, value)
				case C.OFTInteger64:
					value := C.OGR_F_GetFieldAsInteger64(sourceFeature, i)
					C.OGR_F_SetFieldInteger64(newFeature, i, value)
				case C.OFTReal:
					value := C.OGR_F_GetFieldAsDouble(sourceFeature, i)
					C.OGR_F_SetFieldDouble(newFeature, i, value)
				case C.OFTString:
					value := C.OGR_F_GetFieldAsString(sourceFeature, i)
					C.OGR_F_SetFieldString(newFeature, i, value)
				case C.OFTDate, C.OFTTime, C.OFTDateTime:
					var year, month, day, hour, minute, second, tzflag C.int
					C.OGR_F_GetFieldAsDateTime(sourceFeature, i, &year, &month, &day, &hour, &minute, &second, &tzflag)
					C.OGR_F_SetFieldDateTime(newFeature, i, year, month, day, hour, minute, second, tzflag)
				default:
					value := C.OGR_F_GetFieldAsString(sourceFeature, i)
					C.OGR_F_SetFieldString(newFeature, i, value)
				}
			}
		}

		// 添加要素到内存图层
		if C.OGR_L_CreateFeature(memLayerPtr, newFeature) == C.OGRERR_NONE {
			count++
		}
	})

	if count <= 0 {
		memLayer.Close()
		return nil, fmt.Errorf("复制要素到内存图层失败，复制了 %d 个要素", count)
	}

	fmt.Printf("成功创建内存图层副本，复制了 %d 个要素\n", count)
	return memLayer, nil
}

// deleteFieldFromLayer 从图层中删除指定字段
func deleteFieldFromLayer(layer *GDALLayer, fieldName string) error {
	layerDefn := C.OGR_L_GetLayerDefn(layer.layer)
	fieldNameC := C.CString(fieldName)
	defer C.free(unsafe.Pointer(fieldNameC))

	fieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, fieldNameC)
	if fieldIndex < 0 {
		fmt.Printf("字段 %s 不存在，跳过删除\n", fieldName)
		return nil
	}

	err := C.OGR_L_DeleteField(layer.layer, fieldIndex)
	if err != C.OGRERR_NONE {
		return fmt.Errorf("删除字段失败，错误代码: %d", int(err))
	} else {
		fmt.Println("删除成功")
	}

	return nil
}

// DeleteFieldFromLayerFuzzy 从图层中模糊匹配删除包含指定字段名的所有字段
func DeleteFieldFromLayerFuzzy(layer *GDALLayer, fieldName string) error {
	layerDefn := C.OGR_L_GetLayerDefn(layer.layer)

	// 收集需要删除的字段名称（而不是索引）
	var fieldsToDelete []string

	// 遍历所有字段，找到包含指定字段名的字段
	fieldCount := C.OGR_FD_GetFieldCount(layerDefn)
	for i := C.int(0); i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, i)
		fieldNamePtr := C.OGR_Fld_GetNameRef(fieldDefn)
		currentFieldName := C.GoString(fieldNamePtr)

		// 检查字段名是否包含目标字段名（不区分大小写）
		if strings.Contains(strings.ToLower(currentFieldName), strings.ToLower(fieldName)) {
			fieldsToDelete = append(fieldsToDelete, currentFieldName)
		}
	}

	if len(fieldsToDelete) == 0 {
		fmt.Printf("未找到包含 '%s' 的字段，跳过删除\n", fieldName)
		return nil
	}

	fmt.Printf("找到 %d 个包含 '%s' 的字段: %v\n", len(fieldsToDelete), fieldName, fieldsToDelete)

	// 逐个删除字段（每次都重新获取索引）
	deletedCount := 0
	for _, fieldToDelete := range fieldsToDelete {
		// 每次删除前都重新获取图层定义和字段索引
		layerDefn = C.OGR_L_GetLayerDefn(layer.layer)
		fieldNameC := C.CString(fieldToDelete)
		fieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, fieldNameC)
		C.free(unsafe.Pointer(fieldNameC))

		if fieldIndex < 0 {
			fmt.Printf("字段 '%s' 已不存在，可能已被删除\n", fieldToDelete)
			continue
		}

		err := C.OGR_L_DeleteField(layer.layer, fieldIndex)
		if err != C.OGRERR_NONE {
			fmt.Printf("删除字段 '%s' 失败，错误代码: %d\n", fieldToDelete, int(err))
			continue
		}

		fmt.Printf("成功删除字段: %s\n", fieldToDelete)
		deletedCount++
	}

	fmt.Printf("总共删除了 %d 个字段\n", deletedCount)
	return nil
}

func performUnionByFields(inputLayer *GDALLayer,
	precisionConfig *GeometryPrecisionConfig, progressCallback ProgressCallback) (*GeosAnalysisResult, error) {
	layerDefn := C.OGR_L_GetLayerDefn(inputLayer.layer)
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))
	// 构建分组字段列表（只需要输入图层的标识字段）
	// 查找方法表的标识字段（可能有前缀）
	var groupFields []string
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))

		if strings.Contains(fieldName, "gogeo_analysis_id") {
			groupFields = append(groupFields, fieldName)
			break
		}
	}

	// 构建输出图层名称
	outputLayerName := fmt.Sprintf("analysis_union_outlayer")
	precisionConfig.GridSize = 0
	// 执行融合操作
	return UnionByFieldsWithPrecision(inputLayer, groupFields, outputLayerName, precisionConfig, progressCallback)
}

// 添加安全的图层验证函数
func validateLayer(layer *GDALLayer) error {
	if layer == nil {
		return fmt.Errorf("图层为空")
	}
	if layer.layer == nil {
		return fmt.Errorf("图层句柄无效")
	}

	// 尝试获取图层定义来验证图层是否有效
	layerDefn := C.OGR_L_GetLayerDefn(layer.layer)
	if layerDefn == nil {
		return fmt.Errorf("图层定义无效")
	}

	return nil
}

func mergeResultsToMainLayer(sourceLayer, targetLayer *GDALLayer) error {
	if sourceLayer == nil || targetLayer == nil {
		return fmt.Errorf("源图层或目标图层为空")
	}

	// 验证图层有效性
	if err := validateLayer(sourceLayer); err != nil {
		return fmt.Errorf("源图层无效: %v", err)
	}
	if err := validateLayer(targetLayer); err != nil {
		return fmt.Errorf("目标图层无效: %v", err)
	}

	// 重置源图层读取位置
	C.OGR_L_ResetReading(sourceLayer.layer)

	// 获取目标图层定义
	targetDefn := C.OGR_L_GetLayerDefn(targetLayer.layer)
	if targetDefn == nil {
		return fmt.Errorf("获取目标图层定义失败")
	}

	// 获取源图层定义用于字段映射
	sourceDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	if sourceDefn == nil {
		return fmt.Errorf("获取源图层定义失败")
	}

	// 创建字段映射（保持原有逻辑）
	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	targetFieldCount := int(C.OGR_FD_GetFieldCount(targetDefn))
	fieldMapping := make(map[int]int)

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		sourceFieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		for j := 0; j < targetFieldCount; j++ {
			targetFieldDefn := C.OGR_FD_GetFieldDefn(targetDefn, C.int(j))
			if targetFieldDefn == nil {
				continue
			}

			targetFieldName := C.GoString(C.OGR_Fld_GetNameRef(targetFieldDefn))
			if sourceFieldName == targetFieldName {
				fieldMapping[i] = j
				break
			}
		}
	}

	// 统计变量
	var copiedCount int
	var errorCount int

	// 逐个复制要素 - 关键修复点
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break // 没有更多要素
		}

		// 确保在所有退出路径都释放 sourceFeature
		func() {
			defer C.OGR_F_Destroy(sourceFeature)

			// 创建新要素
			newFeature := C.OGR_F_Create(targetDefn)
			if newFeature == nil {
				errorCount++
				return
			}
			defer C.OGR_F_Destroy(newFeature)

			// 复制几何体 - 关键修复点
			sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
			if sourceGeom != nil {
				clonedGeom := C.OGR_G_Clone(sourceGeom)
				if clonedGeom != nil {
					// 确保在所有情况下都释放克隆的几何体
					defer C.OGR_G_DestroyGeometry(clonedGeom)

					setGeomErr := C.OGR_F_SetGeometry(newFeature, clonedGeom)
					if setGeomErr != C.OGRERR_NONE {

					}
				}
			}

			// 复制字段值
			for sourceIdx, targetIdx := range fieldMapping {
				if C.OGR_F_IsFieldSet(sourceFeature, C.int(sourceIdx)) != 0 {
					sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(sourceIdx))
					if sourceFieldDefn == nil {
						continue
					}

					fieldType := C.OGR_Fld_GetType(sourceFieldDefn)

					switch fieldType {
					case C.OFTInteger:
						value := C.OGR_F_GetFieldAsInteger(sourceFeature, C.int(sourceIdx))
						C.OGR_F_SetFieldInteger(newFeature, C.int(targetIdx), value)
					case C.OFTInteger64:
						value := C.OGR_F_GetFieldAsInteger64(sourceFeature, C.int(sourceIdx))
						C.OGR_F_SetFieldInteger64(newFeature, C.int(targetIdx), value)
					case C.OFTReal:
						value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(sourceIdx))
						C.OGR_F_SetFieldDouble(newFeature, C.int(targetIdx), value)
					case C.OFTString:
						value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIdx))
						C.OGR_F_SetFieldString(newFeature, C.int(targetIdx), value)
					case C.OFTDate, C.OFTTime, C.OFTDateTime:
						var year, month, day, hour, minute, second, tzflag C.int
						C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(sourceIdx),
							&year, &month, &day, &hour, &minute, &second, &tzflag)
						C.OGR_F_SetFieldDateTime(newFeature, C.int(targetIdx),
							year, month, day, hour, minute, second, tzflag)
					default:
						value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIdx))
						C.OGR_F_SetFieldString(newFeature, C.int(targetIdx), value)
					}
				}
			}

			// 添加要素到目标图层
			createErr := C.OGR_L_CreateFeature(targetLayer.layer, newFeature)
			if createErr == C.OGRERR_NONE {
				copiedCount++
			} else {
				errorCount++

			}
		}()

	}

	return nil
}

type taskResult struct {
	layer    *GDALLayer
	err      error
	duration time.Duration
	index    int
}

// createTileResultLayer 为分块创建结果图层
func createTileResultLayer(inputLayer *GDALLayer, layerName string) (*GDALLayer, error) {
	layerNameC := C.CString(layerName)
	defer C.free(unsafe.Pointer(layerNameC))

	// 获取空间参考系统
	srs := inputLayer.GetSpatialRef()

	// 创建内存图层
	resultLayerPtr := C.createMemoryLayer(layerNameC, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建分块结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 添加字段定义
	err := addLayerFields(resultLayer, inputLayer, "")
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

// cleanupTileFiles 清理临时分块文件
func cleanupTileFiles(taskid string) error {
	// 删除整个任务目录
	taskDir := taskid
	if _, err := os.Stat(taskDir); os.IsNotExist(err) {
		return nil // 目录不存在，无需清理
	}

	return os.RemoveAll(taskDir)
}

func fixGeometryTopology(layer *GDALLayer) error {
	layer.ResetReading()
	layer.IterateFeatures(func(feature C.OGRFeatureH) {
		geom := C.OGR_F_GetGeometryRef(feature)
		if geom != nil {
			// 修复几何体
			fixedGeom := C.OGR_G_MakeValid(geom)
			if fixedGeom != nil {
				C.OGR_F_SetGeometry(feature, fixedGeom)
				defer C.OGR_G_DestroyGeometry(fixedGeom)
				C.OGR_L_SetFeature(layer.layer, feature)
			}
		}
	})
	return nil
}

// getLayersExtent 获取两个图层的合并范围
func getLayersExtent(layer1, layer2 *GDALLayer) (*Extent, error) {
	// 定义 OGREnvelope 结构体
	type OGREnvelope struct {
		MinX C.double
		MaxX C.double
		MinY C.double
		MaxY C.double
	}

	var extent1, extent2 OGREnvelope

	// 获取第一个图层的范围
	err := C.OGR_L_GetExtent(layer1.layer, (*C.OGREnvelope)(unsafe.Pointer(&extent1)), 1)
	if err != C.OGRERR_NONE {
		return nil, fmt.Errorf("获取图层1范围失败，错误代码: %d", int(err))
	}

	// 获取第二个图层的范围
	err = C.OGR_L_GetExtent(layer2.layer, (*C.OGREnvelope)(unsafe.Pointer(&extent2)), 1)
	if err != C.OGRERR_NONE {
		return nil, fmt.Errorf("获取图层2范围失败，错误代码: %d", int(err))
	}

	// 计算合并范围
	return &Extent{
		MinX: math.Min(float64(extent1.MinX), float64(extent2.MinX)),
		MaxX: math.Max(float64(extent1.MaxX), float64(extent2.MaxX)),
		MinY: math.Min(float64(extent1.MinY), float64(extent2.MinY)),
		MaxY: math.Max(float64(extent1.MaxY), float64(extent2.MaxY)),
	}, nil
}
