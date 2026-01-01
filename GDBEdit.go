package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// DeleteFeatureByObjectID 删除GDB图层中指定ObjectID的要素
// gdbPath: GDB文件路径
// layerName: 图层名称
// objectID: 要删除的ObjectID
func DeleteFeatureByObjectID(gdbPath string, layerName string, objectID int64) error {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取FileGDB驱动（需要支持写入）
	driver := C.OGRGetDriverByName(C.CString("FileGDB"))
	if driver == nil {
		// 尝试OpenFileGDB，但注意OpenFileGDB通常是只读的
		driver = C.OGRGetDriverByName(C.CString("OpenFileGDB"))
		if driver == nil {
			return fmt.Errorf("无法获取GDB驱动")
		}
	}

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil) // 1表示可写
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	if layer == nil {
		return fmt.Errorf("无法找到图层: %s", layerName)
	}

	// 检查图层是否支持删除操作
	if C.OGR_L_TestCapability(layer, C.CString("DeleteFeature")) == 0 {
		return fmt.Errorf("图层不支持删除要素操作")
	}

	// 方法1: 直接通过FID删除（如果ObjectID就是FID）
	result := C.OGR_L_DeleteFeature(layer, C.GIntBig(objectID))
	if result != C.OGRERR_NONE {
		// 如果直接删除失败，尝试方法2：通过查询找到要素再删除
		return deleteFeatureByQuery(layer, objectID)
	}

	// 同步更改到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("成功删除图层 '%s' 中 ObjectID=%d 的要素\n", layerName, objectID)
	return nil
}

// deleteFeatureByQuery 通过查询方式删除要素
func deleteFeatureByQuery(layer C.OGRLayerH, objectID int64) error {
	// 设置属性过滤器查找ObjectID
	filterStr := fmt.Sprintf("OBJECTID = %d", objectID)
	cFilterStr := C.CString(filterStr)
	defer C.free(unsafe.Pointer(cFilterStr))

	result := C.OGR_L_SetAttributeFilter(layer, cFilterStr)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("设置属性过滤器失败")
	}

	// 重置读取位置
	C.OGR_L_ResetReading(layer)

	// 获取匹配的要素
	feature := C.OGR_L_GetNextFeature(layer)
	if feature == nil {
		// 清除过滤器
		C.OGR_L_SetAttributeFilter(layer, nil)
		return fmt.Errorf("未找到 ObjectID=%d 的要素", objectID)
	}

	// 获取要素的FID
	fid := C.OGR_F_GetFID(feature)
	C.OGR_F_Destroy(feature)

	// 清除过滤器
	C.OGR_L_SetAttributeFilter(layer, nil)

	// 删除要素
	deleteResult := C.OGR_L_DeleteFeature(layer, fid)
	if deleteResult != C.OGRERR_NONE {
		return fmt.Errorf("删除要素失败，错误代码: %d", int(deleteResult))
	}

	// 同步更改到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	return nil
}

// DeleteFeaturesByFilter 根据SQL过滤条件删除多个要素
// gdbPath: GDB文件路径
// layerName: 图层名称
// whereClause: SQL WHERE条件，如 "OBJECTID = 0" 或 "OBJECTID IN (0, 1, 2)"
func DeleteFeaturesByFilter(gdbPath string, layerName string, whereClause string) (int, error) {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return 0, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	if layer == nil {
		return 0, fmt.Errorf("无法找到图层: %s", layerName)
	}

	// 检查图层是否支持删除操作
	if C.OGR_L_TestCapability(layer, C.CString("DeleteFeature")) == 0 {
		return 0, fmt.Errorf("图层不支持删除要素操作")
	}

	// 设置属性过滤器
	cWhereClause := C.CString(whereClause)
	defer C.free(unsafe.Pointer(cWhereClause))

	result := C.OGR_L_SetAttributeFilter(layer, cWhereClause)
	if result != C.OGRERR_NONE {
		return 0, fmt.Errorf("设置属性过滤器失败: %s", whereClause)
	}

	// 收集所有要删除的FID
	var fidsToDelete []C.GIntBig
	C.OGR_L_ResetReading(layer)

	for {
		feature := C.OGR_L_GetNextFeature(layer)
		if feature == nil {
			break
		}
		fid := C.OGR_F_GetFID(feature)
		fidsToDelete = append(fidsToDelete, fid)
		C.OGR_F_Destroy(feature)
	}

	// 清除过滤器
	C.OGR_L_SetAttributeFilter(layer, nil)

	if len(fidsToDelete) == 0 {
		return 0, fmt.Errorf("未找到符合条件的要素: %s", whereClause)
	}

	// 删除收集到的要素
	deletedCount := 0
	for _, fid := range fidsToDelete {
		deleteResult := C.OGR_L_DeleteFeature(layer, fid)
		if deleteResult == C.OGRERR_NONE {
			deletedCount++
		} else {
			fmt.Printf("警告: 删除FID=%d失败\n", int64(fid))
		}
	}

	// 同步更改到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return deletedCount, fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("成功删除 %d 个要素\n", deletedCount)
	return deletedCount, nil
}

// InsertLayerToGDB 将GDALLayer插入到GDB文件的对应图层中，并进行坐标转换
// sourceLayer: 源图层（4326坐标系）
// gdbPath: 目标GDB文件路径
// targetLayerName: 目标图层名称
// options: 插入选项（可选）
func InsertLayerToGDB(sourceLayer *GDALLayer, gdbPath string, targetLayerName string, options *InsertOptions) error {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return fmt.Errorf("源图层为空")
	}

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开GDB数据源
	targetDataset := C.OGROpen(cFilePath, C.int(1), nil)
	if targetDataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 获取目标图层
	cTargetLayerName := C.CString(targetLayerName)
	defer C.free(unsafe.Pointer(cTargetLayerName))

	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cTargetLayerName)
	if targetLayer == nil {
		return fmt.Errorf("无法找到目标图层: %s", targetLayerName)
	}

	// 获取源图层的空间参考（假设为4326）
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)
	if sourceSRS == nil {
		// 如果源图层没有空间参考，创建4326空间参考
		sourceSRS = C.OSRNewSpatialReference(nil)
		defer C.OSRDestroySpatialReference(sourceSRS)
		C.OSRImportFromEPSG(sourceSRS, 4326)
	}

	// 获取目标图层的空间参考
	targetSRS := C.OGR_L_GetSpatialRef(targetLayer)
	if targetSRS == nil {
		return fmt.Errorf("目标图层没有空间参考系统")
	}

	// 创建坐标转换对象
	var transform C.OGRCoordinateTransformationH
	needTransform := C.OSRIsSame(sourceSRS, targetSRS) == 0

	if needTransform {
		transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
		if transform == nil {
			return fmt.Errorf("无法创建坐标转换对象")
		}
		defer C.OCTDestroyCoordinateTransformation(transform)
	}

	// 获取图层定义
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)

	// 创建字段映射
	fieldMapping, err := createFieldMapping(sourceLayerDefn, targetLayerDefn)
	if err != nil {
		return fmt.Errorf("创建字段映射失败: %v", err)
	}

	// 开始事务（如果支持）
	useTransaction := C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		result := C.OGR_L_StartTransaction(targetLayer)
		if result != C.OGRERR_NONE {
			useTransaction = false
		}
	}

	// 遍历源图层的所有要素
	C.OGR_L_ResetReading(sourceLayer.layer)

	insertedCount := 0
	failedCount := 0

	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		// 创建新要素
		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			failedCount++
			continue
		}

		// 复制并转换几何
		sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
		if sourceGeom != nil {
			// 克隆几何对象
			clonedGeom := C.OGR_G_Clone(sourceGeom)
			if clonedGeom != nil {
				// 执行坐标转换
				if needTransform && transform != nil {
					transformResult := C.OGR_G_Transform(clonedGeom, transform)
					if transformResult != C.OGRERR_NONE {
						fmt.Printf("警告: 几何转换失败，跳过该要素\n")
						C.OGR_G_DestroyGeometry(clonedGeom)
						C.OGR_F_Destroy(targetFeature)
						C.OGR_F_Destroy(sourceFeature)
						failedCount++
						continue
					}
				}

				// 设置几何到目标要素
				C.OGR_F_SetGeometry(targetFeature, clonedGeom)
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		}

		// 复制属性字段
		err := copyFeatureFields(sourceFeature, targetFeature, fieldMapping)
		if err != nil && options != nil && options.StrictMode {
			fmt.Printf("警告: 复制字段失败: %v\n", err)
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			failedCount++
			continue
		}

		// 插入要素到目标图层
		result := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if result == C.OGRERR_NONE {
			insertedCount++
		} else {
			failedCount++
			fmt.Printf("警告: 插入要素失败，错误代码: %d\n", int(result))
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		// 定期同步（可选，提高性能）
		if options != nil && options.SyncInterval > 0 && insertedCount%options.SyncInterval == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
		}
	}

	// 提交事务
	if useTransaction {
		commitResult := C.OGR_L_CommitTransaction(targetLayer)
		if commitResult != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return fmt.Errorf("提交事务失败")
		}
	}

	// 最终同步到磁盘
	C.OGR_L_SyncToDisk(targetLayer)

	fmt.Printf("插入完成: 成功 %d 个，失败 %d 个\n", insertedCount, failedCount)

	if failedCount > 0 && options != nil && options.StrictMode {
		return fmt.Errorf("部分要素插入失败: %d/%d", failedCount, insertedCount+failedCount)
	}

	return nil
}

// FieldMapping 字段映射结构
type FieldMapping struct {
	SourceIndex int
	TargetIndex int
	FieldName   string
}

// createFieldMapping 创建源图层和目标图层之间的字段映射
func createFieldMapping(sourceLayerDefn, targetLayerDefn C.OGRFeatureDefnH) ([]FieldMapping, error) {
	var mappings []FieldMapping

	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceLayerDefn))

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 在目标图层中查找同名字段
		cFieldName := C.CString(fieldName)
		targetIndex := C.OGR_FD_GetFieldIndex(targetLayerDefn, cFieldName)
		C.free(unsafe.Pointer(cFieldName))

		if targetIndex >= 0 {
			mappings = append(mappings, FieldMapping{
				SourceIndex: i,
				TargetIndex: int(targetIndex),
				FieldName:   fieldName,
			})
		}
	}

	return mappings, nil
}

// copyFeatureFields 根据字段映射复制要素字段
func copyFeatureFields(sourceFeature, targetFeature C.OGRFeatureH, mappings []FieldMapping) error {
	for _, mapping := range mappings {
		// 检查源字段是否已设置
		if C.OGR_F_IsFieldSet(sourceFeature, C.int(mapping.SourceIndex)) == 0 {
			// 字段未设置，设置为NULL
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 检查是否为NULL
		if C.OGR_F_IsFieldNull(sourceFeature, C.int(mapping.SourceIndex)) != 0 {
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 获取字段类型
		sourceFieldDefn := C.OGR_F_GetFieldDefnRef(sourceFeature, C.int(mapping.SourceIndex))
		targetFieldDefn := C.OGR_F_GetFieldDefnRef(targetFeature, C.int(mapping.TargetIndex))

		if sourceFieldDefn == nil || targetFieldDefn == nil {
			continue
		}

		sourceFieldType := C.OGR_Fld_GetType(sourceFieldDefn)
		targetFieldType := C.OGR_Fld_GetType(targetFieldDefn)

		// 根据字段类型复制值
		err := copyFieldValue(sourceFeature, targetFeature,
			C.int(mapping.SourceIndex), C.int(mapping.TargetIndex),
			sourceFieldType, targetFieldType)

		if err != nil {
			fmt.Printf("警告: 复制字段 '%s' 失败: %v\n", mapping.FieldName, err)
		}
	}

	return nil
}

// copyFieldValue 复制单个字段值（支持类型转换）
func copyFieldValue(sourceFeature, targetFeature C.OGRFeatureH,
	sourceIndex, targetIndex C.int,
	sourceType, targetType C.OGRFieldType) error {

	// 如果类型相同，直接复制
	if sourceType == targetType {
		switch sourceType {
		case C.OFTInteger:
			val := C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldInteger(targetFeature, targetIndex, val)
		case C.OFTInteger64:
			val := C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldInteger64(targetFeature, targetIndex, val)
		case C.OFTReal:
			val := C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldDouble(targetFeature, targetIndex, val)
		case C.OFTString:
			val := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldString(targetFeature, targetIndex, val)
		case C.OFTDate, C.OFTTime, C.OFTDateTime:
			var year, month, day, hour, minute, second C.int
			var tzflag C.int
			C.OGR_F_GetFieldAsDateTime(sourceFeature, sourceIndex,
				&year, &month, &day, &hour, &minute, &second, &tzflag)
			C.OGR_F_SetFieldDateTime(targetFeature, targetIndex,
				year, month, day, hour, minute, second, tzflag)
		default:
			// 其他类型作为字符串处理
			val := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldString(targetFeature, targetIndex, val)
		}
		return nil
	}

	// 类型不同，需要转换
	switch targetType {
	case C.OFTInteger:
		val := C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldInteger(targetFeature, targetIndex, val)
	case C.OFTInteger64:
		val := C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldInteger64(targetFeature, targetIndex, val)
	case C.OFTReal:
		val := C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldDouble(targetFeature, targetIndex, val)
	case C.OFTString:
		val := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, val)
	default:
		// 默认转为字符串
		val := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, val)
	}

	return nil
}

// InsertOptions 插入选项
type InsertOptions struct {
	StrictMode          bool // 严格模式，遇到错误立即停止
	SyncInterval        int  // 同步间隔（每插入多少条要素同步一次）
	SkipInvalidGeometry bool // 跳过无效几何
	CreateMissingFields bool // 创建缺失的字段（如果目标图层支持）
}

//字段修改相关

// FieldDefinition 字段定义结构
type FieldDefinition struct {
	Name      string      // 字段名称
	Type      FieldType   // 字段类型
	Width     int         // 字段宽度
	Precision int         // 精度（用于数值类型）
	Nullable  bool        // 是否允许NULL
	Default   interface{} // 默认值
}

// FieldType 字段类型枚举
type FieldType int

const (
	FieldTypeInteger   FieldType = C.OFTInteger
	FieldTypeInteger64 FieldType = C.OFTInteger64
	FieldTypeReal      FieldType = C.OFTReal
	FieldTypeString    FieldType = C.OFTString
	FieldTypeDate      FieldType = C.OFTDate
	FieldTypeTime      FieldType = C.OFTTime
	FieldTypeDateTime  FieldType = C.OFTDateTime
	FieldTypeBinary    FieldType = C.OFTBinary
)

// AddField 向GDB图层添加字段
// gdbPath: GDB文件路径
// layerName: 图层名称
// fieldDef: 字段定义
func AddField(gdbPath string, layerName string, fieldDef FieldDefinition) error {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	if layer == nil {
		return fmt.Errorf("无法找到图层: %s", layerName)
	}

	// 检查图层是否支持添加字段
	if C.OGR_L_TestCapability(layer, C.CString("CreateField")) == 0 {
		return fmt.Errorf("图层不支持添加字段操作")
	}

	// 检查字段是否已存在
	layerDefn := C.OGR_L_GetLayerDefn(layer)
	cFieldName := C.CString(fieldDef.Name)
	defer C.free(unsafe.Pointer(cFieldName))

	if C.OGR_FD_GetFieldIndex(layerDefn, cFieldName) >= 0 {
		return fmt.Errorf("字段 '%s' 已存在", fieldDef.Name)
	}

	// 创建字段定义
	fieldDefn := C.OGR_Fld_Create(cFieldName, C.OGRFieldType(fieldDef.Type))
	if fieldDefn == nil {
		return fmt.Errorf("创建字段定义失败")
	}
	defer C.OGR_Fld_Destroy(fieldDefn)

	// 设置字段属性
	if fieldDef.Width > 0 {
		C.OGR_Fld_SetWidth(fieldDefn, C.int(fieldDef.Width))
	}

	if fieldDef.Precision > 0 {
		C.OGR_Fld_SetPrecision(fieldDefn, C.int(fieldDef.Precision))
	}

	// 设置是否可为NULL
	if fieldDef.Nullable {
		C.OGR_Fld_SetNullable(fieldDefn, C.int(1))
	} else {
		C.OGR_Fld_SetNullable(fieldDefn, C.int(0))
	}

	// 设置默认值（如果提供）
	if fieldDef.Default != nil {
		err := setFieldDefault(fieldDefn, fieldDef.Default, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("设置默认值失败: %v", err)
		}
	}

	// 添加字段到图层
	result := C.OGR_L_CreateField(layer, fieldDefn, C.int(1)) // 1表示允许近似
	if result != C.OGRERR_NONE {
		return fmt.Errorf("添加字段失败，错误代码: %d", int(result))
	}

	// 同步到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("成功添加字段 '%s' 到图层 '%s'\n", fieldDef.Name, layerName)
	return nil
}

// DeleteField 从GDB图层删除字段
// gdbPath: GDB文件路径
// layerName: 图层名称
// fieldName: 要删除的字段名称
func DeleteField(gdbPath string, layerName string, fieldName string) error {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	if layer == nil {
		return fmt.Errorf("无法找到图层: %s", layerName)
	}

	// 检查图层是否支持删除字段
	if C.OGR_L_TestCapability(layer, C.CString("DeleteField")) == 0 {
		return fmt.Errorf("图层不支持删除字段操作")
	}

	// 获取字段索引
	layerDefn := C.OGR_L_GetLayerDefn(layer)
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	fieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, cFieldName)
	if fieldIndex < 0 {
		return fmt.Errorf("字段 '%s' 不存在", fieldName)
	}

	// 删除字段
	result := C.OGR_L_DeleteField(layer, fieldIndex)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("删除字段失败，错误代码: %d", int(result))
	}

	// 同步到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("成功从图层 '%s' 删除字段 '%s'\n", layerName, fieldName)
	return nil
}

// setFieldDefault 设置字段默认值的辅助函数
func setFieldDefault(fieldDefn C.OGRFieldDefnH, defaultValue interface{}, fieldType FieldType) error {
	switch fieldType {
	case FieldTypeInteger, FieldTypeInteger64:
		var intVal int64
		switch v := defaultValue.(type) {
		case int:
			intVal = int64(v)
		case int32:
			intVal = int64(v)
		case int64:
			intVal = v
		default:
			return fmt.Errorf("无效的整数默认值类型: %T", defaultValue)
		}
		defaultStr := fmt.Sprintf("%d", intVal)
		cDefaultStr := C.CString(defaultStr)
		defer C.free(unsafe.Pointer(cDefaultStr))
		C.OGR_Fld_SetDefault(fieldDefn, cDefaultStr)

	case FieldTypeReal:
		var floatVal float64
		switch v := defaultValue.(type) {
		case float32:
			floatVal = float64(v)
		case float64:
			floatVal = v
		default:
			return fmt.Errorf("无效的浮点数默认值类型: %T", defaultValue)
		}
		defaultStr := fmt.Sprintf("%f", floatVal)
		cDefaultStr := C.CString(defaultStr)
		defer C.free(unsafe.Pointer(cDefaultStr))
		C.OGR_Fld_SetDefault(fieldDefn, cDefaultStr)

	case FieldTypeString:
		strVal, ok := defaultValue.(string)
		if !ok {
			return fmt.Errorf("无效的字符串默认值类型: %T", defaultValue)
		}
		// 字符串默认值需要用单引号包围
		defaultStr := fmt.Sprintf("'%s'", strVal)
		cDefaultStr := C.CString(defaultStr)
		defer C.free(unsafe.Pointer(cDefaultStr))
		C.OGR_Fld_SetDefault(fieldDefn, cDefaultStr)

	default:
		return fmt.Errorf("不支持的字段类型设置默认值: %d", fieldType)
	}

	return nil
}

//字段同步

// SyncFieldOptions 字段同步选项
type SyncFieldOptions struct {
	SourceField      string            // PostGIS源字段名
	TargetField      string            // GDB目标字段名
	SourceIDField    string            // PostGIS的ID字段名（默认为"objectid"）
	TargetIDField    string            // GDB的ID字段名（"FID"或"OBJECTID"）
	UseFID           bool              // 是否使用FID作为关联字段（默认true）
	BatchSize        int               // 批处理大小（默认1000）
	UseTransaction   bool              // 是否使用事务（默认true）
	UpdateNullValues bool              // 是否更新NULL值（默认false）
	FieldMapping     map[string]string // 多字段映射（源字段->目标字段）
	WhereClause      string            // SQL过滤条件（可选）
}

// SyncResult 同步结果
type SyncResult struct {
	TotalCount   int      // 总记录数
	UpdatedCount int      // 成功更新数
	FailedCount  int      // 失败数
	SkippedCount int      // 跳过数
	Errors       []string // 错误信息列表
}

// SyncFieldFromPostGIS 从PostGIS同步字段值到GDB
// postGISConfig: PostGIS配置
// gdbPath: GDB文件路径
// gdbLayerName: GDB图层名称
// options: 同步选项
func SyncFieldFromPostGIS(postGISConfig *PostGISConfig, gdbPath string, gdbLayerName string, options *SyncFieldOptions) (*SyncResult, error) {

	// 设置默认选项
	if options == nil {
		options = &SyncFieldOptions{}
	}
	if options.SourceIDField == "" {
		options.SourceIDField = "objectid"
	}
	if options.TargetIDField == "" {
		options.TargetIDField = "FID"
	}
	// 默认使用FID
	options.UseFID = (options.TargetIDField == "FID")

	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}

	result := &SyncResult{
		Errors: make([]string, 0),
	}

	// 1. 从PostGIS读取数据
	reader := NewPostGISReader(postGISConfig)
	sourceLayer, err := reader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取PostGIS表失败: %v", err)
	}
	defer sourceLayer.Close()

	// 2. 打开GDB数据源（可写模式）
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 3. 获取GDB目标图层
	cGDBLayerName := C.CString(gdbLayerName)
	defer C.free(unsafe.Pointer(cGDBLayerName))

	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cGDBLayerName)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法找到GDB图层: %s", gdbLayerName)
	}

	// 4. 验证字段存在性
	err = validateFieldsWithFID(sourceLayer.layer, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("字段验证失败: %v", err)
	}

	// 5. 构建ID到值的映射
	valueMap, err := buildValueMapFromPostGIS(sourceLayer.layer, options)
	if err != nil {
		return nil, fmt.Errorf("构建值映射失败: %v", err)
	}

	result.TotalCount = len(valueMap)
	if result.TotalCount == 0 {
		return result, fmt.Errorf("PostGIS表中没有数据")
	}

	fmt.Printf("从PostGIS读取了 %d 条记录\n", result.TotalCount)

	// 6. 开始事务（如果支持）
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
			fmt.Println("警告: 无法启动事务，将使用非事务模式")
		}
	}

	// 7. 批量更新GDB图层
	err = updateGDBLayerByFID(targetLayer, valueMap, options, result)
	if err != nil {
		if useTransaction {
			C.OGR_L_RollbackTransaction(targetLayer)
		}
		return result, fmt.Errorf("更新GDB图层失败: %v", err)
	}

	// 8. 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}

	// 9. 同步到磁盘
	if C.OGR_L_SyncToDisk(targetLayer) != C.OGRERR_NONE {
		return result, fmt.Errorf("同步到磁盘失败")
	}

	fmt.Printf("同步完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.UpdatedCount, result.FailedCount, result.SkippedCount)

	if len(result.Errors) > 0 {
		fmt.Printf("发生 %d 个错误\n", len(result.Errors))
	}

	return result, nil
}

// FieldValue 字段值结构
type FieldValue struct {
	Values map[string]interface{} // 字段名 -> 值
}

// buildValueMapFromPostGIS 从PostGIS图层构建ID到字段值的映射
func buildValueMapFromPostGIS(sourceLayer C.OGRLayerH, options *SyncFieldOptions) (map[int64]*FieldValue, error) {
	valueMap := make(map[int64]*FieldValue)

	// 获取图层定义
	layerDefn := C.OGR_L_GetLayerDefn(sourceLayer)

	// 获取PostGIS的ID字段索引（通常是objectid）
	cSourceIDField := C.CString(options.SourceIDField)
	defer C.free(unsafe.Pointer(cSourceIDField))

	sourceIDIndex := C.OGR_FD_GetFieldIndex(layerDefn, cSourceIDField)
	if sourceIDIndex < 0 {
		return nil, fmt.Errorf("PostGIS表中未找到ID字段: %s", options.SourceIDField)
	}

	// 确定要读取的字段
	fieldsToRead := make(map[string]int) // 字段名 -> 索引

	if len(options.FieldMapping) > 0 {
		// 使用字段映射
		for sourceField := range options.FieldMapping {
			cSourceField := C.CString(sourceField)
			fieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, cSourceField)
			C.free(unsafe.Pointer(cSourceField))

			if fieldIndex < 0 {
				return nil, fmt.Errorf("PostGIS表中未找到字段: %s", sourceField)
			}
			fieldsToRead[sourceField] = int(fieldIndex)
		}
	} else {
		// 使用单字段
		if options.SourceField == "" {
			return nil, fmt.Errorf("必须指定SourceField或FieldMapping")
		}
		cSourceField := C.CString(options.SourceField)
		fieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, cSourceField)
		C.free(unsafe.Pointer(cSourceField))

		if fieldIndex < 0 {
			return nil, fmt.Errorf("PostGIS表中未找到字段: %s", options.SourceField)
		}
		fieldsToRead[options.SourceField] = int(fieldIndex)
	}

	// 应用过滤条件（如果有）
	if options.WhereClause != "" {
		cWhereClause := C.CString(options.WhereClause)
		defer C.free(unsafe.Pointer(cWhereClause))
		C.OGR_L_SetAttributeFilter(sourceLayer, cWhereClause)
	}

	// 重置读取位置
	C.OGR_L_ResetReading(sourceLayer)

	// 遍历所有要素
	count := 0
	for {
		feature := C.OGR_L_GetNextFeature(sourceLayer)
		if feature == nil {
			break
		}

		// 获取PostGIS的ID（objectid）
		sourceID := int64(C.OGR_F_GetFieldAsInteger64(feature, sourceIDIndex))

		// 读取字段值
		fieldValue := &FieldValue{
			Values: make(map[string]interface{}),
		}

		for fieldName, fieldIndex := range fieldsToRead {
			// 检查字段是否为NULL
			if C.OGR_F_IsFieldNull(feature, C.int(fieldIndex)) != 0 {
				if options.UpdateNullValues {
					fieldValue.Values[fieldName] = nil
				}
				continue
			}

			// 检查字段是否已设置
			if C.OGR_F_IsFieldSet(feature, C.int(fieldIndex)) == 0 {
				continue
			}

			// 获取字段类型和值
			fieldDefn := C.OGR_F_GetFieldDefnRef(feature, C.int(fieldIndex))
			fieldType := C.OGR_Fld_GetType(fieldDefn)

			value := getFieldValue(feature, C.int(fieldIndex), fieldType)

			fieldValue.Values[fieldName] = value
		}

		if len(fieldValue.Values) > 0 {
			valueMap[sourceID] = fieldValue
		}

		C.OGR_F_Destroy(feature)
		count++

		if count%1000 == 0 {
			fmt.Printf("已读取 %d 条PostGIS记录...\n", count)
		}
	}

	// 清除过滤器
	if options.WhereClause != "" {
		C.OGR_L_SetAttributeFilter(sourceLayer, nil)
	}

	return valueMap, nil
}

// updateGDBLayerByFID 通过FID更新GDB图层
func updateGDBLayerByFID(targetLayer C.OGRLayerH, valueMap map[int64]*FieldValue, options *SyncFieldOptions, result *SyncResult) error {
	// 获取图层定义
	layerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	// 构建字段映射（源字段名 -> 目标字段索引）
	fieldMapping := make(map[string]int)

	if len(options.FieldMapping) > 0 {
		for sourceField, targetField := range options.FieldMapping {
			cTargetField := C.CString(targetField)
			targetIndex := C.OGR_FD_GetFieldIndex(layerDefn, cTargetField)
			C.free(unsafe.Pointer(cTargetField))

			if targetIndex < 0 {
				return fmt.Errorf("GDB图层中未找到字段: %s", targetField)
			}
			fieldMapping[sourceField] = int(targetIndex)
		}
	} else {
		targetField := options.TargetField
		if targetField == "" {
			targetField = options.SourceField
		}
		cTargetField := C.CString(targetField)
		targetIndex := C.OGR_FD_GetFieldIndex(layerDefn, cTargetField)
		C.free(unsafe.Pointer(cTargetField))

		if targetIndex < 0 {
			return fmt.Errorf("GDB图层中未找到字段: %s", targetField)
		}
		fieldMapping[options.SourceField] = int(targetIndex)
	}

	// 如果使用FID，直接通过FID访问要素
	if options.UseFID {
		return updateByFID(targetLayer, valueMap, fieldMapping, layerDefn, options, result)
	} else {
		// 使用OBJECTID字段
		return updateByObjectID(targetLayer, valueMap, fieldMapping, layerDefn, options, result)
	}
}

// updateByFID 通过FID直接更新
func updateByFID(targetLayer C.OGRLayerH, valueMap map[int64]*FieldValue, fieldMapping map[string]int, layerDefn C.OGRFeatureDefnH, options *SyncFieldOptions, result *SyncResult) error {
	batchCount := 0

	// 直接通过FID访问要素
	for fid, fieldValue := range valueMap {
		// 通过FID获取要素
		feature := C.OGR_L_GetFeature(targetLayer, C.GIntBig(fid))
		if feature == nil {
			result.SkippedCount++
			errMsg := fmt.Sprintf("FID=%d 在GDB中不存在", fid)
			result.Errors = append(result.Errors, errMsg)
			continue
		}

		// 更新字段值
		updated := false
		for sourceField, targetIndex := range fieldMapping {
			value, hasValue := fieldValue.Values[sourceField]
			if !hasValue {
				continue
			}

			// 设置字段值
			err := setGDBFieldValue(feature, C.int(targetIndex), value, layerDefn)
			if err != nil {
				errMsg := fmt.Sprintf("FID=%d, 字段=%s, 设置值失败: %v", fid, sourceField, err)
				result.Errors = append(result.Errors, errMsg)
				continue
			}
			updated = true
		}

		if updated {
			// 更新要素
			if C.OGR_L_SetFeature(targetLayer, feature) == C.OGRERR_NONE {
				result.UpdatedCount++
			} else {
				result.FailedCount++
				errMsg := fmt.Sprintf("FID=%d, 更新要素失败", fid)
				result.Errors = append(result.Errors, errMsg)
			}
		}

		C.OGR_F_Destroy(feature)

		// 批量同步
		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录...\n", batchCount)
		}
	}

	return nil
}

// updateByObjectID 通过OBJECTID字段更新
func updateByObjectID(targetLayer C.OGRLayerH, valueMap map[int64]*FieldValue, fieldMapping map[string]int, layerDefn C.OGRFeatureDefnH, options *SyncFieldOptions, result *SyncResult) error {
	// 获取OBJECTID字段索引
	cObjectIDField := C.CString(options.TargetIDField)
	defer C.free(unsafe.Pointer(cObjectIDField))

	objectIDIndex := C.OGR_FD_GetFieldIndex(layerDefn, cObjectIDField)
	if objectIDIndex < 0 {
		return fmt.Errorf("GDB图层中未找到字段: %s", options.TargetIDField)
	}

	// 重置读取位置
	C.OGR_L_ResetReading(targetLayer)

	// 遍历GDB图层的所有要素
	batchCount := 0
	for {
		feature := C.OGR_L_GetNextFeature(targetLayer)
		if feature == nil {
			break
		}

		// 获取OBJECTID
		objectID := int64(C.OGR_F_GetFieldAsInteger64(feature, C.int(objectIDIndex)))

		// 查找对应的值
		fieldValue, exists := valueMap[objectID]
		if !exists {
			result.SkippedCount++
			C.OGR_F_Destroy(feature)
			continue
		}

		// 更新字段值
		updated := false
		for sourceField, targetIndex := range fieldMapping {
			value, hasValue := fieldValue.Values[sourceField]
			if !hasValue {
				continue
			}

			// 设置字段值
			err := setGDBFieldValue(feature, C.int(targetIndex), value, layerDefn)
			if err != nil {
				errMsg := fmt.Sprintf("OBJECTID=%d, 字段=%s, 设置值失败: %v", objectID, sourceField, err)
				result.Errors = append(result.Errors, errMsg)
				continue
			}
			updated = true
		}

		if updated {
			// 更新要素
			if C.OGR_L_SetFeature(targetLayer, feature) == C.OGRERR_NONE {
				result.UpdatedCount++
			} else {
				result.FailedCount++
				errMsg := fmt.Sprintf("OBJECTID=%d, 更新要素失败", objectID)
				result.Errors = append(result.Errors, errMsg)
			}
		}

		C.OGR_F_Destroy(feature)

		// 批量同步
		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录...\n", batchCount)
		}
	}

	return nil
}

// setGDBFieldValue 设置GDB字段值
func setGDBFieldValue(feature C.OGRFeatureH, fieldIndex C.int, value interface{}, layerDefn C.OGRFeatureDefnH) error {
	if value == nil {
		C.OGR_F_SetFieldNull(feature, fieldIndex)
		return nil
	}

	// 获取目标字段类型
	fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, fieldIndex)
	fieldType := C.OGR_Fld_GetType(fieldDefn)

	switch fieldType {
	case C.OFTInteger:
		var intVal int32
		switch v := value.(type) {
		case int:
			intVal = int32(v)
		case int32:
			intVal = v
		case int64:
			intVal = int32(v)
		case float64:
			intVal = int32(v)
		case string:
			fmt.Sscanf(v, "%d", &intVal)
		default:
			return fmt.Errorf("无法转换为整数: %T", value)
		}
		C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(intVal))

	case C.OFTInteger64:
		var intVal int64
		switch v := value.(type) {
		case int:
			intVal = int64(v)
		case int32:
			intVal = int64(v)
		case int64:
			intVal = v
		case float64:
			intVal = int64(v)
		case string:
			fmt.Sscanf(v, "%d", &intVal)
		default:
			return fmt.Errorf("无法转换为长整数: %T", value)
		}
		C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(intVal))

	case C.OFTReal:
		var floatVal float64
		switch v := value.(type) {
		case int:
			floatVal = float64(v)
		case int32:
			floatVal = float64(v)
		case int64:
			floatVal = float64(v)
		case float32:
			floatVal = float64(v)
		case float64:
			floatVal = v
		case string:
			fmt.Sscanf(v, "%f", &floatVal)
		default:
			return fmt.Errorf("无法转换为浮点数: %T", value)
		}
		C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(floatVal))

	case C.OFTString:
		strVal := fmt.Sprintf("%v", value)
		cStrVal := C.CString(strVal)
		defer C.free(unsafe.Pointer(cStrVal))
		C.OGR_F_SetFieldString(feature, fieldIndex, cStrVal)

	default:
		// 默认转为字符串
		strVal := fmt.Sprintf("%v", value)
		cStrVal := C.CString(strVal)
		defer C.free(unsafe.Pointer(cStrVal))
		C.OGR_F_SetFieldString(feature, fieldIndex, cStrVal)
	}

	return nil
}

// validateFieldsWithFID 验证字段存在性（支持FID）
func validateFieldsWithFID(sourceLayer, targetLayer C.OGRLayerH, options *SyncFieldOptions) error {
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	// 验证PostGIS的ID字段
	cSourceIDField := C.CString(options.SourceIDField)
	defer C.free(unsafe.Pointer(cSourceIDField))

	if C.OGR_FD_GetFieldIndex(sourceLayerDefn, cSourceIDField) < 0 {
		return fmt.Errorf("PostGIS表中未找到ID字段: %s", options.SourceIDField)
	}

	// 如果不使用FID，验证GDB的OBJECTID字段
	if !options.UseFID {
		cTargetIDField := C.CString(options.TargetIDField)
		defer C.free(unsafe.Pointer(cTargetIDField))

		if C.OGR_FD_GetFieldIndex(targetLayerDefn, cTargetIDField) < 0 {
			return fmt.Errorf("GDB图层中未找到ID字段: %s", options.TargetIDField)
		}
	}

	// 验证数据字段
	if len(options.FieldMapping) > 0 {
		for sourceField, targetField := range options.FieldMapping {
			cSourceField := C.CString(sourceField)
			cTargetField := C.CString(targetField)

			if C.OGR_FD_GetFieldIndex(sourceLayerDefn, cSourceField) < 0 {
				C.free(unsafe.Pointer(cSourceField))
				C.free(unsafe.Pointer(cTargetField))
				return fmt.Errorf("PostGIS表中未找到字段: %s", sourceField)
			}
			if C.OGR_FD_GetFieldIndex(targetLayerDefn, cTargetField) < 0 {
				C.free(unsafe.Pointer(cSourceField))
				C.free(unsafe.Pointer(cTargetField))
				return fmt.Errorf("GDB图层中未找到字段: %s", targetField)
			}

			C.free(unsafe.Pointer(cSourceField))
			C.free(unsafe.Pointer(cTargetField))
		}
	} else {
		if options.SourceField == "" {
			return fmt.Errorf("必须指定SourceField或FieldMapping")
		}

		cSourceField := C.CString(options.SourceField)
		if C.OGR_FD_GetFieldIndex(sourceLayerDefn, cSourceField) < 0 {
			C.free(unsafe.Pointer(cSourceField))
			return fmt.Errorf("PostGIS表中未找到字段: %s", options.SourceField)
		}
		C.free(unsafe.Pointer(cSourceField))

		targetField := options.TargetField
		if targetField == "" {
			targetField = options.SourceField
		}
		cTargetField := C.CString(targetField)
		if C.OGR_FD_GetFieldIndex(targetLayerDefn, cTargetField) < 0 {
			C.free(unsafe.Pointer(cTargetField))
			return fmt.Errorf("GDB图层中未找到字段: %s", targetField)
		}
		C.free(unsafe.Pointer(cTargetField))
	}

	return nil
}

// ImportPostGISToGDB 将PostGIS数据表导入到GDB文件的指定图层
// postGISConfig: PostGIS配置
// gdbPath: GDB文件路径
// gdbLayerName: GDB目标图层名称
// options: 导入选项（可选）
func ImportPostGISToGDB(postGISConfig *PostGISConfig, gdbPath string, gdbLayerName string, options *ImportToGDBOptions) (*ImportResult, error) {

	// 设置默认选项
	if options == nil {
		options = &ImportToGDBOptions{}
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.GeomField == "" {
		options.GeomField = "geom"
	}

	result := &ImportResult{
		Errors: make([]string, 0),
	}

	// 1. 从PostGIS读取数据
	reader := NewPostGISReader(postGISConfig)
	sourceLayer, err := reader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取PostGIS表失败: %v", err)
	}
	defer sourceLayer.Close()

	// 2. 打开GDB数据源（可写模式）
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 3. 获取GDB目标图层
	cGDBLayerName := C.CString(gdbLayerName)
	defer C.free(unsafe.Pointer(cGDBLayerName))

	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cGDBLayerName)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法找到GDB图层: %s", gdbLayerName)
	}

	// 4. 验证几何类型是否匹配
	err = validateGeometryTypes(sourceLayer.layer, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("几何类型验证失败: %v", err)
	}

	// 5. 获取源图层和目标图层的空间参考
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)
	targetSRS := C.OGR_L_GetSpatialRef(targetLayer)

	// 6. 创建坐标转换对象（如果需要）
	var transform C.OGRCoordinateTransformationH
	needTransform := false

	if sourceSRS != nil && targetSRS != nil {
		if C.OSRIsSame(sourceSRS, targetSRS) == 0 {
			transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
			if transform != nil {
				needTransform = true
				defer C.OCTDestroyCoordinateTransformation(transform)
				fmt.Printf("创建坐标转换: 从PostGIS坐标系到GDB坐标系\n")
			} else {
				fmt.Println("警告: 无法创建坐标转换，将使用原始坐标")
			}
		}
	} else if sourceSRS == nil {
		fmt.Println("警告: PostGIS图层没有空间参考，假设与GDB图层相同")
	}

	// 7. 创建字段映射
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	fieldMapping, err := createImportFieldMapping(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}

	fmt.Printf("字段映射: %d 个字段将被导入\n", len(fieldMapping))

	// 8. 开始事务（如果支持）
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
			fmt.Println("警告: 无法启动事务，将使用非事务模式")
		}
	}

	// 9. 重置源图层读取位置
	C.OGR_L_ResetReading(sourceLayer.layer)

	// 10. 遍历源图层并导入要素
	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		result.TotalCount++

		// 创建目标要素
		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			result.Errors = append(result.Errors, "无法创建目标要素")
			continue
		}

		// 处理几何
		sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
		if sourceGeom != nil {
			// 克隆几何对象
			clonedGeom := C.OGR_G_Clone(sourceGeom)
			if clonedGeom != nil {
				// 执行坐标转换（如果需要）
				if needTransform && transform != nil {
					transformResult := C.OGR_G_Transform(clonedGeom, transform)
					if transformResult != C.OGRERR_NONE {
						if options.SkipInvalidGeometry {
							C.OGR_G_DestroyGeometry(clonedGeom)
							C.OGR_F_Destroy(targetFeature)
							C.OGR_F_Destroy(sourceFeature)
							result.SkippedCount++
							continue
						} else {
							errMsg := fmt.Sprintf("要素 %d: 几何转换失败", result.TotalCount)
							result.Errors = append(result.Errors, errMsg)
						}
					}
				}

				// 验证几何有效性
				if options.ValidateGeometry && C.OGR_G_IsValid(clonedGeom) == 0 {
					// 尝试修复几何
					fixedGeom := C.OGR_G_MakeValid(clonedGeom)
					if fixedGeom != nil && C.OGR_G_IsValid(fixedGeom) == 1 {
						C.OGR_G_DestroyGeometry(clonedGeom)
						clonedGeom = fixedGeom
					} else {
						if fixedGeom != nil {
							C.OGR_G_DestroyGeometry(fixedGeom)
						}
						if options.SkipInvalidGeometry {
							C.OGR_G_DestroyGeometry(clonedGeom)
							C.OGR_F_Destroy(targetFeature)
							C.OGR_F_Destroy(sourceFeature)
							result.SkippedCount++
							continue
						}
					}
				}

				// 设置几何到目标要素
				C.OGR_F_SetGeometry(targetFeature, clonedGeom)
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		} else if !options.AllowNullGeometry {
			// 源要素没有几何，且不允许空几何
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.SkippedCount++
			continue
		}

		// 复制属性字段
		err := copyImportFeatureFields(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)
		if err != nil && options.StrictMode {
			errMsg := fmt.Sprintf("要素 %d: 复制字段失败: %v", result.TotalCount, err)
			result.Errors = append(result.Errors, errMsg)
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}

		// 插入要素到目标图层
		createResult := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if createResult == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
			errMsg := fmt.Sprintf("要素 %d: 插入失败，错误代码: %d", result.TotalCount, int(createResult))
			result.Errors = append(result.Errors, errMsg)
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		// 批量同步
		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录，成功插入 %d 条...\n", result.TotalCount, result.InsertedCount)
		}
	}

	// 11. 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}

	// 12. 最终同步到磁盘
	if C.OGR_L_SyncToDisk(targetLayer) != C.OGRERR_NONE {
		return result, fmt.Errorf("同步到磁盘失败")
	}

	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)

	if len(result.Errors) > 0 && len(result.Errors) <= 10 {
		fmt.Printf("错误详情:\n")
		for _, err := range result.Errors {
			fmt.Printf("  - %s\n", err)
		}
	} else if len(result.Errors) > 10 {
		fmt.Printf("发生 %d 个错误（仅显示前10个）:\n", len(result.Errors))
		for i := 0; i < 10; i++ {
			fmt.Printf("  - %s\n", result.Errors[i])
		}
	}

	return result, nil
}

// ImportToGDBOptions 导入选项
type ImportToGDBOptions struct {
	GeomField           string            // PostGIS几何字段名（默认"geom"）
	BatchSize           int               // 批处理大小（默认1000）
	UseTransaction      bool              // 是否使用事务（默认false）
	StrictMode          bool              // 严格模式，遇到错误立即停止
	SkipInvalidGeometry bool              // 跳过无效几何（默认false）
	ValidateGeometry    bool              // 验证几何有效性（默认false）
	AllowNullGeometry   bool              // 允许空几何（默认false）
	FieldMapping        map[string]string // 字段映射（源字段->目标字段），为空则自动匹配同名字段
	ExcludeFields       []string          // 排除的字段列表
	IncludeFields       []string          // 仅包含的字段列表（优先级高于ExcludeFields）
}

// ImportResult 导入结果
type ImportResult struct {
	TotalCount    int      // 总记录数
	InsertedCount int      // 成功插入数
	FailedCount   int      // 失败数
	SkippedCount  int      // 跳过数
	Errors        []string // 错误信息列表
}

// ImportFieldMapping 导入字段映射结构
type ImportFieldMapping struct {
	SourceIndex int
	TargetIndex int
	SourceName  string
	TargetName  string
	SourceType  C.OGRFieldType
	TargetType  C.OGRFieldType
}

// validateGeometryTypes 验证源图层和目标图层的几何类型是否兼容
func validateGeometryTypes(sourceLayer, targetLayer C.OGRLayerH, options *ImportToGDBOptions) error {
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	sourceGeomType := C.OGR_FD_GetGeomType(sourceLayerDefn)
	targetGeomType := C.OGR_FD_GetGeomType(targetLayerDefn)

	// 获取基础几何类型（去除维度标志）
	sourceBaseType := getBaseGeometryType(sourceGeomType)
	targetBaseType := getBaseGeometryType(targetGeomType)

	fmt.Printf("源图层几何类型: %s, 目标图层几何类型: %s\n",
		getGeometryTypeName(sourceGeomType), getGeometryTypeName(targetGeomType))

	// 如果目标是Unknown类型，接受任何几何
	if targetBaseType == C.wkbUnknown {
		return nil
	}

	// 检查几何类型兼容性
	if !isGeometryTypeCompatible(sourceBaseType, targetBaseType) {
		return fmt.Errorf("几何类型不兼容: 源=%s, 目标=%s",
			getGeometryTypeName(sourceGeomType), getGeometryTypeName(targetGeomType))
	}

	return nil
}

// getBaseGeometryType 获取基础几何类型（去除维度标志）
func getBaseGeometryType(geomType C.OGRwkbGeometryType) C.OGRwkbGeometryType {
	// 使用GDAL的宏来获取基础类型
	baseType := int(geomType) % 1000

	// 处理25D类型
	if int(geomType)&0x80000000 != 0 {
		baseType = int(geomType) & 0x7FFFFFFF
	}

	return C.OGRwkbGeometryType(baseType)
}

// isGeometryTypeCompatible 检查几何类型是否兼容
func isGeometryTypeCompatible(sourceType, targetType C.OGRwkbGeometryType) bool {
	// 相同类型
	if sourceType == targetType {
		return true
	}

	// Point 可以导入到 MultiPoint
	if sourceType == C.wkbPoint && targetType == C.wkbMultiPoint {
		return true
	}

	// LineString 可以导入到 MultiLineString
	if sourceType == C.wkbLineString && targetType == C.wkbMultiLineString {
		return true
	}

	// Polygon 可以导入到 MultiPolygon
	if sourceType == C.wkbPolygon && targetType == C.wkbMultiPolygon {
		return true
	}

	// Multi类型可以导入到对应的单一类型（如果只有一个子几何）
	// 这种情况在实际导入时处理

	return false
}

// createImportFieldMapping 创建导入字段映射
func createImportFieldMapping(sourceLayerDefn, targetLayerDefn C.OGRFeatureDefnH, options *ImportToGDBOptions) ([]ImportFieldMapping, error) {
	var mappings []ImportFieldMapping

	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceLayerDefn))

	// 构建排除字段集合
	excludeSet := make(map[string]bool)
	for _, field := range options.ExcludeFields {
		excludeSet[field] = true
	}

	// 构建包含字段集合
	includeSet := make(map[string]bool)
	if len(options.IncludeFields) > 0 {
		for _, field := range options.IncludeFields {
			includeSet[field] = true
		}
	}

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		sourceFieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 检查是否在包含列表中（如果指定了包含列表）
		if len(includeSet) > 0 && !includeSet[sourceFieldName] {
			continue
		}

		// 检查是否在排除列表中
		if excludeSet[sourceFieldName] {
			continue
		}

		// 确定目标字段名
		targetFieldName := sourceFieldName
		if options.FieldMapping != nil {
			if mappedName, ok := options.FieldMapping[sourceFieldName]; ok {
				targetFieldName = mappedName
			}
		}

		// 在目标图层中查找字段
		cTargetFieldName := C.CString(targetFieldName)
		targetIndex := C.OGR_FD_GetFieldIndex(targetLayerDefn, cTargetFieldName)
		C.free(unsafe.Pointer(cTargetFieldName))

		if targetIndex >= 0 {
			sourceFieldType := C.OGR_Fld_GetType(sourceFieldDefn)
			targetFieldDefn := C.OGR_FD_GetFieldDefn(targetLayerDefn, targetIndex)
			targetFieldType := C.OGR_Fld_GetType(targetFieldDefn)

			mappings = append(mappings, ImportFieldMapping{
				SourceIndex: i,
				TargetIndex: int(targetIndex),
				SourceName:  sourceFieldName,
				TargetName:  targetFieldName,
				SourceType:  sourceFieldType,
				TargetType:  targetFieldType,
			})
		}
	}

	return mappings, nil
}

// copyImportFeatureFields 复制导入要素的字段
func copyImportFeatureFields(sourceFeature, targetFeature C.OGRFeatureH, mappings []ImportFieldMapping, targetLayerDefn C.OGRFeatureDefnH) error {
	for _, mapping := range mappings {
		// 检查源字段是否已设置
		if C.OGR_F_IsFieldSet(sourceFeature, C.int(mapping.SourceIndex)) == 0 {
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 检查是否为NULL
		if C.OGR_F_IsFieldNull(sourceFeature, C.int(mapping.SourceIndex)) != 0 {
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 复制字段值（带类型转换）
		err := copyImportFieldValue(sourceFeature, targetFeature,
			C.int(mapping.SourceIndex), C.int(mapping.TargetIndex),
			mapping.SourceType, mapping.TargetType)

		if err != nil {
			return fmt.Errorf("复制字段 '%s' 失败: %v", mapping.SourceName, err)
		}
	}

	return nil
}

// copyImportFieldValue 复制单个字段值（支持类型转换）
func copyImportFieldValue(sourceFeature, targetFeature C.OGRFeatureH,
	sourceIndex, targetIndex C.int,
	sourceType, targetType C.OGRFieldType) error {

	// 根据目标类型设置值
	switch targetType {
	case C.OFTInteger:
		var intVal C.int
		switch sourceType {
		case C.OFTInteger:
			intVal = C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		case C.OFTInteger64:
			intVal = C.int(C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex))
		case C.OFTReal:
			intVal = C.int(C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex))
		case C.OFTString:
			intVal = C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		default:
			intVal = C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldInteger(targetFeature, targetIndex, intVal)

	case C.OFTInteger64:
		var int64Val C.longlong
		switch sourceType {
		case C.OFTInteger:
			int64Val = C.longlong(C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex))
		case C.OFTInteger64:
			int64Val = C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		case C.OFTReal:
			int64Val = C.longlong(C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex))
		case C.OFTString:
			int64Val = C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		default:
			int64Val = C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldInteger64(targetFeature, targetIndex, int64Val)

	case C.OFTReal:
		var doubleVal C.double
		switch sourceType {
		case C.OFTInteger:
			doubleVal = C.double(C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex))
		case C.OFTInteger64:
			doubleVal = C.double(C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex))
		case C.OFTReal:
			doubleVal = C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		case C.OFTString:
			doubleVal = C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		default:
			doubleVal = C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldDouble(targetFeature, targetIndex, doubleVal)

	case C.OFTString:
		strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)

	case C.OFTDate, C.OFTTime, C.OFTDateTime:
		if sourceType == C.OFTDate || sourceType == C.OFTTime || sourceType == C.OFTDateTime {
			var year, month, day, hour, minute, second, tzflag C.int
			C.OGR_F_GetFieldAsDateTime(sourceFeature, sourceIndex,
				&year, &month, &day, &hour, &minute, &second, &tzflag)
			C.OGR_F_SetFieldDateTime(targetFeature, targetIndex,
				year, month, day, hour, minute, second, tzflag)
		} else {
			// 尝试从字符串解析
			strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)
		}

	default:
		// 默认转为字符串
		strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)
	}

	return nil
}

// ImportPostGISToNewGDBLayer 将PostGIS数据表导入到GDB文件，创建新图层
// 如果图层已存在，根据选项决定是覆盖还是追加
func ImportPostGISToNewGDBLayer(postGISConfig *PostGISConfig, gdbPath string, layerName string, options *ImportToGDBOptions) (*ImportResult, error) {

	// 设置默认选项
	if options == nil {
		options = &ImportToGDBOptions{}
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.GeomField == "" {
		options.GeomField = "geom"
	}

	result := &ImportResult{
		Errors: make([]string, 0),
	}

	// 1. 从PostGIS读取数据
	reader := NewPostGISReader(postGISConfig)
	sourceLayer, err := reader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取PostGIS表失败: %v", err)
	}
	defer sourceLayer.Close()

	// 2. 打开GDB数据源（可写模式）
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	// 获取FileGDB驱动
	cDriverName := C.CString("FileGDB")
	defer C.free(unsafe.Pointer(cDriverName))

	driver := C.OGRGetDriverByName(cDriverName)
	if driver == nil {
		// 尝试OpenFileGDB
		cDriverName2 := C.CString("OpenFileGDB")
		defer C.free(unsafe.Pointer(cDriverName2))
		driver = C.OGRGetDriverByName(cDriverName2)
		if driver == nil {
			return nil, fmt.Errorf("无法获取GDB驱动")
		}
	}

	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 3. 检查图层是否已存在
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	existingLayer := C.OGR_DS_GetLayerByName(targetDataset, cLayerName)
	if existingLayer != nil {
		// 图层已存在，使用现有的导入函数
		fmt.Printf("图层 '%s' 已存在，将追加数据\n", layerName)
		return ImportPostGISToGDB(postGISConfig, gdbPath, layerName, options)
	}

	// 4. 获取源图层信息
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	sourceGeomType := C.OGR_FD_GetGeomType(sourceLayerDefn)
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)

	// 5. 创建新图层
	targetLayer := C.OGR_DS_CreateLayer(targetDataset, cLayerName, sourceSRS, sourceGeomType, nil)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法创建图层: %s", layerName)
	}

	// 6. 复制字段定义
	err = copyFieldDefinitions(sourceLayerDefn, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("复制字段定义失败: %v", err)
	}

	// 7. 创建字段映射
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)
	fieldMapping, err := createImportFieldMapping(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}

	// 8. 开始事务
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
		}
	}

	// 9. 导入要素
	C.OGR_L_ResetReading(sourceLayer.layer)

	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		result.TotalCount++

		// 创建目标要素
		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}

		// 继续 ImportPostGISToNewGDBLayer 函数

		// 复制几何
		sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
		if sourceGeom != nil {
			clonedGeom := C.OGR_G_Clone(sourceGeom)
			if clonedGeom != nil {
				// 验证几何有效性
				if options.ValidateGeometry && C.OGR_G_IsValid(clonedGeom) == 0 {
					fixedGeom := C.OGR_G_MakeValid(clonedGeom)
					if fixedGeom != nil && C.OGR_G_IsValid(fixedGeom) == 1 {
						C.OGR_G_DestroyGeometry(clonedGeom)
						clonedGeom = fixedGeom
					} else {
						if fixedGeom != nil {
							C.OGR_G_DestroyGeometry(fixedGeom)
						}
						if options.SkipInvalidGeometry {
							C.OGR_G_DestroyGeometry(clonedGeom)
							C.OGR_F_Destroy(targetFeature)
							C.OGR_F_Destroy(sourceFeature)
							result.SkippedCount++
							continue
						}
					}
				}

				C.OGR_F_SetGeometry(targetFeature, clonedGeom)
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		} else if !options.AllowNullGeometry {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.SkippedCount++
			continue
		}

		// 复制属性字段
		err := copyImportFeatureFields(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)
		if err != nil && options.StrictMode {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: %v", result.TotalCount, err))
			continue
		}

		// 插入要素
		createResult := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if createResult == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
			result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: 插入失败", result.TotalCount))
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录...\n", result.TotalCount)
		}
	}

	// 10. 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}

	// 11. 同步到磁盘
	C.OGR_L_SyncToDisk(targetLayer)

	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)

	return result, nil
}

// copyFieldDefinitions 复制字段定义到目标图层
func copyFieldDefinitions(sourceLayerDefn C.OGRFeatureDefnH, targetLayer C.OGRLayerH, options *ImportToGDBOptions) error {
	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceLayerDefn))

	// 构建排除字段集合
	excludeSet := make(map[string]bool)
	for _, field := range options.ExcludeFields {
		excludeSet[field] = true
	}

	// 构建包含字段集合
	includeSet := make(map[string]bool)
	if len(options.IncludeFields) > 0 {
		for _, field := range options.IncludeFields {
			includeSet[field] = true
		}
	}

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 检查是否在包含列表中
		if len(includeSet) > 0 && !includeSet[fieldName] {
			continue
		}

		// 检查是否在排除列表中
		if excludeSet[fieldName] {
			continue
		}

		// 确定目标字段名
		targetFieldName := fieldName
		if options.FieldMapping != nil {
			if mappedName, ok := options.FieldMapping[fieldName]; ok {
				targetFieldName = mappedName
			}
		}

		// 创建新的字段定义
		cTargetFieldName := C.CString(targetFieldName)
		fieldType := C.OGR_Fld_GetType(sourceFieldDefn)
		newFieldDefn := C.OGR_Fld_Create(cTargetFieldName, fieldType)
		C.free(unsafe.Pointer(cTargetFieldName))

		if newFieldDefn == nil {
			continue
		}

		// 复制字段属性
		width := C.OGR_Fld_GetWidth(sourceFieldDefn)
		precision := C.OGR_Fld_GetPrecision(sourceFieldDefn)
		nullable := C.OGR_Fld_IsNullable(sourceFieldDefn)

		if width > 0 {
			C.OGR_Fld_SetWidth(newFieldDefn, width)
		}
		if precision > 0 {
			C.OGR_Fld_SetPrecision(newFieldDefn, precision)
		}
		C.OGR_Fld_SetNullable(newFieldDefn, nullable)

		// 添加字段到目标图层
		result := C.OGR_L_CreateField(targetLayer, newFieldDefn, C.int(1))
		C.OGR_Fld_Destroy(newFieldDefn)

		if result != C.OGRERR_NONE {
			fmt.Printf("警告: 创建字段 '%s' 失败\n", targetFieldName)
		}
	}

	return nil
}

// PostGISReaderFiltered 带过滤条件的PostGIS读取器
type PostGISReaderFiltered struct {
	config      *PostGISConfig
	whereClause string
}

// ReadGeometryTable 读取带过滤条件的几何表
func (r *PostGISReaderFiltered) ReadGeometryTable() (*GDALLayer, error) {
	// 构建连接字符串
	connStr := fmt.Sprintf("PG:host=%s port=%s dbname=%s user=%s password=%s schemas=%s tables=%s",
		r.config.Host,
		r.config.Port,
		r.config.Database,
		r.config.User,
		r.config.Password,
		r.config.Schema,
		r.config.Table,
	)

	cConnStr := C.CString(connStr)
	defer C.free(unsafe.Pointer(cConnStr))

	// 打开数据源
	hDataSource := C.OGROpen(cConnStr, 0, nil)
	if hDataSource == nil {
		return nil, fmt.Errorf("无法连接到PostGIS数据库")
	}

	// 获取图层
	hLayer := C.OGR_DS_GetLayer(hDataSource, 0)
	if hLayer == nil {
		C.OGRReleaseDataSource(hDataSource)
		return nil, fmt.Errorf("无法获取图层")
	}

	// 应用过滤条件
	if r.whereClause != "" {
		cWhereClause := C.CString(r.whereClause)
		defer C.free(unsafe.Pointer(cWhereClause))

		result := C.OGR_L_SetAttributeFilter(hLayer, cWhereClause)
		if result != C.OGRERR_NONE {
			C.OGRReleaseDataSource(hDataSource)
			return nil, fmt.Errorf("设置过滤条件失败: %s", r.whereClause)
		}
	}

	return &GDALLayer{
		layer:   hLayer,
		dataset: hDataSource,
	}, nil
}

// ImportGDALLayerToGDB 将GDALLayer直接导入到GDB（通用方法）
// 这个函数可以处理任何来源的GDALLayer，不仅限于PostGIS
func ImportGDALLayerToGDB(sourceLayer *GDALLayer, gdbPath string, gdbLayerName string, options *ImportToGDBOptions) (*ImportResult, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	// 设置默认选项
	if options == nil {
		options = &ImportToGDBOptions{}
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}

	result := &ImportResult{
		Errors: make([]string, 0),
	}

	// 打开GDB数据源
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 获取目标图层
	cGDBLayerName := C.CString(gdbLayerName)
	defer C.free(unsafe.Pointer(cGDBLayerName))

	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cGDBLayerName)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法找到GDB图层: %s", gdbLayerName)
	}

	// 验证几何类型
	err := validateGeometryTypes(sourceLayer.layer, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("几何类型验证失败: %v", err)
	}

	// 获取坐标转换
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)
	targetSRS := C.OGR_L_GetSpatialRef(targetLayer)

	var transform C.OGRCoordinateTransformationH
	needTransform := false

	if sourceSRS != nil && targetSRS != nil && C.OSRIsSame(sourceSRS, targetSRS) == 0 {
		transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
		if transform != nil {
			needTransform = true
			defer C.OCTDestroyCoordinateTransformation(transform)
		}
	}

	// 创建字段映射
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	fieldMapping, err := createImportFieldMapping(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}

	// 开始事务
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		C.OGR_L_StartTransaction(targetLayer)
	}

	// 导入要素
	C.OGR_L_ResetReading(sourceLayer.layer)

	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		result.TotalCount++

		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}

		// 处理几何
		sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
		if sourceGeom != nil {
			clonedGeom := C.OGR_G_Clone(sourceGeom)
			if clonedGeom != nil {
				if needTransform && transform != nil {
					C.OGR_G_Transform(clonedGeom, transform)
				}

				if options.ValidateGeometry && C.OGR_G_IsValid(clonedGeom) == 0 {
					fixedGeom := C.OGR_G_MakeValid(clonedGeom)
					if fixedGeom != nil {
						C.OGR_G_DestroyGeometry(clonedGeom)
						clonedGeom = fixedGeom
					}
				}

				C.OGR_F_SetGeometry(targetFeature, clonedGeom)
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		}

		// 复制字段
		copyImportFeatureFields(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)

		// 插入要素
		if C.OGR_L_CreateFeature(targetLayer, targetFeature) == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录...\n", result.TotalCount)
		}
	}

	// 提交事务
	if useTransaction {
		C.OGR_L_CommitTransaction(targetLayer)
	}

	// 同步到磁盘
	C.OGR_L_SyncToDisk(targetLayer)

	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)

	return result, nil
}

// DeleteLayer 删除GDB文件中的指定图层
// gdbPath: GDB文件路径
// layerName: 要删除的图层名称
func DeleteLayer(gdbPath string, layerName string) error { // 初始化GDAL

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil) // 1表示可写
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 检查数据源是否支持删除图层操作
	if C.OGR_DS_TestCapability(dataset, C.CString("DeleteLayer")) == 0 {
		return fmt.Errorf("该数据源不支持删除图层操作")
	}

	// 获取图层数量
	layerCount := int(C.OGR_DS_GetLayerCount(dataset))
	if layerCount == 0 {
		return fmt.Errorf("GDB文件中没有图层")
	}

	// 查找图层索引
	layerIndex := -1
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	for i := 0; i < layerCount; i++ {
		layer := C.OGR_DS_GetLayer(dataset, C.int(i))
		if layer == nil {
			continue
		}

		currentLayerName := C.GoString(C.OGR_L_GetName(layer))
		if currentLayerName == layerName {
			layerIndex = i
			break
		}
	}

	if layerIndex < 0 {
		return fmt.Errorf("未找到图层: %s", layerName)
	}

	// 删除图层
	result := C.OGR_DS_DeleteLayer(dataset, C.int(layerIndex))
	if result != C.OGRERR_NONE {
		return fmt.Errorf("删除图层失败，错误代码: %d", int(result))
	}

	fmt.Printf("成功删除图层: %s\n", layerName)
	return nil
}

// DeleteLayerByIndex 通过索引删除GDB文件中的图层
// gdbPath: GDB文件路径
// layerIndex: 图层索引（从0开始）
func DeleteLayerByIndex(gdbPath string, layerIndex int) error {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 检查数据源是否支持删除图层操作
	if C.OGR_DS_TestCapability(dataset, C.CString("DeleteLayer")) == 0 {
		return fmt.Errorf("该数据源不支持删除图层操作")
	}

	// 检查索引有效性
	layerCount := int(C.OGR_DS_GetLayerCount(dataset))
	if layerIndex < 0 || layerIndex >= layerCount {
		return fmt.Errorf("图层索引无效: %d (有效范围: 0-%d)", layerIndex, layerCount-1)
	}

	// 获取图层名称（用于日志）
	layer := C.OGR_DS_GetLayer(dataset, C.int(layerIndex))
	layerName := ""
	if layer != nil {
		layerName = C.GoString(C.OGR_L_GetName(layer))
	}

	// 删除图层
	result := C.OGR_DS_DeleteLayer(dataset, C.int(layerIndex))
	if result != C.OGRERR_NONE {
		return fmt.Errorf("删除图层失败，错误代码: %d", int(result))
	}

	fmt.Printf("成功删除图层[%d]: %s\n", layerIndex, layerName)
	return nil
}

// DeleteMultipleLayers 批量删除GDB文件中的多个图层
// gdbPath: GDB文件路径
// layerNames: 要删除的图层名称列表
// continueOnError: 遇到错误是否继续删除其他图层
func DeleteMultipleLayers(gdbPath string, layerNames []string, continueOnError bool) (int, []error) {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return 0, []error{fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)}
	}
	defer C.OGR_DS_Destroy(dataset)

	// 检查数据源是否支持删除图层操作
	if C.OGR_DS_TestCapability(dataset, C.CString("DeleteLayer")) == 0 {
		return 0, []error{fmt.Errorf("该数据源不支持删除图层操作")}
	}

	deletedCount := 0
	var errors []error

	// 注意：删除图层后索引会变化，所以需要每次重新查找
	for _, layerName := range layerNames {
		// 重新获取图层数量和索引
		layerCount := int(C.OGR_DS_GetLayerCount(dataset))
		layerIndex := -1

		for i := 0; i < layerCount; i++ {
			layer := C.OGR_DS_GetLayer(dataset, C.int(i))
			if layer == nil {
				continue
			}

			currentLayerName := C.GoString(C.OGR_L_GetName(layer))
			if currentLayerName == layerName {
				layerIndex = i
				break
			}
		}

		if layerIndex < 0 {
			err := fmt.Errorf("未找到图层: %s", layerName)
			errors = append(errors, err)
			if !continueOnError {
				return deletedCount, errors
			}
			continue
		}

		// 删除图层
		result := C.OGR_DS_DeleteLayer(dataset, C.int(layerIndex))
		if result != C.OGRERR_NONE {
			err := fmt.Errorf("删除图层 '%s' 失败，错误代码: %d", layerName, int(result))
			errors = append(errors, err)
			if !continueOnError {
				return deletedCount, errors
			}
			continue
		}

		deletedCount++
		fmt.Printf("成功删除图层: %s\n", layerName)
	}

	fmt.Printf("批量删除完成: 成功 %d 个，失败 %d 个\n", deletedCount, len(errors))
	return deletedCount, errors
}

// LayerExists 检查GDB文件中是否存在指定图层
// gdbPath: GDB文件路径
// layerName: 图层名称
func LayerExists(gdbPath string, layerName string) (bool, error) {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以只读模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return false, fmt.Errorf("无法打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 查找图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	return layer != nil, nil
}

// GetLayerNames 获取GDB文件中所有图层的名称
// gdbPath: GDB文件路径
func GetLayerNames(gdbPath string) ([]string, error) {

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以只读模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层数量
	layerCount := int(C.OGR_DS_GetLayerCount(dataset))
	layerNames := make([]string, 0, layerCount)

	for i := 0; i < layerCount; i++ {
		layer := C.OGR_DS_GetLayer(dataset, C.int(i))
		if layer == nil {
			continue
		}

		layerName := C.GoString(C.OGR_L_GetName(layer))
		layerNames = append(layerNames, layerName)
	}

	return layerNames, nil
}
