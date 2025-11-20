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
	// 初始化GDAL
	InitializeGDAL()

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
	// 初始化GDAL
	InitializeGDAL()

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

	// 初始化GDAL
	InitializeGDAL()

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

// InsertLayerToGDBWithProgress 带进度回调的插入函数
func InsertLayerToGDBWithProgress(sourceLayer *GDALLayer, gdbPath string, targetLayerName string,
	options *InsertOptions, progressCallback func(current, total int)) error {

	if sourceLayer == nil || sourceLayer.layer == nil {
		return fmt.Errorf("源图层为空")
	}

	InitializeGDAL()

	// 获取总要素数
	totalFeatures := int(C.OGR_L_GetFeatureCount(sourceLayer.layer, 1))

	cFilePath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cFilePath))

	targetDataset := C.OGROpen(cFilePath, C.int(1), nil)
	if targetDataset == nil {
		return fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	cTargetLayerName := C.CString(targetLayerName)
	defer C.free(unsafe.Pointer(cTargetLayerName))

	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cTargetLayerName)
	if targetLayer == nil {
		return fmt.Errorf("无法找到目标图层: %s", targetLayerName)
	}

	// 获取空间参考
	sourceSRS := C.OGR_L_GetSpatialRef(sourceLayer.layer)
	if sourceSRS == nil {
		sourceSRS = C.OSRNewSpatialReference(nil)
		defer C.OSRDestroySpatialReference(sourceSRS)
		C.OSRImportFromEPSG(sourceSRS, 4326)
	}

	targetSRS := C.OGR_L_GetSpatialRef(targetLayer)
	if targetSRS == nil {
		return fmt.Errorf("目标图层没有空间参考系统")
	}

	// 创建坐标转换
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

	// 开始事务
	useTransaction := C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		C.OGR_L_StartTransaction(targetLayer)
	}

	// 遍历并插入
	C.OGR_L_ResetReading(sourceLayer.layer)

	insertedCount := 0
	processedCount := 0

	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		processedCount++

		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			continue
		}

		// 处理几何
		sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)
		if sourceGeom != nil {
			clonedGeom := C.OGR_G_Clone(sourceGeom)
			if clonedGeom != nil {
				// 检查几何有效性
				if options != nil && options.SkipInvalidGeometry {
					if C.OGR_G_IsValid(clonedGeom) == 0 {
						fmt.Printf("警告: 第 %d 个要素几何无效，跳过\n", processedCount)
						C.OGR_G_DestroyGeometry(clonedGeom)
						C.OGR_F_Destroy(targetFeature)
						C.OGR_F_Destroy(sourceFeature)

						if progressCallback != nil {
							progressCallback(processedCount, totalFeatures)
						}
						continue
					}
				}

				// 坐标转换
				if needTransform && transform != nil {
					C.OGR_G_Transform(clonedGeom, transform)
				}

				C.OGR_F_SetGeometry(targetFeature, clonedGeom)
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		}

		// 复制字段
		copyFeatureFields(sourceFeature, targetFeature, fieldMapping)

		// 插入
		result := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if result == C.OGRERR_NONE {
			insertedCount++
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		// 进度回调
		if progressCallback != nil {
			progressCallback(processedCount, totalFeatures)
		}

		// 定期同步
		if options != nil && options.SyncInterval > 0 && insertedCount%options.SyncInterval == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
		}
	}

	// 提交事务
	if useTransaction {
		C.OGR_L_CommitTransaction(targetLayer)
	}

	C.OGR_L_SyncToDisk(targetLayer)

	fmt.Printf("插入完成: 成功 %d/%d 个要素\n", insertedCount, totalFeatures)

	return nil
}
