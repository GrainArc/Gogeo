package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// DeleteShapeFeatureByFID 删除Shapefile图层中指定FID的要素
// shpPath: Shapefile文件路径（.shp文件）
// fid: 要删除的要素ID
func DeleteShapeFeatureByFID(shpPath string, fid int64) error {
	// 初始化GDAL
	InitializeGDAL()

	cFilePath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开Shapefile
	dataset := C.OGROpen(cFilePath, C.int(1), nil) // 1表示可写
	if dataset == nil {
		return fmt.Errorf("无法以可写模式打开Shapefile: %s", shpPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取第一个图层（Shapefile通常只有一个图层）
	layer := C.OGR_DS_GetLayer(dataset, 0)
	if layer == nil {
		return fmt.Errorf("无法获取Shapefile图层")
	}

	// 检查图层是否支持删除操作
	cDeleteCap := C.CString("DeleteFeature")
	defer C.free(unsafe.Pointer(cDeleteCap))

	if C.OGR_L_TestCapability(layer, cDeleteCap) == 0 {
		return fmt.Errorf("Shapefile图层不支持删除要素操作")
	}

	// 删除要素
	result := C.OGR_L_DeleteFeature(layer, C.GIntBig(fid))
	if result != C.OGRERR_NONE {
		return fmt.Errorf("删除要素失败，FID=%d，错误代码: %d", fid, int(result))
	}

	// 同步更改到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("成功删除Shapefile中 FID=%d 的要素\n", fid)
	return nil
}

// DeleteShapeFeaturesByFilter 根据SQL过滤条件删除Shapefile中的多个要素
// shpPath: Shapefile文件路径
// whereClause: SQL WHERE条件，如 "ID > 100" 或 "NAME = 'test'"
func DeleteShapeFeaturesByFilter(shpPath string, whereClause string) (int, error) {
	// 初始化GDAL
	InitializeGDAL()

	cFilePath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开数据源
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return 0, fmt.Errorf("无法以可写模式打开Shapefile: %s", shpPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取图层
	layer := C.OGR_DS_GetLayer(dataset, 0)
	if layer == nil {
		return 0, fmt.Errorf("无法获取Shapefile图层")
	}

	// 检查图层是否支持删除操作
	cDeleteCap := C.CString("DeleteFeature")
	defer C.free(unsafe.Pointer(cDeleteCap))

	if C.OGR_L_TestCapability(layer, cDeleteCap) == 0 {
		return 0, fmt.Errorf("Shapefile图层不支持删除要素操作")
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

// InsertLayerToShapefile 将GDALLayer插入到Shapefile中，并进行坐标转换
// sourceLayer: 源图层（4326坐标系）
// shpPath: 目标Shapefile文件路径
// options: 插入选项（可选）
func InsertLayerToShapefile(sourceLayer *GDALLayer, shpPath string, options *InsertOptions) error {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return fmt.Errorf("源图层为空")
	}

	// 初始化GDAL
	InitializeGDAL()

	cFilePath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开Shapefile数据源
	targetDataset := C.OGROpen(cFilePath, C.int(1), nil)
	if targetDataset == nil {
		return fmt.Errorf("无法以可写模式打开Shapefile: %s", shpPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)

	// 获取目标图层（Shapefile只有一个图层）
	targetLayer := C.OGR_DS_GetLayer(targetDataset, 0)
	if targetLayer == nil {
		return fmt.Errorf("无法获取Shapefile图层")
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
		return fmt.Errorf("目标Shapefile没有空间参考系统")
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

	// 开始事务（Shapefile通常不支持事务，但尝试一下）
	cTransactionCap := C.CString("Transactions")
	defer C.free(unsafe.Pointer(cTransactionCap))

	useTransaction := C.OGR_L_TestCapability(targetLayer, cTransactionCap) != 0
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
				// 检查几何有效性（可选）
				if options != nil && options.SkipInvalidGeometry {
					if C.OGR_G_IsValid(clonedGeom) == 0 {
						fmt.Printf("警告: 几何无效，跳过该要素\n")
						C.OGR_G_DestroyGeometry(clonedGeom)
						C.OGR_F_Destroy(targetFeature)
						C.OGR_F_Destroy(sourceFeature)
						failedCount++
						continue
					}
				}

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

// PackShapefile 压缩Shapefile以回收删除要素后的空间
// shpPath: Shapefile文件路径
// 注意：此操作会重建Shapefile，FID可能会改变
func PackShapefile(shpPath string) error {
	InitializeGDAL()

	cFilePath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return fmt.Errorf("无法打开Shapefile: %s", shpPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	layer := C.OGR_DS_GetLayer(dataset, 0)
	if layer == nil {
		return fmt.Errorf("无法获取图层")
	}

	// 执行SQL REPACK命令（如果驱动支持）
	cSQL := C.CString("REPACK " + shpPath)
	defer C.free(unsafe.Pointer(cSQL))

	result := C.OGR_DS_ExecuteSQL(dataset, cSQL, nil, nil)
	if result != nil {
		C.OGR_DS_ReleaseResultSet(dataset, result)
	}

	// 同步到磁盘
	C.OGR_L_SyncToDisk(layer)

	fmt.Printf("Shapefile压缩完成: %s\n", shpPath)
	return nil
}

// EnsureObjectIDField 确保shp文件包含objectid字段（不区分大小写）
// 如果不存在，则创建该字段并填充唯一值
// shpPath: Shapefile文件路径
// 返回: 是否创建了新字段, error
func EnsureObjectIDField(shpPath string) (bool, error) {
	// 初始化GDAL
	InitializeGDAL()

	cFilePath := C.CString(shpPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 以可写模式打开Shapefile
	dataset := C.OGROpen(cFilePath, C.int(1), nil) // 1表示可写
	if dataset == nil {
		return false, fmt.Errorf("无法以可写模式打开Shapefile: %s", shpPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取第一个图层（Shapefile通常只有一个图层）
	layer := C.OGR_DS_GetLayer(dataset, 0)
	if layer == nil {
		return false, fmt.Errorf("无法获取Shapefile图层")
	}

	// 获取图层定义
	layerDefn := C.OGR_L_GetLayerDefn(layer)
	if layerDefn == nil {
		return false, fmt.Errorf("无法获取图层定义")
	}

	// 检查是否已存在objectid字段（不区分大小写）
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))
	objectIDFieldIndex := -1

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		if fieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		// 不区分大小写比较
		if len(fieldName) == 8 &&
			(fieldName == "objectid" || fieldName == "OBJECTID" ||
				fieldName == "ObjectID" || fieldName == "ObjectId" ||
				fieldName == "objectId" || fieldName == "Objectid") {
			objectIDFieldIndex = i
			fmt.Printf("找到已存在的ObjectID字段: %s (索引: %d)\n", fieldName, i)
			break
		}
	}

	// 如果字段已存在，不需要创建
	if objectIDFieldIndex >= 0 {
		fmt.Println("ObjectID字段已存在，无需创建")
		return false, nil
	}

	// 检查图层是否支持字段创建
	if C.OGR_L_TestCapability(layer, C.CString("CreateField")) == 0 {
		return false, fmt.Errorf("图层不支持创建字段操作")
	}

	// 创建新的objectid字段定义
	cFieldName := C.CString("objectid")
	defer C.free(unsafe.Pointer(cFieldName))

	fieldDefn := C.OGR_Fld_Create(cFieldName, C.OFTInteger)
	if fieldDefn == nil {
		return false, fmt.Errorf("无法创建字段定义")
	}
	defer C.OGR_Fld_Destroy(fieldDefn)

	// 设置字段宽度（可选）
	C.OGR_Fld_SetWidth(fieldDefn, 10)

	// 添加字段到图层
	result := C.OGR_L_CreateField(layer, fieldDefn, C.int(1)) // 1表示强制创建
	if result != C.OGRERR_NONE {
		return false, fmt.Errorf("创建objectid字段失败，错误代码: %d", int(result))
	}

	fmt.Println("成功创建objectid字段")

	// 开始事务（如果支持）
	useTransaction := C.OGR_L_TestCapability(layer, C.CString("Transactions")) != 0
	if useTransaction {
		transResult := C.OGR_L_StartTransaction(layer)
		if transResult != C.OGRERR_NONE {
			useTransaction = false
			fmt.Println("警告: 无法开始事务，将直接更新")
		}
	}

	// 重新获取图层定义（因为添加了新字段）
	layerDefn = C.OGR_L_GetLayerDefn(layer)

	// 获取新创建字段的索引
	newFieldIndex := C.OGR_FD_GetFieldIndex(layerDefn, cFieldName)
	if newFieldIndex < 0 {
		return false, fmt.Errorf("无法找到新创建的objectid字段")
	}

	// 遍历所有要素，填充唯一的objectid值
	C.OGR_L_ResetReading(layer)

	objectIDValue := 1
	updatedCount := 0
	failedCount := 0

	for {
		feature := C.OGR_L_GetNextFeature(layer)
		if feature == nil {
			break
		}

		// 设置objectid字段值
		C.OGR_F_SetFieldInteger(feature, newFieldIndex, C.int(objectIDValue))

		// 更新要素
		updateResult := C.OGR_L_SetFeature(layer, feature)
		if updateResult == C.OGRERR_NONE {
			updatedCount++
			objectIDValue++
		} else {
			failedCount++
			fmt.Printf("警告: 更新要素失败，FID=%d, 错误代码: %d\n",
				int64(C.OGR_F_GetFID(feature)), int(updateResult))
		}

		C.OGR_F_Destroy(feature)
	}

	// 提交事务
	if useTransaction {
		commitResult := C.OGR_L_CommitTransaction(layer)
		if commitResult != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(layer)
			return false, fmt.Errorf("提交事务失败")
		}
	}

	// 同步到磁盘
	syncResult := C.OGR_L_SyncToDisk(layer)
	if syncResult != C.OGRERR_NONE {
		return false, fmt.Errorf("同步到磁盘失败，错误代码: %d", int(syncResult))
	}

	fmt.Printf("ObjectID字段填充完成: 成功 %d 个，失败 %d 个\n", updatedCount, failedCount)

	if failedCount > 0 {
		return true, fmt.Errorf("部分要素更新失败: %d/%d", failedCount, updatedCount+failedCount)
	}

	return true, nil
}
