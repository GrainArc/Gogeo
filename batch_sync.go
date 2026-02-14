// gogeo/batch_sync.go
package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// SyncTask 描述一次批量同步的全部操作
type SyncTask struct {
	FilePath      string
	LayerName     string // SHP 留空
	IsShapefile   bool
	KeyField      string // 源文件中用于定位的字段名，如 OBJECTID / FID
	DeleteIDs     []int64
	UpdatePairs   []UpdatePair
	InsertSources []*GDALLayer
	Options       *InsertOptions
}

// UpdatePair 一个更新操作 = 删除源文件中的 SourceID + 插入新的 GDALLayer
type UpdatePair struct {
	SourceID    int64
	InsertLayer *GDALLayer
}

func BatchSyncToFile(task *SyncTask) (*SyncResult, error) {
	result := &SyncResult{}

	cFilePath := C.CString(task.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// ========== 只打开一次 ==========
	dataset := C.OGROpen(cFilePath, C.int(1), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开文件: %s", task.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	var targetLayer C.OGRLayerH
	if task.IsShapefile {
		targetLayer = C.OGR_DS_GetLayer(dataset, 0)
	} else {
		cLayerName := C.CString(task.LayerName)
		defer C.free(unsafe.Pointer(cLayerName))
		targetLayer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	}
	if targetLayer == nil {
		return nil, fmt.Errorf("无法获取目标图层")
	}

	cDeleteCap := C.CString("DeleteFeature")
	defer C.free(unsafe.Pointer(cDeleteCap))
	canDelete := C.OGR_L_TestCapability(targetLayer, cDeleteCap) != 0

	targetSRS := C.OGR_L_GetSpatialRef(targetLayer)
	if targetSRS == nil {
		return nil, fmt.Errorf("目标图层没有空间参考")
	}

	// 开启事务
	cTxnCap := C.CString("Transactions")
	defer C.free(unsafe.Pointer(cTxnCap))
	useTxn := C.OGR_L_TestCapability(targetLayer, cTxnCap) != 0
	if useTxn {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTxn = false
		}
	}

	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	result.TotalCount = len(task.DeleteIDs) + len(task.UpdatePairs)
	for _, src := range task.InsertSources {
		if src != nil && src.layer != nil {
			C.OGR_L_ResetReading(src.layer)
			count := 0
			for {
				f := C.OGR_L_GetNextFeature(src.layer)
				if f == nil {
					break
				}
				count++
				C.OGR_F_Destroy(f)
			}
			result.TotalCount += count
		}
	}

	// ========== Step 1: 批量删除 ==========
	if canDelete && len(task.DeleteIDs) > 0 {
		whereClause := buildWhereClauseInternal(task.KeyField, task.DeleteIDs)
		deleted, err := deleteByFilter(targetLayer, whereClause)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("批量删除失败: %v", err))
			result.FailedCount += len(task.DeleteIDs)
		} else {
			result.UpdatedCount += deleted
			if deleted < len(task.DeleteIDs) {
				result.SkippedCount += len(task.DeleteIDs) - deleted
			}
		}
	} else if !canDelete && len(task.DeleteIDs) > 0 {
		result.Errors = append(result.Errors, "图层不支持删除操作")
		result.SkippedCount += len(task.DeleteIDs)
	}

	// ========== Step 2: 更新（先删后插） ==========
	if canDelete {
		for _, pair := range task.UpdatePairs {
			wc := buildWhereClauseInternal(task.KeyField, []int64{pair.SourceID})
			deleteByFilter(targetLayer, wc)

			if pair.InsertLayer != nil && pair.InsertLayer.layer != nil {
				count, err := insertFromLayer(pair.InsertLayer, targetLayer, targetLayerDefn, targetSRS, task.Options)
				if err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("更新要素失败(SourceID=%d): %v", pair.SourceID, err))
					result.FailedCount++
				} else if count > 0 {
					result.UpdatedCount++
				} else {
					result.SkippedCount++
				}
			} else {
				result.SkippedCount++
			}
		}
	} else if len(task.UpdatePairs) > 0 {
		result.Errors = append(result.Errors, "图层不支持删除操作，无法执行更新")
		result.SkippedCount += len(task.UpdatePairs)
	}

	// ========== Step 3: 批量新增 ==========
	for _, srcLayer := range task.InsertSources {
		if srcLayer == nil || srcLayer.layer == nil {
			continue
		}
		count, err := insertFromLayer(srcLayer, targetLayer, targetLayerDefn, targetSRS, task.Options)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("批量插入失败: %v", err))
			result.FailedCount += count
		} else {
			result.UpdatedCount += count
		}
	}

	// ========== 提交事务 & 同步 ==========
	if useTxn {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}
	C.OGR_L_SyncToDisk(targetLayer)

	return result, nil
}

func deleteByFilter(layer C.OGRLayerH, whereClause string) (int, error) {
	cWhere := C.CString(whereClause)
	defer C.free(unsafe.Pointer(cWhere))

	if C.OGR_L_SetAttributeFilter(layer, cWhere) != C.OGRERR_NONE {
		return 0, fmt.Errorf("设置过滤器失败: %s", whereClause)
	}

	var fids []C.GIntBig
	C.OGR_L_ResetReading(layer)
	for {
		f := C.OGR_L_GetNextFeature(layer)
		if f == nil {
			break
		}
		fids = append(fids, C.OGR_F_GetFID(f))
		C.OGR_F_Destroy(f)
	}
	C.OGR_L_SetAttributeFilter(layer, nil)

	deleted := 0
	for _, fid := range fids {
		if C.OGR_L_DeleteFeature(layer, fid) == C.OGRERR_NONE {
			deleted++
		}
	}
	return deleted, nil
}

func insertFromLayer(src *GDALLayer, targetLayer C.OGRLayerH, targetDefn C.OGRFeatureDefnH, targetSRS C.OGRSpatialReferenceH, options *InsertOptions) (int, error) {
	srcSRS := C.OGR_L_GetSpatialRef(src.layer)
	if srcSRS == nil {
		srcSRS = C.OSRNewSpatialReference(nil)
		defer C.OSRDestroySpatialReference(srcSRS)
		C.OSRImportFromEPSG(srcSRS, 4326)
	}

	var transform C.OGRCoordinateTransformationH
	needTransform := C.OSRIsSame(srcSRS, targetSRS) == 0
	if needTransform {
		transform = C.OCTNewCoordinateTransformation(srcSRS, targetSRS)
		if transform == nil {
			return 0, fmt.Errorf("无法创建坐标转换")
		}
		defer C.OCTDestroyCoordinateTransformation(transform)
	}

	srcDefn := C.OGR_L_GetLayerDefn(src.layer)
	fieldMapping, err := createFieldMapping(srcDefn, targetDefn)
	if err != nil {
		return 0, fmt.Errorf("字段映射失败: %v", err)
	}

	C.OGR_L_ResetReading(src.layer)
	inserted := 0

	for {
		sf := C.OGR_L_GetNextFeature(src.layer)
		if sf == nil {
			break
		}

		tf := C.OGR_F_Create(targetDefn)
		if tf == nil {
			C.OGR_F_Destroy(sf)
			continue
		}

		geom := C.OGR_F_GetGeometryRef(sf)
		if geom != nil {
			cloned := C.OGR_G_Clone(geom)
			if cloned != nil {
				if options != nil && options.SkipInvalidGeometry && C.OGR_G_IsValid(cloned) == 0 {
					C.OGR_G_DestroyGeometry(cloned)
					C.OGR_F_Destroy(tf)
					C.OGR_F_Destroy(sf)
					continue
				}
				if needTransform && transform != nil {
					if C.OGR_G_Transform(cloned, transform) != C.OGRERR_NONE {
						C.OGR_G_DestroyGeometry(cloned)
						C.OGR_F_Destroy(tf)
						C.OGR_F_Destroy(sf)
						continue
					}
				}
				C.OGR_F_SetGeometry(tf, cloned)
				C.OGR_G_DestroyGeometry(cloned)
			}
		}

		copyFeatureFields(sf, tf, fieldMapping)

		if C.OGR_L_CreateFeature(targetLayer, tf) == C.OGRERR_NONE {
			inserted++
		}

		C.OGR_F_Destroy(tf)
		C.OGR_F_Destroy(sf)
	}

	return inserted, nil
}

func buildWhereClauseInternal(keyField string, ids []int64) string {
	if len(ids) == 1 {
		return fmt.Sprintf("%s = %d", keyField, ids[0])
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	result := keyField + " IN ("
	for i, p := range parts {
		if i > 0 {
			result += ","
		}
		result += p
	}
	result += ")"
	return result
}
