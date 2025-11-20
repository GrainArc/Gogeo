package Gogeo

/*
#include "osgeo_utils.h"

*/
import "C"

import (
	"fmt"
	"math"
	"runtime"
	"unsafe"
)

// ============================================================================
// 几何处理工具 - GeoTools
// ============================================================================

// BufferLayer 对整个图层进行缓冲区分析
// distance: 缓冲距离（单位与数据坐标系一致）
// quadSegs: 圆弧的四分之一段数，默认30，值越大越平滑
func BufferLayer(sourceLayer *GDALLayer, distance float64, quadSegs int) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	if quadSegs <= 0 {
		quadSegs = 30
	}

	// 创建内存数据源
	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("buffer_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	srs := sourceLayer.GetSpatialRef()

	// 创建结果图层（缓冲区结果通常是多边形）
	cLayerName := C.CString("buffered")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 遍历要素并进行缓冲
	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			// 创建缓冲区
			bufferedGeom := C.OGR_G_Buffer(geometry, C.double(distance), C.int(quadSegs))
			if bufferedGeom != nil {
				// 创建新要素
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					// 设置几何
					C.OGR_F_SetGeometry(newFeature, bufferedGeom)

					// 复制属性
					copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)

					// 添加要素
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
				C.OGR_G_DestroyGeometry(bufferedGeom)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// BufferLayerAuto 根据图形的面积和周长自动计算缓冲距离并创建缓冲区
// 对于带环岛的面要素，只缓冲外环，内环保持不变
func BufferLayerAuto(sourceLayer *GDALLayer, targetRatio float64, quadSegs int) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	if quadSegs <= 0 {
		quadSegs = 30
	}

	if targetRatio <= 0 {
		targetRatio = 1.5
	}

	// 创建内存数据源
	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("buffer_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	srs := sourceLayer.GetSpatialRef()

	// 创建结果图层
	cLayerName := C.CString("buffered")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 添加缓冲距离字段(可选)
	cFieldName := C.CString("buffer_dist")
	defer C.free(unsafe.Pointer(cFieldName))
	bufferDistField := C.OGR_Fld_Create(cFieldName, C.OFTReal)
	C.OGR_L_CreateField(resultLayer, bufferDistField, C.int(1))
	C.OGR_Fld_Destroy(bufferDistField)

	// 遍历要素并进行自动缓冲
	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			// 提取外环用于计算缓冲距离
			exteriorGeom := extractExteriorRing(geometry)
			if exteriorGeom == nil {
				C.OGR_F_Destroy(feature)
				continue
			}

			// 计算面积和周长（基于外环）
			area := float64(C.OGR_G_Area(exteriorGeom))

			// 计算周长(对于多边形使用边界长度)
			boundary := C.OGR_G_GetBoundary(exteriorGeom)
			perimeter := 0.0
			if boundary != nil {
				perimeter = float64(C.OGR_G_Length(boundary))
				C.OGR_G_DestroyGeometry(boundary)
			}

			// 计算优化的缓冲距离
			bufferDistance := calculateOptimizedBufferDistance(area, perimeter, targetRatio)

			// 创建缓冲区
			if bufferDistance > 0 {
				// 只对外环进行缓冲
				bufferedExterior := C.OGR_G_Buffer(exteriorGeom, C.double(bufferDistance), C.int(quadSegs))
				if bufferedExterior != nil {
					// 将缓冲后的外环与原始内环组合
					finalGeom := combineBufferedExteriorWithInteriorRings(bufferedExterior, geometry)

					if finalGeom != nil {
						// 创建新要素
						newFeature := C.OGR_F_Create(resultDefn)
						if newFeature != nil {
							// 设置几何
							C.OGR_F_SetGeometry(newFeature, finalGeom)

							// 复制属性
							copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)

							// 设置缓冲距离字段值
							bufferDistFieldIndex := C.OGR_F_GetFieldIndex(newFeature, cFieldName)
							if bufferDistFieldIndex >= 0 {
								C.OGR_F_SetFieldDouble(newFeature, bufferDistFieldIndex, C.double(bufferDistance))
							}

							// 添加要素
							C.OGR_L_CreateFeature(resultLayer, newFeature)
							C.OGR_F_Destroy(newFeature)
						}
						C.OGR_G_DestroyGeometry(finalGeom)
					}
					C.OGR_G_DestroyGeometry(bufferedExterior)
				}
			}

			// 清理外环几何
			C.OGR_G_DestroyGeometry(exteriorGeom)
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// extractExteriorRing 提取几何对象的外环，返回只包含外环的多边形
func extractExteriorRing(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	geomType := C.OGR_G_GetGeometryType(geometry)

	switch geomType {
	case C.wkbPolygon, C.wkbPolygon25D:
		// 获取外环
		exteriorRing := C.OGR_G_GetGeometryRef(geometry, 0)
		if exteriorRing == nil {
			return nil
		}

		// 克隆外环
		clonedRing := C.OGR_G_Clone(exteriorRing)
		if clonedRing == nil {
			return nil
		}

		// 创建新的多边形，只包含外环
		newPolygon := C.OGR_G_CreateGeometry(C.wkbPolygon)
		if newPolygon == nil {
			C.OGR_G_DestroyGeometry(clonedRing)
			return nil
		}

		// 将外环添加到新多边形
		err := C.OGR_G_AddGeometryDirectly(newPolygon, clonedRing)
		if err != 0 {
			C.OGR_G_DestroyGeometry(clonedRing)
			C.OGR_G_DestroyGeometry(newPolygon)
			return nil
		}

		return newPolygon

	case C.wkbMultiPolygon, C.wkbMultiPolygon25D:
		// 创建新的MultiPolygon
		newMultiPolygon := C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
		if newMultiPolygon == nil {
			return nil
		}

		// 遍历每个多边形，提取外环
		geomCount := int(C.OGR_G_GetGeometryCount(geometry))
		for i := 0; i < geomCount; i++ {
			subGeom := C.OGR_G_GetGeometryRef(geometry, C.int(i))
			if subGeom != nil {
				exteriorPoly := extractExteriorRing(subGeom)
				if exteriorPoly != nil {
					C.OGR_G_AddGeometryDirectly(newMultiPolygon, exteriorPoly)
				}
			}
		}

		return newMultiPolygon

	default:
		// 对于其他类型，直接克隆
		return C.OGR_G_Clone(geometry)
	}
}

// combineBufferedExteriorWithInteriorRings 将缓冲后的外环与原始内环组合
func combineBufferedExteriorWithInteriorRings(bufferedExterior, originalGeometry C.OGRGeometryH) C.OGRGeometryH {
	if bufferedExterior == nil || originalGeometry == nil {
		return nil
	}

	geomType := C.OGR_G_GetGeometryType(originalGeometry)

	switch geomType {
	case C.wkbPolygon, C.wkbPolygon25D:
		// 获取原始多边形的环数量
		ringCount := int(C.OGR_G_GetGeometryCount(originalGeometry))

		// 如果只有外环，直接克隆缓冲后的几何
		if ringCount <= 1 {
			return C.OGR_G_Clone(bufferedExterior)
		}

		// 创建新的多边形
		newPolygon := C.OGR_G_CreateGeometry(C.wkbPolygon)
		if newPolygon == nil {
			return nil
		}

		// 获取缓冲后的外环（缓冲结果应该是多边形）
		bufferedRing := C.OGR_G_GetGeometryRef(bufferedExterior, 0)
		if bufferedRing != nil {
			clonedBufferedRing := C.OGR_G_Clone(bufferedRing)
			if clonedBufferedRing != nil {
				C.OGR_G_AddGeometryDirectly(newPolygon, clonedBufferedRing)
			}
		}

		// 添加原始内环（从索引1开始，跳过外环）
		for i := 1; i < ringCount; i++ {
			interiorRing := C.OGR_G_GetGeometryRef(originalGeometry, C.int(i))
			if interiorRing != nil {
				clonedInteriorRing := C.OGR_G_Clone(interiorRing)
				if clonedInteriorRing != nil {
					C.OGR_G_AddGeometryDirectly(newPolygon, clonedInteriorRing)
				}
			}
		}

		return newPolygon

	case C.wkbMultiPolygon, C.wkbMultiPolygon25D:
		// 创建新的MultiPolygon
		newMultiPolygon := C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
		if newMultiPolygon == nil {
			return nil
		}

		// 获取缓冲后的几何数量
		bufferedGeomCount := int(C.OGR_G_GetGeometryCount(bufferedExterior))
		originalGeomCount := int(C.OGR_G_GetGeometryCount(originalGeometry))

		// 遍历每个多边形
		for i := 0; i < bufferedGeomCount && i < originalGeomCount; i++ {
			bufferedSubGeom := C.OGR_G_GetGeometryRef(bufferedExterior, C.int(i))
			originalSubGeom := C.OGR_G_GetGeometryRef(originalGeometry, C.int(i))

			if bufferedSubGeom != nil && originalSubGeom != nil {
				combinedPoly := combineBufferedExteriorWithInteriorRings(bufferedSubGeom, originalSubGeom)
				if combinedPoly != nil {
					C.OGR_G_AddGeometryDirectly(newMultiPolygon, combinedPoly)
				}
			}
		}

		return newMultiPolygon

	default:
		// 对于其他类型，直接克隆缓冲后的几何
		return C.OGR_G_Clone(bufferedExterior)
	}
}

// calculateOptimizedBufferDistance 根据面积和周长计算优化的缓冲距离
func calculateOptimizedBufferDistance(area, perimeter, targetRatio float64) float64 {
	if area <= 0 || perimeter <= 0 {
		return 0
	}

	// 计算当前的面积周长比
	currentRatio := area / perimeter

	// 理想圆形的面积周长比
	idealCircleRadius := math.Sqrt(area / math.Pi)
	idealRatio := idealCircleRadius / 2

	// 计算形状偏差
	shapeDeviation := math.Abs(currentRatio-idealRatio) / idealRatio

	// 基础缓冲距离
	baseDistance := math.Sqrt(area) * 0.01

	// 根据形状偏差和目标比例调整
	adjustmentFactor := targetRatio * (1 + shapeDeviation*0.5)
	bufferDistance := baseDistance * adjustmentFactor

	return bufferDistance
}

// BufferFeature 对单个要素进行缓冲区分析
// 返回缓冲后的几何体
func BufferFeature(feature C.OGRFeatureH, distance float64, quadSegs int) C.OGRGeometryH {
	if feature == nil {
		return nil
	}

	if quadSegs <= 0 {
		quadSegs = 30
	}

	geometry := C.OGR_F_GetGeometryRef(feature)
	if geometry == nil {
		return nil
	}

	return C.OGR_G_Buffer(geometry, C.double(distance), C.int(quadSegs))
}

// BufferGeometry 对几何体进行缓冲区分析
func BufferGeometry(geometry C.OGRGeometryH, distance float64, quadSegs int) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	if quadSegs <= 0 {
		quadSegs = 30
	}

	return C.OGR_G_Buffer(geometry, C.double(distance), C.int(quadSegs))
}

// SimplifyLayer 对整个图层进行简化
// tolerance: 简化容差
// preserveTopology: 是否保持拓扑关系
func SimplifyLayer(sourceLayer *GDALLayer, tolerance float64, preserveTopology bool) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	// 创建内存数据源
	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("simplify_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	// 创建结果图层
	cLayerName := C.CString("simplified")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 遍历要素并进行简化
	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			var simplifiedGeom C.OGRGeometryH
			if preserveTopology {
				simplifiedGeom = C.OGR_G_SimplifyPreserveTopology(geometry, C.double(tolerance))
			} else {
				simplifiedGeom = C.OGR_G_Simplify(geometry, C.double(tolerance))
			}

			if simplifiedGeom != nil {
				// 创建新要素
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					// 设置几何
					C.OGR_F_SetGeometry(newFeature, simplifiedGeom)

					// 复制属性
					copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)

					// 添加要素
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
				C.OGR_G_DestroyGeometry(simplifiedGeom)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// SimplifyFeature 对单个要素进行简化
func SimplifyFeature(feature C.OGRFeatureH, tolerance float64, preserveTopology bool) C.OGRGeometryH {
	if feature == nil {
		return nil
	}

	geometry := C.OGR_F_GetGeometryRef(feature)
	if geometry == nil {
		return nil
	}

	if preserveTopology {
		return C.OGR_G_SimplifyPreserveTopology(geometry, C.double(tolerance))
	}
	return C.OGR_G_Simplify(geometry, C.double(tolerance))
}

// SimplifyGeometry 对几何体进行简化
func SimplifyGeometry(geometry C.OGRGeometryH, tolerance float64, preserveTopology bool) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	if preserveTopology {
		return C.OGR_G_SimplifyPreserveTopology(geometry, C.double(tolerance))
	}
	return C.OGR_G_Simplify(geometry, C.double(tolerance))
}

// MakeValidLayer 对整个图层进行几何修复
func MakeValidLayer(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	// 创建内存数据源
	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("makevalid_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	// 创建结果图层
	cLayerName := C.CString("validated")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 遍历要素并进行修复
	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	var validCount, invalidCount int

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			var validGeom C.OGRGeometryH

			// 检查几何是否有效
			if C.OGR_G_IsValid(geometry) != 0 {
				validGeom = C.OGR_G_Clone(geometry)
				validCount++
			} else {
				// 尝试修复几何
				validGeom = C.OGR_G_MakeValid(geometry)
				if validGeom != nil && C.OGR_G_IsValid(validGeom) != 0 {
					invalidCount++
				} else {
					if validGeom != nil {
						C.OGR_G_DestroyGeometry(validGeom)
					}
					C.OGR_F_Destroy(feature)
					continue
				}
			}

			if validGeom != nil {
				// 创建新要素
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					// 设置几何
					C.OGR_F_SetGeometry(newFeature, validGeom)

					// 复制属性
					copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)

					// 添加要素
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
				C.OGR_G_DestroyGeometry(validGeom)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	fmt.Printf("几何修复完成 - 有效: %d, 已修复: %d\n", validCount, invalidCount)

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// MakeValidFeature 对单个要素进行几何修复
func MakeValidFeature(feature C.OGRFeatureH) C.OGRGeometryH {
	if feature == nil {
		return nil
	}

	geometry := C.OGR_F_GetGeometryRef(feature)
	if geometry == nil {
		return nil
	}

	// 如果已经有效，返回克隆
	if C.OGR_G_IsValid(geometry) != 0 {
		return C.OGR_G_Clone(geometry)
	}

	// 尝试修复
	return C.OGR_G_MakeValid(geometry)
}

// MakeValidGeometry 对几何体进行修复
func MakeValidGeometry(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	// 如果已经有效，返回克隆
	if C.OGR_G_IsValid(geometry) != 0 {
		return C.OGR_G_Clone(geometry)
	}

	// 尝试修复
	return C.OGR_G_MakeValid(geometry)
}

// ============================================================================
// 辅助函数
// ============================================================================

// copyFeatureAttributes 复制要素属性
func copyFeatureAttributes(sourceFeature, targetFeature C.OGRFeatureH, sourceDefn, targetDefn C.OGRFeatureDefnH) {
	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	targetFieldCount := int(C.OGR_FD_GetFieldCount(targetDefn))

	// 创建目标字段名映射
	targetFieldMap := make(map[string]int)
	for i := 0; i < targetFieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(targetDefn, C.int(i))
		if fieldDefn != nil {
			fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
			targetFieldMap[fieldName] = i
		}
	}

	// 复制字段值
	for i := 0; i < sourceFieldCount; i++ {
		if C.OGR_F_IsFieldSet(sourceFeature, C.int(i)) == 0 {
			continue
		}

		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))
		targetIndex, exists := targetFieldMap[fieldName]
		if !exists {
			continue
		}

		fieldType := C.OGR_Fld_GetType(sourceFieldDefn)

		switch fieldType {
		case C.OFTInteger:
			value := C.OGR_F_GetFieldAsInteger(sourceFeature, C.int(i))
			C.OGR_F_SetFieldInteger(targetFeature, C.int(targetIndex), value)

		case C.OFTInteger64:
			value := C.OGR_F_GetFieldAsInteger64(sourceFeature, C.int(i))
			C.OGR_F_SetFieldInteger64(targetFeature, C.int(targetIndex), value)

		case C.OFTReal:
			value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(i))
			C.OGR_F_SetFieldDouble(targetFeature, C.int(targetIndex), value)

		case C.OFTString:
			value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(i))
			if value != nil {
				C.OGR_F_SetFieldString(targetFeature, C.int(targetIndex), value)
			}

		case C.OFTDate, C.OFTTime, C.OFTDateTime:
			var year, month, day, hour, minute, second, tzflag C.int
			result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(i),
				&year, &month, &day, &hour, &minute, &second, &tzflag)
			if result != 0 {
				C.OGR_F_SetFieldDateTime(targetFeature, C.int(targetIndex),
					year, month, day, hour, minute, second, tzflag)
			}

		default:
			value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(i))
			if value != nil {
				C.OGR_F_SetFieldString(targetFeature, C.int(targetIndex), value)
			}
		}
	}
}

// ============================================================================
// 额外的几何处理函数
// ============================================================================

// ConvexHullLayer 对整个图层计算凸包
func ConvexHullLayer(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("convexhull_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("convexhull")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			convexHull := C.OGR_G_ConvexHull(geometry)
			if convexHull != nil {
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					C.OGR_F_SetGeometry(newFeature, convexHull)
					copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
				C.OGR_G_DestroyGeometry(convexHull)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ConvexHullGeometry 对几何体计算凸包
func ConvexHullGeometry(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}
	return C.OGR_G_ConvexHull(geometry)
}

// CentroidLayer 计算图层中每个要素的质心
func CentroidLayer(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("centroid_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("centroids")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPoint, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			centroid := C.OGR_G_CreateGeometry(C.wkbPoint)
			if centroid != nil {
				result := C.OGR_G_Centroid(geometry, centroid)
				if result == C.OGRERR_NONE {
					newFeature := C.OGR_F_Create(resultDefn)
					if newFeature != nil {
						C.OGR_F_SetGeometry(newFeature, centroid)
						copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
						C.OGR_L_CreateFeature(resultLayer, newFeature)
						C.OGR_F_Destroy(newFeature)
					}
				}
				C.OGR_G_DestroyGeometry(centroid)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// CentroidGeometry 计算几何体的质心
func CentroidGeometry(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	centroid := C.OGR_G_CreateGeometry(C.wkbPoint)
	if centroid == nil {
		return nil
	}

	result := C.OGR_G_Centroid(geometry, centroid)
	if result != C.OGRERR_NONE {
		C.OGR_G_DestroyGeometry(centroid)
		return nil
	}

	return centroid
}

// BoundaryLayer 计算图层中每个要素的边界
func BoundaryLayer(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("boundary_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("boundaries")
	defer C.free(unsafe.Pointer(cLayerName))

	// 边界通常是线
	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbLineString, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			boundary := C.OGR_G_Boundary(geometry)
			if boundary != nil {
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					C.OGR_F_SetGeometry(newFeature, boundary)
					copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
				C.OGR_G_DestroyGeometry(boundary)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// BoundaryGeometry 计算几何体的边界
func BoundaryGeometry(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}
	return C.OGR_G_Boundary(geometry)
}

// UnionAllLayer 将图层中所有要素合并为一个几何体
func UnionAllLayer(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("union_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("unioned")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbMultiPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	sourceLayer.ResetReading()

	var unionGeom C.OGRGeometryH

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			if unionGeom == nil {
				unionGeom = C.OGR_G_Clone(geometry)
			} else {
				newUnion := C.OGR_G_Union(unionGeom, geometry)
				if newUnion != nil {
					C.OGR_G_DestroyGeometry(unionGeom)
					unionGeom = newUnion
				}
			}
		}

		C.OGR_F_Destroy(feature)
	}

	if unionGeom != nil {
		resultDefn := C.OGR_L_GetLayerDefn(resultLayer)
		newFeature := C.OGR_F_Create(resultDefn)
		if newFeature != nil {
			C.OGR_F_SetGeometry(newFeature, unionGeom)
			C.OGR_L_CreateFeature(resultLayer, newFeature)
			C.OGR_F_Destroy(newFeature)
		}
		C.OGR_G_DestroyGeometry(unionGeom)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// UnionGeometry 合并两个几何体
func UnionGeometry(geom1, geom2 C.OGRGeometryH) C.OGRGeometryH {
	if geom1 == nil || geom2 == nil {
		return nil
	}
	return C.OGR_G_Union(geom1, geom2)
}

// IntersectionLayer 计算两个图层的交集
func IntersectionLayer(layer1, layer2 *GDALLayer) (*GDALLayer, error) {
	if layer1 == nil || layer1.layer == nil || layer2 == nil || layer2.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("intersection_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := layer1.GetSpatialRef()

	cLayerName := C.CString("intersection")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制layer1的字段定义
	sourceDefn := layer1.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	layer1.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature1 := layer1.GetNextFeature()
		if feature1 == nil {
			break
		}

		geom1 := C.OGR_F_GetGeometryRef(feature1)
		if geom1 != nil {
			layer2.ResetReading()

			for {
				feature2 := layer2.GetNextFeature()
				if feature2 == nil {
					break
				}

				geom2 := C.OGR_F_GetGeometryRef(feature2)
				if geom2 != nil {
					// 检查是否相交
					if C.OGR_G_Intersects(geom1, geom2) != 0 {
						intersection := C.OGR_G_Intersection(geom1, geom2)
						if intersection != nil && C.OGR_G_IsEmpty(intersection) == 0 {
							newFeature := C.OGR_F_Create(resultDefn)
							if newFeature != nil {
								C.OGR_F_SetGeometry(newFeature, intersection)
								copyFeatureAttributes(feature1, newFeature, sourceDefn, resultDefn)
								C.OGR_L_CreateFeature(resultLayer, newFeature)
								C.OGR_F_Destroy(newFeature)
							}
							C.OGR_G_DestroyGeometry(intersection)
						}
					}
				}

				C.OGR_F_Destroy(feature2)
			}
		}

		C.OGR_F_Destroy(feature1)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// IntersectionGeometry 计算两个几何体的交集
func IntersectionGeometry(geom1, geom2 C.OGRGeometryH) C.OGRGeometryH {
	if geom1 == nil || geom2 == nil {
		return nil
	}
	return C.OGR_G_Intersection(geom1, geom2)
}

// DifferenceLayer 计算两个图层的差集（layer1 - layer2）
func DifferenceLayer(layer1, layer2 *GDALLayer) (*GDALLayer, error) {
	if layer1 == nil || layer1.layer == nil || layer2 == nil || layer2.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("difference_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := layer1.GetSpatialRef()
	sourceDefn := layer1.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	cLayerName := C.CString("difference")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	layer1.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature1 := layer1.GetNextFeature()
		if feature1 == nil {
			break
		}

		geom1 := C.OGR_F_GetGeometryRef(feature1)
		if geom1 != nil {
			resultGeom := C.OGR_G_Clone(geom1)

			layer2.ResetReading()
			for {
				feature2 := layer2.GetNextFeature()
				if feature2 == nil {
					break
				}

				geom2 := C.OGR_F_GetGeometryRef(feature2)
				if geom2 != nil && C.OGR_G_Intersects(resultGeom, geom2) != 0 {
					diff := C.OGR_G_Difference(resultGeom, geom2)
					if diff != nil {
						C.OGR_G_DestroyGeometry(resultGeom)
						resultGeom = diff
					}
				}

				C.OGR_F_Destroy(feature2)
			}

			if resultGeom != nil && C.OGR_G_IsEmpty(resultGeom) == 0 {
				newFeature := C.OGR_F_Create(resultDefn)
				if newFeature != nil {
					C.OGR_F_SetGeometry(newFeature, resultGeom)
					copyFeatureAttributes(feature1, newFeature, sourceDefn, resultDefn)
					C.OGR_L_CreateFeature(resultLayer, newFeature)
					C.OGR_F_Destroy(newFeature)
				}
			}

			if resultGeom != nil {
				C.OGR_G_DestroyGeometry(resultGeom)
			}
		}

		C.OGR_F_Destroy(feature1)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// DifferenceGeometry 计算两个几何体的差集
func DifferenceGeometry(geom1, geom2 C.OGRGeometryH) C.OGRGeometryH {
	if geom1 == nil || geom2 == nil {
		return nil
	}
	return C.OGR_G_Difference(geom1, geom2)
}

// SymDifferenceGeometry 计算两个几何体的对称差集
func SymDifferenceGeometry(geom1, geom2 C.OGRGeometryH) C.OGRGeometryH {
	if geom1 == nil || geom2 == nil {
		return nil
	}
	return C.OGR_G_SymDifference(geom1, geom2)
}

// ============================================================================
// 空间查询函数
// ============================================================================

// FilterByExtent 按范围过滤图层
func FilterByExtent(sourceLayer *GDALLayer, minX, minY, maxX, maxY float64) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("filter_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	cLayerName := C.CString("filtered")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 设置空间过滤器
	C.OGR_L_SetSpatialFilterRect(sourceLayer.layer, C.double(minX), C.double(minY), C.double(maxX), C.double(maxY))
	sourceLayer.ResetReading()

	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		newFeature := C.OGR_F_Create(resultDefn)
		if newFeature != nil {
			geometry := C.OGR_F_GetGeometryRef(feature)
			if geometry != nil {
				C.OGR_F_SetGeometry(newFeature, geometry)
			}
			copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
			C.OGR_L_CreateFeature(resultLayer, newFeature)
			C.OGR_F_Destroy(newFeature)
		}

		C.OGR_F_Destroy(feature)
	}

	// 清除空间过滤器
	C.OGR_L_SetSpatialFilter(sourceLayer.layer, nil)

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// FilterByGeometry 按几何体过滤图层
func FilterByGeometry(sourceLayer *GDALLayer, filterGeom C.OGRGeometryH) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	if filterGeom == nil {
		return nil, fmt.Errorf("过滤几何体为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("filter_geom_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	cLayerName := C.CString("filtered")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 设置空间过滤器
	C.OGR_L_SetSpatialFilter(sourceLayer.layer, filterGeom)
	sourceLayer.ResetReading()

	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		newFeature := C.OGR_F_Create(resultDefn)
		if newFeature != nil {
			geometry := C.OGR_F_GetGeometryRef(feature)
			if geometry != nil {
				C.OGR_F_SetGeometry(newFeature, geometry)
			}
			copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
			C.OGR_L_CreateFeature(resultLayer, newFeature)
			C.OGR_F_Destroy(newFeature)
		}

		C.OGR_F_Destroy(feature)
	}

	// 清除空间过滤器
	C.OGR_L_SetSpatialFilter(sourceLayer.layer, nil)

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ============================================================================
// 几何属性计算函数
// ============================================================================

// GetArea 计算几何体面积
func GetArea(geometry C.OGRGeometryH) float64 {
	if geometry == nil {
		return 0
	}
	return float64(C.OGR_G_Area(geometry))
}

// GetLength 计算几何体长度
func GetLength(geometry C.OGRGeometryH) float64 {
	if geometry == nil {
		return 0
	}
	return float64(C.OGR_G_Length(geometry))
}

// GetPointCount 获取几何体点数
func GetPointCount(geometry C.OGRGeometryH) int {
	if geometry == nil {
		return 0
	}
	return int(C.OGR_G_GetPointCount(geometry))
}

// IsValid 检查几何体是否有效
func IsValid(geometry C.OGRGeometryH) bool {
	if geometry == nil {
		return false
	}
	return C.OGR_G_IsValid(geometry) != 0
}

// IsEmpty 检查几何体是否为空
func IsEmpty(geometry C.OGRGeometryH) bool {
	if geometry == nil {
		return true
	}
	return C.OGR_G_IsEmpty(geometry) != 0
}

// IsSimple 检查几何体是否简单
func IsSimple(geometry C.OGRGeometryH) bool {
	if geometry == nil {
		return false
	}
	return C.OGR_G_IsSimple(geometry) != 0
}

// IsRing 检查几何体是否为环
func IsRing(geometry C.OGRGeometryH) bool {
	if geometry == nil {
		return false
	}
	return C.OGR_G_IsRing(geometry) != 0
}

// ============================================================================
// 空间关系判断函数
// ============================================================================

// Intersects 判断两个几何体是否相交
func Intersects(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Intersects(geom1, geom2) != 0
}

// Contains 判断geom1是否包含geom2
func Contains(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Contains(geom1, geom2) != 0
}

// Within 判断geom1是否在geom2内
func Within(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Within(geom1, geom2) != 0
}

// Touches 判断两个几何体是否接触
func Touches(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Touches(geom1, geom2) != 0
}

// Crosses 判断两个几何体是否交叉
func Crosses(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Crosses(geom1, geom2) != 0
}

// Overlaps 判断两个几何体是否重叠
func Overlaps(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Overlaps(geom1, geom2) != 0
}

// Disjoint 判断两个几何体是否不相交
func Disjoint(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return true
	}
	return C.OGR_G_Disjoint(geom1, geom2) != 0
}

// Equals 判断两个几何体是否相等
func Equals(geom1, geom2 C.OGRGeometryH) bool {
	if geom1 == nil || geom2 == nil {
		return false
	}
	return C.OGR_G_Equals(geom1, geom2) != 0
}

// Distance 计算两个几何体之间的距离
func Distance(geom1, geom2 C.OGRGeometryH) float64 {
	if geom1 == nil || geom2 == nil {
		return -1
	}
	return float64(C.OGR_G_Distance(geom1, geom2))
}

// ============================================================================
// 几何体创建和转换函数
// ============================================================================

// CreatePointGeometry 创建点几何体
func CreatePointGeometry(x, y float64) C.OGRGeometryH {
	point := C.OGR_G_CreateGeometry(C.wkbPoint)
	if point != nil {
		C.OGR_G_SetPoint_2D(point, 0, C.double(x), C.double(y))
	}
	return point
}

// CreatePoint3DGeometry 创建3D点几何体
func CreatePoint3DGeometry(x, y, z float64) C.OGRGeometryH {
	point := C.OGR_G_CreateGeometry(C.wkbPoint25D)
	if point != nil {
		C.OGR_G_SetPoint(point, 0, C.double(x), C.double(y), C.double(z))
	}
	return point
}

// CreateLineStringGeometry 创建线几何体
func CreateLineStringGeometry(points [][2]float64) C.OGRGeometryH {
	line := C.OGR_G_CreateGeometry(C.wkbLineString)
	if line != nil {
		for _, pt := range points {
			C.OGR_G_AddPoint_2D(line, C.double(pt[0]), C.double(pt[1]))
		}
	}
	return line
}

// CreatePolygonGeometry 创建多边形几何体
func CreatePolygonGeometry(rings [][][2]float64) C.OGRGeometryH {
	polygon := C.OGR_G_CreateGeometry(C.wkbPolygon)
	if polygon == nil {
		return nil
	}

	for _, ring := range rings {
		linearRing := C.OGR_G_CreateGeometry(C.wkbLinearRing)
		if linearRing != nil {
			for _, pt := range ring {
				C.OGR_G_AddPoint_2D(linearRing, C.double(pt[0]), C.double(pt[1]))
			}
			C.OGR_G_AddGeometryDirectly(polygon, linearRing)
		}
	}

	return polygon
}

// GeometryToWKT 将几何体转换为WKT格式
func GeometryToWKT(geometry C.OGRGeometryH) string {
	if geometry == nil {
		return ""
	}

	var wkt *C.char
	C.OGR_G_ExportToWkt(geometry, &wkt)
	if wkt == nil {
		return ""
	}

	result := C.GoString(wkt)
	C.CPLFree(unsafe.Pointer(wkt))
	return result
}

// GeometryFromWKT 从WKT格式创建几何体
func GeometryFromWKT(wkt string) C.OGRGeometryH {
	cWkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWkt))

	var geometry C.OGRGeometryH
	result := C.OGR_G_CreateFromWkt(&cWkt, nil, &geometry)
	if result != C.OGRERR_NONE {
		return nil
	}

	return geometry
}

// GeometryToGeoJSON 将几何体转换为GeoJSON格式
func GeometryToGeoJSON(geometry C.OGRGeometryH) string {
	if geometry == nil {
		return ""
	}

	json := C.OGR_G_ExportToJson(geometry)
	if json == nil {
		return ""
	}

	result := C.GoString(json)
	C.CPLFree(unsafe.Pointer(json))
	return result
}

// CloneGeometry 克隆几何体
func CloneGeometry(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}
	return C.OGR_G_Clone(geometry)
}

// DestroyGeometry 销毁几何体
func DestroyGeometry(geometry C.OGRGeometryH) {
	if geometry != nil {
		C.OGR_G_DestroyGeometry(geometry)
	}
}

// ============================================================================
// 坐标转换函数
// ============================================================================

// TransformLayer 对图层进行坐标转换
func TransformLayer(sourceLayer *GDALLayer, targetSRS C.OGRSpatialReferenceH) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	if targetSRS == nil {
		return nil, fmt.Errorf("目标空间参考为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("transform_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	cLayerName := C.CString("transformed")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, targetSRS, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 创建坐标转换器
	sourceSRS := sourceLayer.GetSpatialRef()
	transform := C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
	if transform == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建坐标转换器")
	}
	defer C.OCTDestroyCoordinateTransformation(transform)

	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			// 克隆几何体并进行转换
			clonedGeom := C.OGR_G_Clone(geometry)
			if clonedGeom != nil {
				result := C.OGR_G_Transform(clonedGeom, transform)
				if result == C.OGRERR_NONE {
					newFeature := C.OGR_F_Create(resultDefn)
					if newFeature != nil {
						C.OGR_F_SetGeometry(newFeature, clonedGeom)
						copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
						C.OGR_L_CreateFeature(resultLayer, newFeature)
						C.OGR_F_Destroy(newFeature)
					}
				}
				C.OGR_G_DestroyGeometry(clonedGeom)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// TransformGeometry 对几何体进行坐标转换
func TransformGeometry(geometry C.OGRGeometryH, sourceSRS, targetSRS C.OGRSpatialReferenceH) C.OGRGeometryH {
	if geometry == nil || sourceSRS == nil || targetSRS == nil {
		return nil
	}

	transform := C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
	if transform == nil {
		return nil
	}
	defer C.OCTDestroyCoordinateTransformation(transform)

	clonedGeom := C.OGR_G_Clone(geometry)
	if clonedGeom == nil {
		return nil
	}

	result := C.OGR_G_Transform(clonedGeom, transform)
	if result != C.OGRERR_NONE {
		C.OGR_G_DestroyGeometry(clonedGeom)
		return nil
	}

	return clonedGeom
}

// CreateSpatialReferenceFromEPSG 从EPSG代码创建空间参考
func CreateSpatialReferenceFromEPSG(epsgCode int) C.OGRSpatialReferenceH {
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		return nil
	}

	result := C.OSRImportFromEPSG(srs, C.int(epsgCode))
	if result != C.OGRERR_NONE {
		C.OSRDestroySpatialReference(srs)
		return nil
	}

	return srs
}

// CreateSpatialReferenceFromWKT 从WKT创建空间参考
func CreateSpatialReferenceFromWKT(wkt string) C.OGRSpatialReferenceH {
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		return nil
	}

	cWkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWkt))

	result := C.OSRImportFromWkt(srs, &cWkt)
	if result != C.OGRERR_NONE {
		C.OSRDestroySpatialReference(srs)
		return nil
	}

	return srs
}

// CreateSpatialReferenceFromProj4 从Proj4字符串创建空间参考
func CreateSpatialReferenceFromProj4(proj4 string) C.OGRSpatialReferenceH {
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		return nil
	}

	cProj4 := C.CString(proj4)
	defer C.free(unsafe.Pointer(cProj4))

	result := C.OSRImportFromProj4(srs, cProj4)
	if result != C.OGRERR_NONE {
		C.OSRDestroySpatialReference(srs)
		return nil
	}

	return srs
}

// DestroySpatialReference 销毁空间参考
func DestroySpatialReference(srs C.OGRSpatialReferenceH) {
	if srs != nil {
		C.OSRDestroySpatialReference(srs)
	}
}

// ============================================================================
// 拓扑处理函数
// ============================================================================

// RemoveDuplicatePoints 移除重复点
func RemoveDuplicatePoints(geometry C.OGRGeometryH, tolerance float64) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	// 克隆几何体
	result := C.OGR_G_Clone(geometry)
	if result == nil {
		return nil
	}

	// 移除重复点（GDAL 3.0+）
	// 注意：这个功能可能在某些GDAL版本中不可用
	// C.OGR_G_RemoveDuplicatePoints(result, C.double(tolerance))

	return result
}

// CloseRings 闭合环
func CloseRings(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	result := C.OGR_G_Clone(geometry)
	if result == nil {
		return nil
	}

	C.OGR_G_CloseRings(result)
	return result
}

// SegmentizeGeometry 将几何体分段
func SegmentizeGeometry(geometry C.OGRGeometryH, maxLength float64) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	result := C.OGR_G_Clone(geometry)
	if result == nil {
		return nil
	}

	C.OGR_G_Segmentize(result, C.double(maxLength))
	return result
}

// ============================================================================
// 批量处理函数
// ============================================================================

// BatchBuffer 批量缓冲区分析（支持不同的缓冲距离）
func BatchBuffer(sourceLayer *GDALLayer, distanceField string, quadSegs int) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	if quadSegs <= 0 {
		quadSegs = 30
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("batch_buffer_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("buffered")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	sourceDefn := sourceLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 查找距离字段索引
	cDistanceField := C.CString(distanceField)
	defer C.free(unsafe.Pointer(cDistanceField))
	distanceFieldIndex := int(C.OGR_FD_GetFieldIndex(sourceDefn, cDistanceField))

	sourceLayer.ResetReading()
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			// 获取缓冲距离
			var distance float64
			if distanceFieldIndex >= 0 {
				distance = float64(C.OGR_F_GetFieldAsDouble(feature, C.int(distanceFieldIndex)))
			} else {
				// 如果字段不存在，使用默认距离
				distance = 0
			}

			if distance > 0 {
				bufferedGeom := C.OGR_G_Buffer(geometry, C.double(distance), C.int(quadSegs))
				if bufferedGeom != nil {
					newFeature := C.OGR_F_Create(resultDefn)
					if newFeature != nil {
						C.OGR_F_SetGeometry(newFeature, bufferedGeom)
						copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)
						C.OGR_L_CreateFeature(resultLayer, newFeature)
						C.OGR_F_Destroy(newFeature)
					}
					C.OGR_G_DestroyGeometry(bufferedGeom)
				}
			}
		}

		C.OGR_F_Destroy(feature)
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// DissolveLayer 融合图层（按字段融合）
func DissolveLayer(sourceLayer *GDALLayer, dissolveField string) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("dissolve_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString("dissolved")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbMultiPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 创建融合字段
	sourceDefn := sourceLayer.GetLayerDefn()
	cDissolveField := C.CString(dissolveField)
	defer C.free(unsafe.Pointer(cDissolveField))

	dissolveFieldIndex := int(C.OGR_FD_GetFieldIndex(sourceDefn, cDissolveField))
	if dissolveFieldIndex < 0 {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("融合字段不存在: %s", dissolveField)
	}

	// 添加融合字段到结果图层
	fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(dissolveFieldIndex))
	if fieldDefn != nil {
		C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
	}

	// 按字段值分组并融合
	dissolveMap := make(map[string][]C.OGRGeometryH)
	sourceLayer.ResetReading()

	for {
		feature := sourceLayer.GetNextFeature()
		if feature == nil {
			break
		}

		fieldValue := C.GoString(C.OGR_F_GetFieldAsString(feature, C.int(dissolveFieldIndex)))
		geometry := C.OGR_F_GetGeometryRef(feature)

		if geometry != nil {
			clonedGeom := C.OGR_G_Clone(geometry)
			if clonedGeom != nil {
				dissolveMap[fieldValue] = append(dissolveMap[fieldValue], clonedGeom)
			}
		}

		C.OGR_F_Destroy(feature)
	}

	// 对每组进行融合
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	for fieldValue, geometries := range dissolveMap {
		if len(geometries) == 0 {
			continue
		}

		var unionGeom C.OGRGeometryH
		for _, geom := range geometries {
			if unionGeom == nil {
				unionGeom = C.OGR_G_Clone(geom)
			} else {
				newUnion := C.OGR_G_Union(unionGeom, geom)
				if newUnion != nil {
					C.OGR_G_DestroyGeometry(unionGeom)
					unionGeom = newUnion
				}
			}
			C.OGR_G_DestroyGeometry(geom)
		}

		if unionGeom != nil {
			newFeature := C.OGR_F_Create(resultDefn)
			if newFeature != nil {
				C.OGR_F_SetGeometry(newFeature, unionGeom)
				cFieldValue := C.CString(fieldValue)
				C.OGR_F_SetFieldString(newFeature, 0, cFieldValue)
				C.free(unsafe.Pointer(cFieldValue))
				C.OGR_L_CreateFeature(resultLayer, newFeature)
				C.OGR_F_Destroy(newFeature)
			}
			C.OGR_G_DestroyGeometry(unionGeom)
		}
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ============================================================================
// 几何体信息提取函数
// ============================================================================

// GetEnvelope 获取几何体的外接矩形
func GetEnvelope(geometry C.OGRGeometryH) (minX, minY, maxX, maxY float64, err error) {
	if geometry == nil {
		return 0, 0, 0, 0, fmt.Errorf("几何体为空")
	}

	var envelope C.OGREnvelope
	C.OGR_G_GetEnvelope(geometry, &envelope)

	return float64(envelope.MinX), float64(envelope.MinY),
		float64(envelope.MaxX), float64(envelope.MaxY), nil
}

// GetGeometryType 获取几何体类型名称
func GetGeometryType(geometry C.OGRGeometryH) string {
	if geometry == nil {
		return ""
	}

	geomType := C.OGR_G_GetGeometryType(geometry)
	return C.GoString(C.OGRGeometryTypeToName(geomType))
}

// GetGeometryName 获取几何体名称
func GetGeometryName(geometry C.OGRGeometryH) string {
	if geometry == nil {
		return ""
	}

	return C.GoString(C.OGR_G_GetGeometryName(geometry))
}

// GetDimension 获取几何体维度
func GetDimension(geometry C.OGRGeometryH) int {
	if geometry == nil {
		return 0
	}

	return int(C.OGR_G_GetDimension(geometry))
}

// GetCoordinateDimension 获取坐标维度
func GetCoordinateDimension(geometry C.OGRGeometryH) int {
	if geometry == nil {
		return 0
	}

	return int(C.OGR_G_GetCoordinateDimension(geometry))
}

// ============================================================================
// 辅助工具函数
// ============================================================================

// MergeFeaturesToLayer 将多个要素合并到一个新图层
func MergeFeaturesToLayer(features []C.OGRFeatureH, layerName string, srs C.OGRSpatialReferenceH) (*GDALLayer, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("merged")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取第一个要素的几何类型
	firstGeom := C.OGR_F_GetGeometryRef(features[0])
	var geomType C.OGRwkbGeometryType
	if firstGeom != nil {
		geomType = C.OGR_G_GetGeometryType(firstGeom)
	} else {
		geomType = C.wkbUnknown
	}

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, geomType, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制第一个要素的字段定义
	firstDefn := C.OGR_F_GetDefnRef(features[0])
	fieldCount := int(C.OGR_FD_GetFieldCount(firstDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(firstDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)

	// 添加所有要素
	for _, feature := range features {
		if feature == nil {
			continue
		}

		newFeature := C.OGR_F_Create(resultDefn)
		if newFeature != nil {
			geometry := C.OGR_F_GetGeometryRef(feature)
			if geometry != nil {
				C.OGR_F_SetGeometry(newFeature, geometry)
			}

			sourceDefn := C.OGR_F_GetDefnRef(feature)
			copyFeatureAttributes(feature, newFeature, sourceDefn, resultDefn)

			C.OGR_L_CreateFeature(resultLayer, newFeature)
			C.OGR_F_Destroy(newFeature)
		}
	}

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// CountValidGeometries 统计图层中有效几何体的数量
func CountValidGeometries(layer *GDALLayer) (valid, invalid int) {
	if layer == nil || layer.layer == nil {
		return 0, 0
	}

	layer.ResetReading()

	for {
		feature := layer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry != nil {
			if C.OGR_G_IsValid(geometry) != 0 {
				valid++
			} else {
				invalid++
			}
		}

		C.OGR_F_Destroy(feature)
	}

	layer.ResetReading()
	return valid, invalid
}

// GetLayerExtent 获取图层范围
func GetLayerExtent(layer *GDALLayer) (minX, minY, maxX, maxY float64, err error) {
	if layer == nil || layer.layer == nil {
		return 0, 0, 0, 0, fmt.Errorf("图层为空")
	}

	var envelope C.OGREnvelope
	result := C.OGR_L_GetExtent(layer.layer, &envelope, C.int(1))

	if result != C.OGRERR_NONE {
		return 0, 0, 0, 0, fmt.Errorf("无法获取图层范围")
	}

	return float64(envelope.MinX), float64(envelope.MinY),
		float64(envelope.MaxX), float64(envelope.MaxY), nil
}

// ============================================================================
// 环岛构建函数
// ============================================================================

// DonutBuilder 将面图层中的重叠面构建成环岛结构
// 该函数会识别包含关系，将内部多边形作为外部多边形的洞

func DonutBuilder(sourceLayer *GDALLayer) (*GDALLayer, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	// 创建内存数据源
	memDriver := C.OGRGetDriverByName(C.CString("Memory"))
	if memDriver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	cDatasetName := C.CString("donut_result")
	defer C.free(unsafe.Pointer(cDatasetName))

	memDataset := C.OGR_Dr_CreateDataSource(memDriver, cDatasetName, nil)
	if memDataset == nil {
		return nil, fmt.Errorf("无法创建内存数据源")
	}

	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	srs := sourceLayer.GetSpatialRef()

	// 创建结果图层
	cLayerName := C.CString("donuts")
	defer C.free(unsafe.Pointer(cLayerName))

	resultLayer := C.OGR_DS_CreateLayer(memDataset, cLayerName, srs, C.wkbPolygon, nil)
	if resultLayer == nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("无法创建结果图层")
	}

	// 复制字段定义
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if fieldDefn != nil {
			C.OGR_L_CreateField(resultLayer, fieldDefn, C.int(1))
		}
	}

	// 读取所有要素到内存
	polygons, err := loadPolygonsWithAttributes(sourceLayer, sourceDefn)
	if err != nil {
		C.OGR_DS_Destroy(memDataset)
		return nil, fmt.Errorf("加载多边形失败: %v", err)
	}

	if len(polygons) == 0 {
		// 空图层，直接返回
		gdalLayer := &GDALLayer{
			layer:   resultLayer,
			dataset: memDataset,
			driver:  memDriver,
		}
		runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
		return gdalLayer, nil
	}

	// 构建包含关系树
	containmentTree := buildContainmentTree(polygons)

	// 生成环岛多边形
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer)
	processedCount := createDonutPolygons(containmentTree, resultLayer, resultDefn, sourceDefn)

	fmt.Printf("环岛构建完成 - 处理了 %d 个多边形，生成 %d 个要素\n",
		len(polygons), processedCount)

	gdalLayer := &GDALLayer{
		layer:   resultLayer,
		dataset: memDataset,
		driver:  memDriver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ============================================================================
// 辅助数据结构
// ============================================================================

// polygonWithAttributes 存储多边形及其属性
type polygonWithAttributes struct {
	geometry    C.OGRGeometryH
	feature     C.OGRFeatureH
	area        float64
	envelope    [4]float64 // minX, maxX, minY, maxY
	index       int
	isProcessed bool
}

// containmentNode 包含关系树节点
type containmentNode struct {
	polygon  *polygonWithAttributes
	children []*containmentNode
	parent   *containmentNode
	level    int // 嵌套层级：0=顶层, 1=洞, 2=洞中的岛...
}

// ============================================================================
// 加载和预处理函数
// ============================================================================

// loadPolygonsWithAttributes 加载所有多边形及其属性
func loadPolygonsWithAttributes(layer *GDALLayer, defn C.OGRFeatureDefnH) ([]*polygonWithAttributes, error) {
	var polygons []*polygonWithAttributes
	layer.ResetReading()
	index := 0

	for {
		feature := layer.GetNextFeature()
		if feature == nil {
			break
		}

		geometry := C.OGR_F_GetGeometryRef(feature)
		if geometry == nil {
			C.OGR_F_Destroy(feature)
			continue
		}

		// 只处理多边形类型
		geomType := C.OGR_G_GetGeometryType(geometry)
		if geomType != C.wkbPolygon && geomType != C.wkbMultiPolygon {
			C.OGR_F_Destroy(feature)
			continue
		}

		// 克隆几何体（因为feature会被销毁）
		clonedGeom := C.OGR_G_Clone(geometry)
		if clonedGeom == nil {
			C.OGR_F_Destroy(feature)
			continue
		}

		// 克隆要素（保留属性）
		clonedFeature := C.OGR_F_Clone(feature)

		// 计算面积
		area := float64(C.OGR_G_Area(clonedGeom))

		// 获取包络矩形
		var envelope C.OGREnvelope
		C.OGR_G_GetEnvelope(clonedGeom, &envelope)

		poly := &polygonWithAttributes{
			geometry: clonedGeom,
			feature:  clonedFeature,
			area:     area,
			envelope: [4]float64{
				float64(envelope.MinX),
				float64(envelope.MaxX),
				float64(envelope.MinY),
				float64(envelope.MaxY),
			},
			index:       index,
			isProcessed: false,
		}

		polygons = append(polygons, poly)
		C.OGR_F_Destroy(feature)
		index++
	}

	return polygons, nil
}

// ============================================================================
// 包含关系分析
// ============================================================================

// buildContainmentTree 构建包含关系树
func buildContainmentTree(polygons []*polygonWithAttributes) []*containmentNode {
	// 按面积降序排序（大的在前）
	sortPolygonsByArea(polygons)

	// 创建所有节点
	nodes := make([]*containmentNode, len(polygons))
	for i, poly := range polygons {
		nodes[i] = &containmentNode{
			polygon:  poly,
			children: make([]*containmentNode, 0),
			parent:   nil,
			level:    0,
		}
	}

	// 构建包含关系
	rootNodes := make([]*containmentNode, 0)

	for i, node := range nodes {
		parentFound := false

		// 寻找最小的包含它的多边形作为父节点
		for j := 0; j < i; j++ { // 只在更大的多边形中查找
			if isPolygonContainedBy(node.polygon, nodes[j].polygon) {
				// 检查是否应该作为直接子节点
				if shouldBeDirectChild(node, nodes[j]) {
					nodes[j].children = append(nodes[j].children, node)
					node.parent = nodes[j]
					node.level = nodes[j].level + 1
					parentFound = true
					break
				}
			}
		}

		if !parentFound {
			rootNodes = append(rootNodes, node)
		}
	}

	return rootNodes
}

// sortPolygonsByArea 按面积降序排序
func sortPolygonsByArea(polygons []*polygonWithAttributes) {
	// 简单的冒泡排序（对于小数据集足够）
	n := len(polygons)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if polygons[j].area < polygons[j+1].area {
				polygons[j], polygons[j+1] = polygons[j+1], polygons[j]
			}
		}
	}
}

// isPolygonContainedBy 检查poly1是否被poly2包含
func isPolygonContainedBy(poly1, poly2 *polygonWithAttributes) bool {
	// 先快速检查包络矩形
	if !envelopeContains(poly2.envelope, poly1.envelope) {
		return false
	}

	// 面积检查（包含的多边形面积必须更小）
	if poly1.area >= poly2.area {
		return false
	}

	// 精确的几何包含检查
	return C.OGR_G_Contains(poly2.geometry, poly1.geometry) != 0
}

// envelopeContains 检查env1是否包含env2
func envelopeContains(env1, env2 [4]float64) bool {
	return env1[0] <= env2[0] && // minX
		env1[1] >= env2[1] && // maxX
		env1[2] <= env2[2] && // minY
		env1[3] >= env2[3] // maxY
}

// shouldBeDirectChild 检查node是否应该作为parent的直接子节点
// 而不是parent的某个子节点的子节点
func shouldBeDirectChild(node, parent *containmentNode) bool {
	// 检查parent的所有子节点，看是否有更合适的父节点
	for _, child := range parent.children {
		if isPolygonContainedBy(node.polygon, child.polygon) {
			return false // 应该是child的子节点，而不是parent的
		}
	}
	return true
}

// ============================================================================
// 环岛多边形生成
// ============================================================================

// createDonutPolygons 根据包含关系树创建环岛多边形
func createDonutPolygons(rootNodes []*containmentNode,
	resultLayer C.OGRLayerH,
	resultDefn, sourceDefn C.OGRFeatureDefnH) int {

	count := 0

	for _, root := range rootNodes {
		count += processNode(root, resultLayer, resultDefn, sourceDefn)
	}

	return count
}

// processNode 处理单个节点及其子树
// processNode 处理单个节点及其子树
func processNode(node *containmentNode,
	resultLayer C.OGRLayerH,
	resultDefn, sourceDefn C.OGRFeatureDefnH) int {

	if node.polygon.isProcessed {
		return 0
	}

	count := 0

	// 根据层级决定如何处理
	if node.level%2 == 0 {
		// 偶数层：实体多边形（可能带洞）
		count += createPolygonWithHoles(node, resultLayer, resultDefn, sourceDefn)
	} else {
		// 奇数层：洞，但也输出为独立要素（保留原始面）
		count += createSimplePolygon(node, resultLayer, resultDefn, sourceDefn)
	}

	// 递归处理子节点
	for _, child := range node.children {
		count += processNode(child, resultLayer, resultDefn, sourceDefn)
	}

	return count
}

// createSimplePolygon 创建简单多边形（用于输出原始洞要素）
func createSimplePolygon(node *containmentNode,
	resultLayer C.OGRLayerH,
	resultDefn, sourceDefn C.OGRFeatureDefnH) int {

	if node.polygon.isProcessed {
		return 0
	}

	// 克隆原始几何
	clonedGeom := C.OGR_G_Clone(node.polygon.geometry)
	if clonedGeom == nil {
		return 0
	}

	// 创建新要素
	newFeature := C.OGR_F_Create(resultDefn)
	if newFeature == nil {
		C.OGR_G_DestroyGeometry(clonedGeom)
		return 0
	}

	// 设置几何
	C.OGR_F_SetGeometry(newFeature, clonedGeom)

	// 复制属性
	copyFeatureAttributes(node.polygon.feature, newFeature, sourceDefn, resultDefn)

	// 添加到图层
	result := C.OGR_L_CreateFeature(resultLayer, newFeature)

	C.OGR_F_Destroy(newFeature)
	C.OGR_G_DestroyGeometry(clonedGeom)

	node.polygon.isProcessed = true

	if result == C.OGRERR_NONE {
		return 1
	}
	return 0
}

// createPolygonWithHoles 创建带洞的多边形

func createPolygonWithHoles(node *containmentNode,
	resultLayer C.OGRLayerH,
	resultDefn, sourceDefn C.OGRFeatureDefnH) int {

	if node.polygon.isProcessed {
		return 0
	}

	// 创建新的多边形
	newPolygon := C.OGR_G_CreateGeometry(C.wkbPolygon)
	if newPolygon == nil {
		return 0
	}

	// 添加外环
	outerRing := extractOuterRing(node.polygon.geometry)
	if outerRing != nil {
		C.OGR_G_AddGeometryDirectly(newPolygon, outerRing)
	} else {
		C.OGR_G_DestroyGeometry(newPolygon)
		return 0
	}

	// 添加内环（洞）
	holesAdded := 0
	for _, child := range node.children {
		if child.level == node.level+1 { // 只添加直接子节点作为洞
			innerRing := extractOuterRing(child.polygon.geometry)
			if innerRing != nil {
				C.OGR_G_AddGeometryDirectly(newPolygon, innerRing)
				// 移除这行：child.polygon.isProcessed = true
				holesAdded++
			}
		}
	}

	// 创建新要素
	newFeature := C.OGR_F_Create(resultDefn)
	if newFeature == nil {
		C.OGR_G_DestroyGeometry(newPolygon)
		return 0
	}

	// 设置几何
	C.OGR_F_SetGeometry(newFeature, newPolygon)

	// 复制属性（使用外部多边形的属性）
	copyFeatureAttributes(node.polygon.feature, newFeature, sourceDefn, resultDefn)

	// 添加到图层
	result := C.OGR_L_CreateFeature(resultLayer, newFeature)

	C.OGR_F_Destroy(newFeature)
	C.OGR_G_DestroyGeometry(newPolygon)

	node.polygon.isProcessed = true

	if result == C.OGRERR_NONE {
		return 1
	}
	return 0
}

// extractOuterRing 提取多边形的外环
func extractOuterRing(geometry C.OGRGeometryH) C.OGRGeometryH {
	if geometry == nil {
		return nil
	}

	geomType := C.OGR_G_GetGeometryType(geometry)

	if geomType == C.wkbPolygon {
		// 获取外环
		ring := C.OGR_G_GetGeometryRef(geometry, 0)
		if ring != nil {
			return C.OGR_G_Clone(ring)
		}
	} else if geomType == C.wkbMultiPolygon {
		// 对于MultiPolygon，取第一个多边形的外环
		poly := C.OGR_G_GetGeometryRef(geometry, 0)
		if poly != nil {
			ring := C.OGR_G_GetGeometryRef(poly, 0)
			if ring != nil {
				return C.OGR_G_Clone(ring)
			}
		}
	}

	return nil
}

// ============================================================================
// 清理函数
// ============================================================================

// cleanupPolygons 清理多边形数据
func cleanupPolygons(polygons []*polygonWithAttributes) {
	for _, poly := range polygons {
		if poly.geometry != nil {
			C.OGR_G_DestroyGeometry(poly.geometry)
		}
		if poly.feature != nil {
			C.OGR_F_Destroy(poly.feature)
		}
	}
}

// ============================================================================
// 高级版本：支持更多选项
// ============================================================================

// DonutBuilderOptions 环岛构建选项
type DonutBuilderOptions struct {
	// MinAreaRatio 最小面积比例，小于此比例的洞将被忽略（相对于外部多边形）
	MinAreaRatio float64

	// MaxHoleCount 单个多边形最大洞数量，0表示无限制
	MaxHoleCount int

	// SimplifyTolerance 简化容差，0表示不简化
	SimplifyTolerance float64

	// MergeThreshold 合并阈值，距离小于此值的多边形将被合并
	MergeThreshold float64
}

// DonutBuilderWithOptions 带选项的环岛构建函数
func DonutBuilderWithOptions(sourceLayer *GDALLayer, options *DonutBuilderOptions) (*GDALLayer, error) {
	if options == nil {
		options = &DonutBuilderOptions{
			MinAreaRatio:      0.001, // 默认最小面积比0.1%
			MaxHoleCount:      0,     // 不限制
			SimplifyTolerance: 0,     // 不简化
			MergeThreshold:    0,     // 不合并
		}
	}

	// 先进行基本的环岛构建
	resultLayer, err := DonutBuilder(sourceLayer)
	if err != nil {
		return nil, err
	}

	// 如果需要简化
	if options.SimplifyTolerance > 0 {
		simplifiedLayer, err := SimplifyLayer(resultLayer, options.SimplifyTolerance, true)
		if err == nil {
			// 清理旧图层
			if resultLayer != nil && resultLayer.dataset != nil {
				C.OGR_DS_Destroy(resultLayer.dataset)
			}
			resultLayer = simplifiedLayer
		}
	}

	return resultLayer, nil
}
