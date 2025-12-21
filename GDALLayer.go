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
	"runtime"
	"strings"
	"unsafe"
)

type GDALLayer struct {
	layer   C.OGRLayerH
	dataset C.OGRDataSourceH
	driver  C.OGRSFDriverH
}

// GetFeatureCount 获取要素数量
func (gl *GDALLayer) GetFeatureCount() int {
	return int(C.OGR_L_GetFeatureCount(gl.layer, C.int(1))) // 1表示强制计算
}

// GetLayerDefn 获取图层定义
func (gl *GDALLayer) GetLayerDefn() C.OGRFeatureDefnH {
	return C.OGR_L_GetLayerDefn(gl.layer)
}

// GetFieldDefn 获取字段定义
func (l *GDALLayer) GetFieldDefn(index int) C.OGRFieldDefnH {
	if l.layer == nil {
		return nil
	}
	defn := C.OGR_L_GetLayerDefn(l.layer)
	return C.OGR_FD_GetFieldDefn(defn, C.int(index))
}

// CreateField 创建字段
func (l *GDALLayer) CreateField(fieldDefn C.OGRFieldDefnH) error {
	if l.layer == nil {
		return fmt.Errorf("图层为空")
	}
	result := C.OGR_L_CreateField(l.layer, fieldDefn, C.int(1))
	if result != C.OGRERR_NONE {
		return fmt.Errorf("创建字段失败")
	}
	return nil
}

// GDALFeature 要素结构

// GetFieldCount 获取字段数量
func (gl *GDALLayer) GetFieldCount() int {
	defn := gl.GetLayerDefn()
	return int(C.OGR_FD_GetFieldCount(defn))
}

// GetFieldName 获取字段名称
func (gl *GDALLayer) GetFieldName(index int) string {
	defn := gl.GetLayerDefn()
	fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(index))
	if fieldDefn == nil {
		return ""
	}
	return C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
}

// GetGeometryType 获取几何类型
func (gl *GDALLayer) GetGeometryType() string {
	defn := gl.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(defn)
	return C.GoString(C.OGRGeometryTypeToName(geomType))
}

// GetLayerName 获取图层名称
func (gl *GDALLayer) GetLayerName() string {
	if gl.layer == nil {
		return ""
	}
	layerName := C.OGR_L_GetName(gl.layer)
	if layerName == nil {
		return ""
	}
	return C.GoString(layerName)
}

// GetSpatialRef 获取空间参考系统
func (gl *GDALLayer) GetSpatialRef() C.OGRSpatialReferenceH {
	return C.OGR_L_GetSpatialRef(gl.layer)
}

// ResetReading 重置读取位置
func (gl *GDALLayer) ResetReading() {
	C.OGR_L_ResetReading(gl.layer)
}

// GetNextFeature 获取下一个要素
func (gl *GDALLayer) GetNextFeatureRow() C.OGRFeatureH {
	return C.OGR_L_GetNextFeature(gl.layer)
}

// PrintLayerInfo 打印图层信息（增强版）
func (gl *GDALLayer) PrintLayerInfo() {
	fmt.Printf("图层信息:\n")
	fmt.Printf("  图层名称: %s\n", gl.GetLayerName())
	fmt.Printf("  要素数量: %d\n", gl.GetFeatureCount())
	fmt.Printf("  几何类型: %s\n", gl.GetGeometryType())
	fmt.Printf("  字段数量: %d\n", gl.GetFieldCount())

	// 打印字段定义表
	fmt.Printf("\n字段定义表:\n")
	fmt.Printf("%-4s %-20s %-15s %-8s %-6s\n", "序号", "字段名", "字段类型", "宽度", "精度")
	fmt.Println(strings.Repeat("-", 65))

	fieldCount := gl.GetFieldCount()
	defn := gl.GetLayerDefn()

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(i))
		if fieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		fieldType := C.GoString(C.OGR_GetFieldTypeName(C.OGR_Fld_GetType(fieldDefn)))
		width := int(C.OGR_Fld_GetWidth(fieldDefn))
		precision := int(C.OGR_Fld_GetPrecision(fieldDefn))

		fmt.Printf("%-4d %-20s %-15s %-8d %-6d\n",
			i+1, fieldName, fieldType, width, precision)
	}

	// 打印空间参考系统信息
	fmt.Printf("\n空间参考系统:\n")
	srs := gl.GetSpatialRef()
	if srs != nil {
		var projStr *C.char
		C.OSRExportToProj4(srs, &projStr)
		if projStr != nil {
			fmt.Printf("  投影: %s\n", C.GoString(projStr))
			C.CPLFree(unsafe.Pointer(projStr))
		}

		// 获取地理坐标系名称
		var geogName *C.char
		C.OSRExportToPrettyWkt(srs, &geogName, C.int(0))
		if geogName != nil {
			// 只显示前100个字符的WKT信息，避免输出过长
			wktStr := C.GoString(geogName)
			if len(wktStr) > 100 {
				wktStr = wktStr[:100] + "..."
			}
			fmt.Printf("  坐标系: %s\n", wktStr)
			C.CPLFree(unsafe.Pointer(geogName))
		}
	} else {
		fmt.Printf("  投影: 未定义\n")
	}

	// 打印前10个要素的属性信息
	fmt.Printf("\n前10个要素的属性数据:\n")
	gl.printFirst10Features()
}

// printFirst10Features 打印前10个要素的属性数据
func (gl *GDALLayer) printFirst10Features() {
	gl.ResetReading()

	fieldCount := gl.GetFieldCount()
	if fieldCount == 0 {
		fmt.Println("  没有属性字段")
		return
	}

	// 打印表头
	fmt.Printf("%-6s", "FID")
	for i := 0; i < fieldCount; i++ {
		fieldName := gl.GetFieldName(i)
		// 限制字段名显示长度
		if len(fieldName) > 15 {
			fieldName = fieldName[:12] + "..."
		}
		fmt.Printf("%-16s", fieldName)
	}
	fmt.Printf("%-15s\n", "几何类型")

	// 打印分隔线
	totalWidth := 6 + fieldCount*16 + 15
	fmt.Println(strings.Repeat("-", totalWidth))

	// 遍历前10个要素
	for featureIndex := 0; featureIndex < 10; featureIndex++ {
		feature := gl.GetNextFeature().Feature
		if feature == nil {
			break
		}

		// 安全地处理要素，确保释放资源
		func() {
			defer C.OGR_F_Destroy(feature)

			// 打印FID
			fid := int(C.OGR_F_GetFID(feature))
			fmt.Printf("%-6d", fid)

			// 打印每个字段的值
			for i := 0; i < fieldCount; i++ {
				fieldValue := gl.getFieldValueAsString(feature, i)
				// 限制值的显示长度
				if len(fieldValue) > 15 {
					fieldValue = fieldValue[:12] + "..."
				}
				fmt.Printf("%-16s", fieldValue)
			}

			// 打印几何类型
			geometry := C.OGR_F_GetGeometryRef(feature)
			geomTypeName := "NULL"
			if geometry != nil {
				geomType := C.OGR_G_GetGeometryType(geometry)
				geomTypeName = C.GoString(C.OGRGeometryTypeToName(geomType))
			}
			fmt.Printf("%-15s\n", geomTypeName)
		}()
	}

	// 如果要素数量大于10，显示提示信息
	totalFeatures := gl.GetFeatureCount()
	if totalFeatures > 10 {
		fmt.Printf("\n... 还有 %d 个要素（仅显示前10个）\n", totalFeatures-10)
	}

	// 重置读取位置
	gl.ResetReading()
}

// getFieldValueAsString 获取字段值的字符串表示
func (gl *GDALLayer) getFieldValueAsString(feature C.OGRFeatureH, fieldIndex int) string {
	// 检查字段是否设置
	if C.OGR_F_IsFieldSet(feature, C.int(fieldIndex)) == 0 {
		return "<NULL>"
	}

	// 获取字段定义
	defn := gl.GetLayerDefn()
	fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(fieldIndex))
	if fieldDefn == nil {
		return "<ERROR>"
	}

	fieldType := C.OGR_Fld_GetType(fieldDefn)

	// 根据字段类型返回相应的字符串值
	switch fieldType {
	case C.OFTInteger:
		value := int(C.OGR_F_GetFieldAsInteger(feature, C.int(fieldIndex)))
		return fmt.Sprintf("%d", value)

	case C.OFTInteger64:
		value := int64(C.OGR_F_GetFieldAsInteger64(feature, C.int(fieldIndex)))
		return fmt.Sprintf("%d", value)

	case C.OFTReal:
		value := float64(C.OGR_F_GetFieldAsDouble(feature, C.int(fieldIndex)))
		// 检查特殊值
		if C.check_isnan(C.double(value)) != 0 {
			return "<NaN>"
		}
		if C.check_isinf(C.double(value)) != 0 {
			return "<Inf>"
		}
		// 格式化浮点数，最多显示6位小数
		return fmt.Sprintf("%.6g", value)

	case C.OFTString:
		strPtr := C.OGR_F_GetFieldAsString(feature, C.int(fieldIndex))
		if strPtr == nil {
			return "<NULL>"
		}
		return C.GoString(strPtr)

	case C.OFTDate:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(feature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			return fmt.Sprintf("%04d-%02d-%02d", int(year), int(month), int(day))
		}
		return "<NULL>"

	case C.OFTTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(feature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			return fmt.Sprintf("%02d:%02d:%02d", int(hour), int(minute), int(second))
		}
		return "<NULL>"

	case C.OFTDateTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(feature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
				int(year), int(month), int(day), int(hour), int(minute), int(second))
		}
		return "<NULL>"

	case C.OFTBinary:
		return "<BINARY>"

	case C.OFTIntegerList, C.OFTRealList, C.OFTStringList:
		return "<LIST>"

	default:
		// 对于未知类型，尝试作为字符串获取
		strPtr := C.OGR_F_GetFieldAsString(feature, C.int(fieldIndex))
		if strPtr != nil {
			return C.GoString(strPtr)
		}
		return "<UNKNOWN>"
	}
}

// PrintLayerSummary 打印图层摘要信息（简化版）
func (gl *GDALLayer) PrintLayerSummary() {
	fmt.Printf("图层摘要:\n")
	fmt.Printf("  名称: %s\n", gl.GetLayerName())
	fmt.Printf("  要素数: %d\n", gl.GetFeatureCount())
	fmt.Printf("  几何类型: %s\n", gl.GetGeometryType())
	fmt.Printf("  字段数: %d\n", gl.GetFieldCount())
}

// PrintFieldsInfo 仅打印字段信息
func (gl *GDALLayer) PrintFieldsInfo() {
	fmt.Printf("字段信息:\n")
	fieldCount := gl.GetFieldCount()
	defn := gl.GetLayerDefn()

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(i))
		if fieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		fieldType := C.GoString(C.OGR_GetFieldTypeName(C.OGR_Fld_GetType(fieldDefn)))
		width := int(C.OGR_Fld_GetWidth(fieldDefn))
		precision := int(C.OGR_Fld_GetPrecision(fieldDefn))

		fmt.Printf("  %d. %s (%s", i+1, fieldName, fieldType)
		if width > 0 {
			fmt.Printf(", 宽度:%d", width)
		}
		if precision > 0 {
			fmt.Printf(", 精度:%d", precision)
		}
		fmt.Printf(")\n")
	}
}

// IterateFeatures 遍历所有要素
func (gl *GDALLayer) IterateFeatures(callback func(feature C.OGRFeatureH)) {
	gl.ResetReading()

	for {
		feature := gl.GetNextFeature().Feature
		if feature == nil {
			break
		}

		callback(feature)

		// 释放要素
		C.OGR_F_Destroy(feature)
	}
}

// cleanup 清理资源
func (gl *GDALLayer) cleanup() {
	if gl.dataset != nil {
		C.OGR_DS_Destroy(gl.dataset)
		gl.dataset = nil
	}
}

// Close 手动关闭资源
func (gl *GDALLayer) Close() {
	gl.cleanup()
	runtime.SetFinalizer(gl, nil)
}

// CreateEmptyFeature 创建空要素
func (gl *GDALLayer) CreateEmptyFeature() *GDALFeature {
	if gl.layer == nil {
		return nil
	}
	defn := C.OGR_L_GetLayerDefn(gl.layer)
	handle := C.OGR_F_Create(defn)
	if handle == nil {
		return nil
	}
	return &GDALFeature{Feature: handle}
}

// CreateFeature 将要素添加到图层
func (gl *GDALLayer) CreateFeature(f *GDALFeature) error {
	if gl.layer == nil {
		return fmt.Errorf("图层为空")
	}
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	result := C.OGR_L_CreateFeature(gl.layer, f.Feature)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("创建要素失败，错误码: %d", result)
	}
	return nil
}

// CreateFeatureFromHandle 从C句柄创建要素到图层
func (gl *GDALLayer) CreateFeatureFromHandle(handle C.OGRFeatureH) error {
	if gl.layer == nil {
		return fmt.Errorf("图层为空")
	}
	if handle == nil {
		return fmt.Errorf("要素句柄为空")
	}
	result := C.OGR_L_CreateFeature(gl.layer, handle)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("创建要素失败，错误码: %d", result)
	}
	return nil
}

// ==================== 便捷的复制函数 ====================

// CopyAllFeatures 复制所有要素从源图层到目标图层
func CopyAllFeatures(srcLayer, dstLayer *GDALLayer) (int, error) {
	if srcLayer == nil || dstLayer == nil {
		return 0, fmt.Errorf("源图层或目标图层为空")
	}

	srcLayer.ResetReading()
	count := 0

	for {
		feature := srcLayer.GetNextFeature()
		if feature == nil {
			break
		}

		err := dstLayer.CreateFeature(feature)
		if err != nil {
			feature.Destroy()
			return count, fmt.Errorf("复制要素失败: %v", err)
		}
		count++
		feature.Destroy()
	}

	return count, nil
}

// CopyFieldDefinitions 复制字段定义
func CopyFieldDefinitions(srcLayer, dstLayer *GDALLayer) error {
	if srcLayer == nil || dstLayer == nil {
		return fmt.Errorf("源图层或目标图层为空")
	}

	fieldCount := srcLayer.GetFieldCount()
	for i := 0; i < fieldCount; i++ {
		fieldDefn := srcLayer.GetFieldDefn(i)
		if fieldDefn != nil {
			err := dstLayer.CreateField(fieldDefn)
			if err != nil {
				return fmt.Errorf("复制字段 %d 失败: %v", i, err)
			}
		}
	}
	return nil
}
