/*
Copyright (C) 2025 [fmecool]

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
	"unsafe"
)

// 打印相交分析摘要
func (result *GeosAnalysisResult) PrintIntersectionSummary() {
	fmt.Printf("\n=== 空间分析结果摘要 ===\n")
	fmt.Printf("要素数量: %d\n", result.ResultCount)

	if result.OutputLayer != nil {
		fmt.Printf("结果图层字段数量: %d\n", result.OutputLayer.GetFieldCount())
		fmt.Printf("结果图层几何类型: %s\n", result.OutputLayer.GetGeometryType())

		// 打印字段信息
		result.printFieldInfo()

		// 打印前5条要素的详细信息
		result.printSampleFeatures(5)
	}
	fmt.Printf("========================\n\n")
}

// printFieldInfo 打印字段信息
func (result *GeosAnalysisResult) printFieldInfo() {
	if result.OutputLayer == nil {
		return
	}

	layerDefn := result.OutputLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))

	fmt.Printf("\n--- 字段信息 ---\n")
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		fieldType := C.OGR_Fld_GetType(fieldDefn)
		fieldWidth := int(C.OGR_Fld_GetWidth(fieldDefn))
		fieldPrecision := int(C.OGR_Fld_GetPrecision(fieldDefn))

		typeStr := getFieldTypeString(fieldType)
		if fieldWidth > 0 {
			if fieldPrecision > 0 {
				fmt.Printf("  [%d] %s (%s, %d.%d)\n", i, fieldName, typeStr, fieldWidth, fieldPrecision)
			} else {
				fmt.Printf("  [%d] %s (%s, %d)\n", i, fieldName, typeStr, fieldWidth)
			}
		} else {
			fmt.Printf("  [%d] %s (%s)\n", i, fieldName, typeStr)
		}
	}
}

// printSampleFeatures 打印前N条要素的详细信息
func (result *GeosAnalysisResult) printSampleFeatures(maxCount int) {
	if result.OutputLayer == nil || result.ResultCount == 0 {
		fmt.Printf("\n--- 样本要素 ---\n")
		fmt.Printf("无要素数据\n")
		return
	}

	// 限制显示数量
	displayCount := maxCount
	if result.ResultCount < maxCount {
		displayCount = result.ResultCount
	}

	fmt.Printf("\n--- 前 %d 条要素详情 ---\n", displayCount)

	// 重置读取位置
	result.OutputLayer.ResetReading()

	layerDefn := result.OutputLayer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))

	count := 0
	result.OutputLayer.IterateFeatures(func(feature C.OGRFeatureH) {
		if count >= displayCount {
			return
		}

		fmt.Printf("\n要素 #%d:\n", count+1)

		// 打印几何信息
		result.printFeatureGeometry(feature)

		// 打印属性信息
		result.printFeatureAttributes(feature, layerDefn, fieldCount)

		count++
	})
}

// printFeatureGeometry 打印要素几何信息
func (result *GeosAnalysisResult) printFeatureGeometry(feature C.OGRFeatureH) {
	geom := C.OGR_F_GetGeometryRef(feature)
	if geom == nil {
		fmt.Printf("  几何: <空>\n")
		return
	}

	// 获取几何类型
	geomType := C.OGR_G_GetGeometryType(geom)
	geomTypeName := C.GoString(C.OGR_G_GetGeometryName(geom))

	// 获取WKT
	var wktPtr *C.char
	err := C.OGR_G_ExportToWkt(geom, &wktPtr)
	if err != C.OGRERR_NONE {
		fmt.Printf("  几何: %s <WKT导出失败>\n", geomTypeName)
		return
	}
	defer C.CPLFree(unsafe.Pointer(wktPtr))

	wkt := C.GoString(wktPtr)

	// 如果WKT太长，进行截断显示
	const maxWKTLength = 200
	if len(wkt) > maxWKTLength {
		wkt = wkt[:maxWKTLength] + "..."
	}

	fmt.Printf("  几何类型: %s\n", geomTypeName)
	fmt.Printf("  WKT: %s\n", wkt)

	// 打印几何统计信息
	result.printGeometryStats(geom, geomType)
}

// printGeometryStats 打印几何统计信息
func (result *GeosAnalysisResult) printGeometryStats(geom C.OGRGeometryH, geomType C.OGRwkbGeometryType) {
	// 获取包络
	var envelope C.OGREnvelope
	C.OGR_G_GetEnvelope(geom, &envelope)

	fmt.Printf("  包络: (%.6f, %.6f) - (%.6f, %.6f)\n",
		float64(envelope.MinX), float64(envelope.MinY),
		float64(envelope.MaxX), float64(envelope.MaxY))

	// 根据几何类型显示不同的统计信息
	switch geomType {
	case C.wkbPoint, C.wkbMultiPoint:
		// 点几何：显示坐标
		if geomType == C.wkbPoint {
			x := C.OGR_G_GetX(geom, 0)
			y := C.OGR_G_GetY(geom, 0)
			fmt.Printf("  坐标: (%.6f, %.6f)\n", float64(x), float64(y))
		} else {
			pointCount := int(C.OGR_G_GetGeometryCount(geom))
			fmt.Printf("  点数量: %d\n", pointCount)
		}

	case C.wkbLineString, C.wkbMultiLineString:
		// 线几何：显示长度
		length := C.OGR_G_Length(geom)
		fmt.Printf("  长度: %.6f\n", float64(length))

	case C.wkbPolygon, C.wkbMultiPolygon:
		// 面几何：显示面积
		area := C.OGR_G_Area(geom)
		fmt.Printf("  面积: %.6f\n", float64(area))
	}
}

// printFeatureAttributes 打印要素属性信息
func (result *GeosAnalysisResult) printFeatureAttributes(feature C.OGRFeatureH, layerDefn C.OGRFeatureDefnH, fieldCount int) {
	fmt.Printf("  属性:\n")

	hasAttributes := false
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))

		// 检查字段是否已设置
		if C.OGR_F_IsFieldSet(feature, C.int(i)) == 0 {
			continue
		}

		hasAttributes = true
		fieldType := C.OGR_Fld_GetType(fieldDefn)

		// 根据字段类型获取并格式化值
		var valueStr string
		switch fieldType {
		case C.OFTInteger:
			value := C.OGR_F_GetFieldAsInteger(feature, C.int(i))
			valueStr = fmt.Sprintf("%d", int(value))

		case C.OFTReal:
			value := C.OGR_F_GetFieldAsDouble(feature, C.int(i))
			valueStr = fmt.Sprintf("%.6f", float64(value))

		case C.OFTString:
			value := C.OGR_F_GetFieldAsString(feature, C.int(i))
			valueStr = C.GoString(value)
			// 如果字符串太长，进行截断
			if len(valueStr) > 100 {
				valueStr = valueStr[:100] + "..."
			}

		case C.OFTDate:
			year := C.OGR_F_GetFieldAsInteger(feature, C.int(i))
			month := C.OGR_F_GetFieldAsInteger(feature, C.int(i+1))
			day := C.OGR_F_GetFieldAsInteger(feature, C.int(i+2))
			valueStr = fmt.Sprintf("%04d-%02d-%02d", int(year), int(month), int(day))

		case C.OFTDateTime:
			value := C.OGR_F_GetFieldAsString(feature, C.int(i))
			valueStr = C.GoString(value)

		default:
			// 其他类型转为字符串
			value := C.OGR_F_GetFieldAsString(feature, C.int(i))
			valueStr = C.GoString(value)
		}

		fmt.Printf("    %s: %s\n", fieldName, valueStr)
	}

	if !hasAttributes {
		fmt.Printf("    <无属性数据>\n")
	}
}

// getFieldTypeString 获取字段类型的字符串表示
func getFieldTypeString(fieldType C.OGRFieldType) string {
	switch fieldType {
	case C.OFTInteger:
		return "Integer"
	case C.OFTReal:
		return "Real"
	case C.OFTString:
		return "String"
	case C.OFTDate:
		return "Date"
	case C.OFTTime:
		return "Time"
	case C.OFTDateTime:
		return "DateTime"
	case C.OFTBinary:
		return "Binary"
	case C.OFTIntegerList:
		return "IntegerList"
	case C.OFTRealList:
		return "RealList"
	case C.OFTStringList:
		return "StringList"
	default:
		return fmt.Sprintf("Unknown(%d)", int(fieldType))
	}
}
