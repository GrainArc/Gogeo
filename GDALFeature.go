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
	"unsafe"
)

// GetNextFeatureRaw 获取下一个要素（返回C类型，保持向后兼容）

// ==================== 新增包装函数（返回Go类型） ====================

// GDALFeature 要素包装结构
type GDALFeature struct {
	Feature C.OGRFeatureH
}

// GetNextFeature 获取下一个要素（返回Go包装类型）
func (gl *GDALLayer) GetNextFeature() *GDALFeature {
	if gl.layer == nil {
		return nil
	}
	Feature := C.OGR_L_GetNextFeature(gl.layer)
	if Feature == nil {
		return nil
	}
	return &GDALFeature{Feature: Feature}
}

// WrapFeature 将C类型要素包装为Go类型
func WrapFeature(Feature C.OGRFeatureH) *GDALFeature {
	if Feature == nil {
		return nil
	}
	return &GDALFeature{Feature: Feature}
}

// GetFeature 获取底层C句柄（用于需要直接操作C类型的场景）
func (f *GDALFeature) GetFeature() C.OGRFeatureH {
	if f == nil {
		return nil
	}
	return f.Feature
}

// ==================== 要素操作方法 ====================

// IsValid 检查要素是否有效
func (f *GDALFeature) IsValid() bool {
	return f != nil && f.Feature != nil
}

// GetGeometry 获取几何对象
func (f *GDALFeature) GetGeometry() C.OGRGeometryH {
	if f == nil || f.Feature == nil {
		return nil
	}
	return C.OGR_F_GetGeometryRef(f.Feature)
}

// GetGeometryCopy 获取几何对象的副本（调用者需要负责释放）
func (f *GDALFeature) GetGeometryCopy() C.OGRGeometryH {
	if f == nil || f.Feature == nil {
		return nil
	}
	geom := C.OGR_F_GetGeometryRef(f.Feature)
	if geom == nil {
		return nil
	}
	return C.OGR_G_Clone(geom)
}

// SetGeometry 设置几何对象
func (f *GDALFeature) SetGeometry(geom C.OGRGeometryH) error {
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	if geom == nil {
		return fmt.Errorf("几何为空")
	}
	result := C.OGR_F_SetGeometry(f.Feature, geom)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("设置几何失败，错误码: %d", result)
	}
	return nil
}

// SetGeometryDirectly 设置几何对象（转移所有权，不复制）
func (f *GDALFeature) SetGeometryDirectly(geom C.OGRGeometryH) error {
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	result := C.OGR_F_SetGeometryDirectly(f.Feature, geom)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("设置几何失败，错误码: %d", result)
	}
	return nil
}

// GetFieldIndex 获取字段索引
func (f *GDALFeature) GetFieldIndex(fieldName string) int {
	if f == nil || f.Feature == nil {
		return -1
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))
	return int(C.OGR_F_GetFieldIndex(f.Feature, cFieldName))
}

// GetFieldCount 获取字段数量
func (f *GDALFeature) GetFieldCount() int {
	if f == nil || f.Feature == nil {
		return 0
	}
	return int(C.OGR_F_GetFieldCount(f.Feature))
}

// SetFieldString 设置字符串字段值
func (f *GDALFeature) SetFieldString(fieldName, value string) error {
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return fmt.Errorf("字段 %s 不存在", fieldName)
	}
	C.OGR_F_SetFieldString(f.Feature, index, cValue)
	return nil
}

// SetFieldStringByIndex 通过索引设置字符串字段值
func (f *GDALFeature) SetFieldStringByIndex(index int, value string) {
	if f == nil || f.Feature == nil {
		return
	}
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	C.OGR_F_SetFieldString(f.Feature, C.int(index), cValue)
}

// SetFieldInteger 设置整数字段值
func (f *GDALFeature) SetFieldInteger(fieldName string, value int) error {
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return fmt.Errorf("字段 %s 不存在", fieldName)
	}
	C.OGR_F_SetFieldInteger(f.Feature, index, C.int(value))
	return nil
}

// SetFieldDouble 设置浮点字段值
func (f *GDALFeature) SetFieldDouble(fieldName string, value float64) error {
	if f == nil || f.Feature == nil {
		return fmt.Errorf("要素为空")
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return fmt.Errorf("字段 %s 不存在", fieldName)
	}
	C.OGR_F_SetFieldDouble(f.Feature, index, C.double(value))
	return nil
}

// GetFieldAsString 获取字符串字段值
func (f *GDALFeature) GetFieldAsString(fieldName string) string {
	if f == nil || f.Feature == nil {
		return ""
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return ""
	}
	return C.GoString(C.OGR_F_GetFieldAsString(f.Feature, index))
}

// GetFieldAsStringByIndex 通过索引获取字符串字段值
func (f *GDALFeature) GetFieldAsStringByIndex(index int) string {
	if f == nil || f.Feature == nil {
		return ""
	}
	return C.GoString(C.OGR_F_GetFieldAsString(f.Feature, C.int(index)))
}

// GetFieldAsInteger 获取整数字段值
func (f *GDALFeature) GetFieldAsInteger(fieldName string) int {
	if f == nil || f.Feature == nil {
		return 0
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return 0
	}
	return int(C.OGR_F_GetFieldAsInteger(f.Feature, index))
}

// GetFieldAsDouble 获取浮点字段值
func (f *GDALFeature) GetFieldAsDouble(fieldName string) float64 {
	if f == nil || f.Feature == nil {
		return 0
	}
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))

	index := C.OGR_F_GetFieldIndex(f.Feature, cFieldName)
	if index < 0 {
		return 0
	}
	return float64(C.OGR_F_GetFieldAsDouble(f.Feature, index))
}

// Clone 克隆要素
func (f *GDALFeature) Clone() *GDALFeature {
	if f == nil || f.Feature == nil {
		return nil
	}
	cloned := C.OGR_F_Clone(f.Feature)
	if cloned == nil {
		return nil
	}
	return &GDALFeature{Feature: cloned}
}

// Destroy 销毁要素，释放内存
func (f *GDALFeature) Destroy() {
	if f != nil && f.Feature != nil {
		C.OGR_F_Destroy(f.Feature)
		f.Feature = nil
	}
}
