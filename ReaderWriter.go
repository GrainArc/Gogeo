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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"
)

// PostGISConfig PostGIS连接配置
type PostGISConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	Schema   string
	Table    string
}

// GDALLayer 包装GDAL图层

// PostGISReader PostGIS读取器
type PostGISReader struct {
	config *PostGISConfig
}

// NewPostGISReader 创建新的PostGIS读取器
func NewPostGISReader(config *PostGISConfig) *PostGISReader {
	return &PostGISReader{
		config: config,
	}
}

// ReadGeometryTable 读取PostGIS几何表数据
func (r *PostGISReader) ReadGeometryTable() (*GDALLayer, error) {
	// 初始化GDAL

	// 构建连接字符串
	connStr := fmt.Sprintf("PG:host=%s port=%s dbname=%s user=%s password=%s",
		r.config.Host, r.config.Port, r.config.Database,
		r.config.User, r.config.Password)

	cConnStr := C.CString(connStr)
	defer C.free(unsafe.Pointer(cConnStr))

	// 获取PostgreSQL驱动
	driver := C.OGRGetDriverByName(C.CString("PostgreSQL"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取PostgreSQL驱动")
	}

	// 打开数据源
	dataset := C.OGROpen(cConnStr, C.int(0), nil) // 0表示只读
	if dataset == nil {
		return nil, fmt.Errorf("无法连接到PostGIS数据库: %s", connStr)
	}

	// 构建图层名称（包含schema）
	var layerName string
	if r.config.Schema != "" {
		layerName = fmt.Sprintf("%s.%s", r.config.Schema, r.config.Table)
	} else {
		layerName = r.config.Table
	}

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	// 获取图层
	layer := C.OGR_DS_GetLayerByName(dataset, cLayerName)
	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法找到图层: %s", layerName)
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	// 设置finalizer以确保资源清理
	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)

	return gdalLayer, nil
}

func MakePGReader(table string) *PostGISReader {
	con := MainConfig
	config := &PostGISConfig{
		Host:     con.Host,
		Port:     con.Port,
		Database: con.Dbname,
		User:     con.Username,
		Password: con.Password,
		Schema:   "public", // 可选，默认为public
		Table:    table,
	}
	// 创建读取器
	reader := NewPostGISReader(config)
	return reader
}

// FileGeoReader 文件地理数据读取器
type FileGeoReader struct {
	FilePath string
	FileType string // "shp", "gdb"
}

// NewFileGeoReader 创建新的文件地理数据读取器
func NewFileGeoReader(filePath string) (*FileGeoReader, error) {
	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("文件不存在: %s", filePath)
	}

	// 确定文件类型
	fileType, err := determineFileType(filePath)
	if err != nil {
		return nil, err
	}

	return &FileGeoReader{
		FilePath: filePath,
		FileType: fileType,
	}, nil
}

// determineFileType 确定文件类型
func determineFileType(filePath string) (string, error) {
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".shp":
		return "shp", nil
	case ".gdb":
		return "gdb", nil
	case ".geojson", ".json":
		return "geojson", nil
	case ".dxf":
		return "dxf", nil
	case ".kml":
		return "kml", nil
	case ".kmz":
		return "kmz", nil
	default:
		// 检查是否为文件夹(可能是GDB)
		if info, err := os.Stat(filePath); err == nil && info.IsDir() {
			if strings.HasSuffix(strings.ToLower(filePath), ".gdb") {
				return "gdb", nil
			}
		}
		return "", fmt.Errorf("不支持的文件类型: %s", ext)
	}
}

// ReadShapeFile 读取Shapefile
func (r *FileGeoReader) ReadShapeFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "shp" {
		return nil, fmt.Errorf("文件类型不是Shapefile: %s", r.FileType)
	}

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取Shapefile驱动
	driver := C.OGRGetDriverByName(C.CString("ESRI Shapefile"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取Shapefile驱动")
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil) // 0表示只读
	if dataset == nil {
		return nil, fmt.Errorf("无法打开Shapefile: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	// 如果指定了图层名称，则按名称获取
	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		// 否则获取第一个图层
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	// 设置finalizer以确保资源清理
	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)

	return gdalLayer, nil
}

// ReadGDBFile 读取GDB文件
func (r *FileGeoReader) ReadGDBFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "gdb" {
		return nil, fmt.Errorf("文件类型不是GDB: %s", r.FileType)
	}

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取FileGDB驱动
	driver := C.OGRGetDriverByName(C.CString("FileGDB"))
	if driver == nil {
		// 如果FileGDB驱动不可用，尝试OpenFileGDB驱动
		driver = C.OGRGetDriverByName(C.CString("OpenFileGDB"))
		if driver == nil {
			return nil, fmt.Errorf("无法获取GDB驱动（需要FileGDB或OpenFileGDB驱动）")
		}
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil) // 0表示只读
	if dataset == nil {
		return nil, fmt.Errorf("无法打开GDB文件: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	// 如果指定了图层名称，则按名称获取
	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		// 否则获取第一个图层
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	// 设置finalizer以确保资源清理
	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)

	return gdalLayer, nil
}

// ReadLayer 通用读取图层方法
func (r *FileGeoReader) ReadLayer(layerName ...string) (*GDALLayer, error) {
	switch r.FileType {
	case "shp":
		return r.ReadShapeFile(layerName...)
	case "gdb":
		return r.ReadGDBFile(layerName...)
	case "geojson":
		return r.ReadGeoJSONFile(layerName...)
	case "dxf":
		return r.ReadDXFFile(layerName...)
	case "kml":
		return r.ReadKMLFile(layerName...)
	case "kmz":
		return r.ReadKMZFile(layerName...)
	default:
		return nil, fmt.Errorf("不支持的文件类型: %s", r.FileType)
	}
}

// ListLayers 列出所有图层
func (r *FileGeoReader) ListLayers() ([]string, error) {

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开文件: %s", r.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	layerCount := int(C.OGR_DS_GetLayerCount(dataset))
	layers := make([]string, 0, layerCount)

	for i := 0; i < layerCount; i++ {
		layer := C.OGR_DS_GetLayer(dataset, C.int(i))
		if layer != nil {
			layerName := C.GoString(C.OGR_L_GetName(layer))
			layers = append(layers, layerName)
		}
	}

	return layers, nil
}

// GetLayerInfo 获取图层信息
func (r *FileGeoReader) GetLayerInfo(layerName ...string) (map[string]interface{}, error) {
	layer, err := r.ReadLayer(layerName...)
	if err != nil {
		return nil, err
	}
	defer layer.Close()

	info := make(map[string]interface{})
	info["feature_count"] = layer.GetFeatureCount()
	info["geometry_type"] = layer.GetGeometryType()
	info["field_count"] = layer.GetFieldCount()

	// 获取字段信息
	fields := make([]map[string]interface{}, 0, layer.GetFieldCount())
	for i := 0; i < layer.GetFieldCount(); i++ {
		field := map[string]interface{}{
			"index": i,
			"name":  layer.GetFieldName(i),
			"type":  layer.GetFieldType(i),
		}
		fields = append(fields, field)
	}
	info["fields"] = fields

	// 获取空间参考系统信息
	srs := layer.GetSpatialRef()
	if srs != nil {
		var projStr *C.char
		C.OSRExportToProj4(srs, &projStr)
		if projStr != nil {
			info["projection"] = C.GoString(projStr)
			C.CPLFree(unsafe.Pointer(projStr))
		}
	}

	return info, nil
}

// GetFieldType 获取字段类型
func (gl *GDALLayer) GetFieldType(index int) string {
	defn := gl.GetLayerDefn()
	fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(index))
	if fieldDefn == nil {
		return ""
	}
	fieldType := C.OGR_Fld_GetType(fieldDefn)
	return C.GoString(C.OGR_GetFieldTypeName(fieldType))
}

// 便捷函数

// MakeShapeFileReader 创建Shapefile读取器
func MakeShapeFileReader(filePath string) (*FileGeoReader, error) {
	return NewFileGeoReader(filePath)
}

// MakeGDBReader 创建GDB读取器
func MakeGDBReader(filePath string) (*FileGeoReader, error) {
	return NewFileGeoReader(filePath)
}

// ReadShapeFileLayer 直接读取Shapefile图层
func ReadShapeFileLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadShapeFile(layerName...)
}

// ReadGDBLayer 直接读取GDB图层
func ReadGDBLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadGDBFile(layerName...)
}

// ReadGeospatialFile 通用读取地理空间文件
func ReadGeospatialFile(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadLayer(layerName...)
}

// FileGeoWriter 文件地理数据写入器
type FileGeoWriter struct {
	FilePath  string
	FileType  string // "shp", "gdb"
	Overwrite bool   // 是否覆盖已存在的文件
}

// NewFileGeoWriter 创建新的文件地理数据写入器
func NewFileGeoWriter(filePath string, overwrite bool) (*FileGeoWriter, error) {
	// 确定文件类型
	fileType, err := determineFileTypeForWrite(filePath)
	if err != nil {
		return nil, err
	}

	return &FileGeoWriter{
		FilePath:  filePath,
		FileType:  fileType,
		Overwrite: overwrite,
	}, nil
}

// determineFileTypeForWrite 确定写入文件类型
func determineFileTypeForWrite(filePath string) (string, error) {
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".shp":
		return "shp", nil
	case ".gdb":
		return "gdb", nil
	case ".geojson", ".json":
		return "geojson", nil
	case ".dxf":
		return "dxf", nil
	case ".kml":
		return "kml", nil
	case ".kmz":
		return "kmz", nil
	default:
		if strings.HasSuffix(strings.ToLower(filePath), ".gdb") {
			return "gdb", nil
		}
		return "", fmt.Errorf("不支持的文件类型: %s", ext)
	}
}

// WriteShapeFile 写入Shapefile
func (w *FileGeoWriter) WriteShapeFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "shp" {
		return fmt.Errorf("文件类型不是Shapefile: %s", w.FileType)
	}

	// 设置Shapefile编码为GBK/GB2312（中文Windows系统）
	C.CPLSetConfigOption(C.CString("SHAPE_ENCODING"), C.CString("GBK"))
	defer C.CPLSetConfigOption(C.CString("SHAPE_ENCODING"), nil)
	// 如果需要覆盖，先删除已存在的文件
	if w.Overwrite {
		w.removeShapeFiles()
	}

	// 获取Shapefile驱动
	driver := C.OGRGetDriverByName(C.CString("ESRI Shapefile"))
	if driver == nil {
		return fmt.Errorf("无法获取Shapefile驱动")
	}

	// 创建数据源
	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建Shapefile: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	// 创建图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	// 复制字段定义
	err := w.copyFieldDefinitions(sourceDefn, newLayer)
	if err != nil {
		return err
	}

	// 复制要素
	err = w.copyFeatures(sourceLayer, newLayer)
	if err != nil {
		return err
	}

	return nil
}

// WriteGDBFile 写入GDB文件
func (w *FileGeoWriter) WriteGDBFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "gdb" {
		return fmt.Errorf("文件类型不是GDB: %s", w.FileType)
	}

	// 获取FileGDB驱动
	driver := C.OGRGetDriverByName(C.CString("OpenFileGDB"))
	if driver == nil {
		// 如果FileGDB驱动不可用，尝试OpenFileGDB驱动（但OpenFileGDB通常是只读的）
		return fmt.Errorf("无法获取FileGDB驱动（需要FileGDB驱动支持写入）")
	}

	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	var dataset C.OGRDataSourceH

	// 检查GDB是否已存在
	if _, err := os.Stat(w.FilePath); err == nil {
		if w.Overwrite {
			// 删除已存在的GDB
			os.RemoveAll(w.FilePath)
			// 创建新的GDB
			dataset = C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
		} else {
			// 打开已存在的GDB
			dataset = C.OGROpen(cFilePath, C.int(1), nil) // 1表示可写
		}
	} else {
		// 创建新的GDB
		dataset = C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
	}

	if dataset == nil {
		return fmt.Errorf("无法创建或打开GDB文件: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	// 创建图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	// 复制字段定义
	err := w.copyFieldDefinitions(sourceDefn, newLayer)
	if err != nil {
		return err
	}

	// 复制要素
	err = w.copyFeatures(sourceLayer, newLayer)
	if err != nil {
		return err
	}

	return nil
}

// WriteLayer 通用写入图层方法
func (w *FileGeoWriter) WriteLayer(sourceLayer *GDALLayer, layerName string) error {
	switch w.FileType {
	case "shp":
		return w.WriteShapeFile(sourceLayer, layerName)
	case "gdb":
		return w.WriteGDBFile(sourceLayer, layerName)
	case "geojson":
		return w.WriteGeoJSONFile(sourceLayer, layerName)
	case "dxf":
		return w.WriteDXFFile(sourceLayer, layerName)
	case "kml":
		return w.WriteKMLFile(sourceLayer, layerName)
	case "kmz":
		return w.WriteKMZFile(sourceLayer, layerName)
	default:
		return fmt.Errorf("不支持的文件类型: %s", w.FileType)
	}
}

// copyFieldDefinitions 复制字段定义（改进版）
func (w *FileGeoWriter) copyFieldDefinitions(sourceDefn C.OGRFeatureDefnH, targetLayer C.OGRLayerH) error {
	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))

	// GDB保留字段名列表
	reservedFields := map[string]bool{
		"shape":        true,
		"shape_area":   true,
		"shape_length": true,
		"oid":          true,
	}

	for i := 0; i < fieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		// 获取原始字段名
		originalName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))
		fieldType := C.OGR_Fld_GetType(sourceFieldDefn)

		// 检查是否为保留字段，如果是则跳过
		if reservedFields[strings.ToLower(originalName)] {
			fmt.Printf("跳过保留字段: %s\n", originalName)
			continue
		}

		// 处理字段名（确保符合GDB命名规范）
		fieldName := w.sanitizeFieldName(originalName)

		// 处理字段类型（确保与GDB兼容）
		targetFieldType := w.mapFieldTypeForGDB(fieldType, w.FileType)

		// 创建新的字段定义
		cFieldName := C.CString(fieldName)
		defer C.free(unsafe.Pointer(cFieldName))

		newFieldDefn := C.OGR_Fld_Create(cFieldName, targetFieldType)
		if newFieldDefn == nil {
			fmt.Printf("警告: 无法创建字段定义 %s，跳过\n", fieldName)
			continue
		}

		// 复制字段属性
		C.OGR_Fld_SetWidth(newFieldDefn, C.OGR_Fld_GetWidth(sourceFieldDefn))
		C.OGR_Fld_SetPrecision(newFieldDefn, C.OGR_Fld_GetPrecision(sourceFieldDefn))

		// 添加字段到目标图层
		result := C.OGR_L_CreateField(targetLayer, newFieldDefn, C.int(1))
		if result != C.OGRERR_NONE {
			fmt.Printf("警告: 无法创建字段 %s (错误代码: %d)，跳过\n", fieldName, int(result))
		}

		C.OGR_Fld_Destroy(newFieldDefn)
	}

	return nil
}

// sanitizeFieldName 清理字段名以符合目标格式要求
func (w *FileGeoWriter) sanitizeFieldName(name string) string {
	// 移除特殊字符，替换为下划线
	sanitized := strings.ReplaceAll(name, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")

	// 确保字段名不以数字开头
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "f_" + sanitized
	}

	// 限制字段名长度（Shapefile限制为10个字符）
	if w.FileType == "shp" && len(sanitized) > 10 {
		sanitized = sanitized[:10]
	}

	return sanitized
}

// mapFieldTypeForGDB 映射字段类型以兼容目标格式
func (w *FileGeoWriter) mapFieldTypeForGDB(sourceType C.OGRFieldType, targetFormat string) C.OGRFieldType {
	switch sourceType {
	case C.OFTInteger64:
		// GDB可能不完全支持64位整数，转换为双精度浮点数
		if targetFormat == "gdb" {
			return C.OFTReal
		}
		return sourceType

	case C.OFTBinary:
		// 二进制字段转换为字符串
		return C.OFTString

	case C.OFTIntegerList, C.OFTRealList, C.OFTStringList:
		// 列表类型转换为字符串
		return C.OFTString

	default:
		return sourceType
	}
}

// copyFeatures 复制要素（跳过错误要素）
func (w *FileGeoWriter) copyFeatures(sourceLayer *GDALLayer, targetLayer C.OGRLayerH) error {
	sourceLayer.ResetReading()
	targetDefn := C.OGR_L_GetLayerDefn(targetLayer)

	var totalFeatures, successCount, errorCount int

	for {
		sourceFeature := sourceLayer.GetNextFeatureRow()
		if sourceFeature == nil {
			break
		}

		totalFeatures++

		// 使用recover机制捕获panic
		func() {
			defer func() {
				if r := recover(); r != nil {

					errorCount++
				}
				// 确保源要素被释放
				C.OGR_F_Destroy(sourceFeature)
			}()

			// 尝试复制要素
			if err := w.copyFeatureSafely(sourceFeature, targetLayer, targetDefn); err != nil {

				errorCount++
			} else {
				successCount++
			}
		}()
	}

	return nil
}

// copyFeatureSafely 安全地复制单个要素
func (w *FileGeoWriter) copyFeatureSafely(sourceFeature C.OGRFeatureH, targetLayer C.OGRLayerH, targetDefn C.OGRFeatureDefnH) error {
	// 创建新要素
	newFeature := C.OGR_F_Create(targetDefn)
	if newFeature == nil {
		return fmt.Errorf("无法创建新要素")
	}

	// 确保新要素被释放
	defer C.OGR_F_Destroy(newFeature)

	// 复制几何
	if err := w.copyGeometrySafely(sourceFeature, newFeature); err != nil {
		return fmt.Errorf("几何复制失败: %v", err)
	}

	// 复制字段值
	if err := w.copyFieldsSafely(sourceFeature, newFeature); err != nil {
		return fmt.Errorf("字段复制失败: %v", err)
	}

	// 验证要素是否有效
	if err := w.validateFeature(newFeature); err != nil {
		return fmt.Errorf("要素验证失败: %v", err)
	}

	// 添加要素到目标图层
	result := C.OGR_L_CreateFeature(targetLayer, newFeature)
	if result != C.OGRERR_NONE {
		return fmt.Errorf("无法添加要素到目标图层，错误代码: %d", int(result))
	}

	return nil
}

// 修改copyGeometrySafely方法以使用normalizeGeometryType
func (w *FileGeoWriter) copyGeometrySafely(sourceFeature, newFeature C.OGRFeatureH) error {
	geometry := C.OGR_F_GetGeometryRef(sourceFeature)
	if geometry == nil {
		return nil
	}

	// 获取目标图层的几何类型
	targetDefn := C.OGR_F_GetDefnRef(newFeature)
	targetGeomType := C.OGR_FD_GetGeomType(targetDefn)

	// 检查几何是否有效
	if C.OGR_G_IsValid(geometry) == 0 {
		validGeom := C.OGR_G_MakeValid(geometry)
		if validGeom != nil {
			defer C.OGR_G_DestroyGeometry(validGeom)
			if C.OGR_G_IsValid(validGeom) != 0 {
				geometry = validGeom
			} else {
				return fmt.Errorf("几何无效且无法修复")
			}
		} else {
			return fmt.Errorf("几何无效且无法修复")
		}
	}

	// 使用normalizeGeometryType进行几何类型规范化
	normalizedGeom := C.normalizeGeometryType(geometry, targetGeomType)
	if normalizedGeom == nil {
		return fmt.Errorf("无法规范化几何类型到目标类型: %s",
			C.GoString(C.OGRGeometryTypeToName(targetGeomType)))
	}

	// 如果规范化产生了新的几何体，需要在使用后清理
	shouldCleanup := (normalizedGeom != geometry)
	if shouldCleanup {
		defer C.OGR_G_DestroyGeometry(normalizedGeom)
	}

	// 再次验证规范化后的几何类型
	normalizedType := C.OGR_G_GetGeometryType(normalizedGeom)
	if !w.isGeometryTypeCompatible(normalizedType, targetGeomType) {
		return fmt.Errorf("规范化后的几何类型 %s 仍不兼容目标类型 %s",
			C.GoString(C.OGRGeometryTypeToName(normalizedType)),
			C.GoString(C.OGRGeometryTypeToName(targetGeomType)))
	}

	// 克隆几何体用于设置
	clonedGeom := C.OGR_G_Clone(normalizedGeom)
	if clonedGeom == nil {
		return fmt.Errorf("无法克隆规范化的几何")
	}

	// 设置几何
	result := C.OGR_F_SetGeometry(newFeature, clonedGeom)
	C.OGR_G_DestroyGeometry(clonedGeom)

	if result != C.OGRERR_NONE {
		return fmt.Errorf("设置几何失败，错误代码: %d", int(result))
	}

	return nil
}

// 检查几何类型兼容性
func (w *FileGeoWriter) isGeometryTypeCompatible(sourceType, targetType C.OGRwkbGeometryType) bool {
	// 完全匹配
	if sourceType == targetType {
		return true
	}

	// 检查兼容的类型组合
	compatiblePairs := map[C.OGRwkbGeometryType][]C.OGRwkbGeometryType{
		C.wkbPolygon:         {C.wkbPolygon, C.wkbMultiPolygon, C.wkbPolygon25D, C.wkbMultiPolygon25D},
		C.wkbMultiPolygon:    {C.wkbPolygon, C.wkbMultiPolygon, C.wkbPolygon25D, C.wkbMultiPolygon25D},
		C.wkbLineString:      {C.wkbLineString, C.wkbMultiLineString, C.wkbLineString25D, C.wkbMultiLineString25D},
		C.wkbMultiLineString: {C.wkbLineString, C.wkbMultiLineString, C.wkbLineString25D, C.wkbMultiLineString25D},
		C.wkbPoint:           {C.wkbPoint, C.wkbMultiPoint, C.wkbPoint25D, C.wkbMultiPoint25D},
		C.wkbMultiPoint:      {C.wkbPoint, C.wkbMultiPoint, C.wkbPoint25D, C.wkbMultiPoint25D},
	}

	if compatibleTypes, exists := compatiblePairs[targetType]; exists {
		for _, compatibleType := range compatibleTypes {
			if sourceType == compatibleType {
				return true
			}
		}
	}

	return false
}

// removeShapeFiles 删除Shapefile相关文件
func (w *FileGeoWriter) removeShapeFiles() {
	baseName := strings.TrimSuffix(w.FilePath, filepath.Ext(w.FilePath))
	extensions := []string{".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix", ".sbn", ".sbx"}

	for _, ext := range extensions {
		filePath := baseName + ext
		if _, err := os.Stat(filePath); err == nil {
			os.Remove(filePath)
		}
	}
}

// copyFieldsSafely 安全地复制字段（修复版 - 按字段名匹配）
func (w *FileGeoWriter) copyFieldsSafely(sourceFeature, newFeature C.OGRFeatureH) error {
	// 获取源要素和目标要素的定义
	sourceDefn := C.OGR_F_GetDefnRef(sourceFeature)
	targetDefn := C.OGR_F_GetDefnRef(newFeature)

	if sourceDefn == nil || targetDefn == nil {
		return fmt.Errorf("无法获取要素定义")
	}

	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))
	targetFieldCount := int(C.OGR_FD_GetFieldCount(targetDefn))

	// 创建字段名到索引的映射
	targetFieldMap := make(map[string]int)
	for i := 0; i < targetFieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(targetDefn, C.int(i))
		if fieldDefn != nil {
			fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
			targetFieldMap[fieldName] = i
		}
	}

	// GDB保留字段名列表（需要跳过的字段）
	reservedFields := map[string]bool{
		"shape":        true,
		"shape_area":   true,
		"shape_length": true,
		"oid":          true,
	}

	// 遍历源字段，按字段名匹配复制
	for sourceIndex := 0; sourceIndex < sourceFieldCount; sourceIndex++ {
		// 检查源字段是否设置
		if C.OGR_F_IsFieldSet(sourceFeature, C.int(sourceIndex)) == 0 {
			continue
		}

		// 获取源字段定义
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(sourceIndex))
		if sourceFieldDefn == nil {
			continue
		}

		// 获取源字段名
		originalFieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 跳过保留字段
		if reservedFields[strings.ToLower(originalFieldName)] {
			continue
		}

		// 处理字段名（与copyFieldDefinitions中的处理保持一致）
		sanitizedFieldName := w.sanitizeFieldName(originalFieldName)

		// 查找目标字段索引
		targetIndex, exists := targetFieldMap[sanitizedFieldName]
		if !exists {
			// 如果找不到对应的目标字段，跳过
			fmt.Printf("警告: 目标图层中未找到字段 '%s'（原字段名: '%s'），跳过\n",
				sanitizedFieldName, originalFieldName)
			continue
		}

		// 获取源字段类型
		sourceFieldType := C.OGR_Fld_GetType(sourceFieldDefn)

		// 获取目标字段类型
		targetFieldDefn := C.OGR_FD_GetFieldDefn(targetDefn, C.int(targetIndex))
		if targetFieldDefn == nil {
			continue
		}
		targetFieldType := C.OGR_Fld_GetType(targetFieldDefn)

		// 复制字段值（使用目标字段索引）
		if err := w.copyFieldValueSafelyByName(sourceFeature, newFeature,
			sourceIndex, targetIndex, sourceFieldType, targetFieldType,
			originalFieldName); err != nil {
			fmt.Printf("字段 '%s' 复制失败，跳过: %v\n", originalFieldName, err)
			// 不返回错误，继续处理其他字段
		}
	}

	return nil
}

// copyFieldValueSafelyByName 按字段名安全地复制字段值
func (w *FileGeoWriter) copyFieldValueSafelyByName(sourceFeature, newFeature C.OGRFeatureH,
	sourceIndex, targetIndex int, sourceFieldType, targetFieldType C.OGRFieldType,
	fieldName string) error {

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("字段 '%s' 值复制时发生panic: %v\n", fieldName, r)
		}
	}()

	// 如果源字段类型和目标字段类型不同，进行类型转换
	if sourceFieldType != targetFieldType {
		return w.copyFieldValueWithTypeConversion(sourceFeature, newFeature,
			sourceIndex, targetIndex, sourceFieldType, targetFieldType, fieldName)
	}

	// 类型相同时的直接复制
	switch sourceFieldType {
	case C.OFTInteger:
		value := C.OGR_F_GetFieldAsInteger(sourceFeature, C.int(sourceIndex))
		C.OGR_F_SetFieldInteger(newFeature, C.int(targetIndex), value)

	case C.OFTInteger64:
		value := C.OGR_F_GetFieldAsInteger64(sourceFeature, C.int(sourceIndex))
		C.OGR_F_SetFieldInteger64(newFeature, C.int(targetIndex), value)

	case C.OFTReal:
		value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(sourceIndex))
		// 检查是否为无效数值
		if C.check_isnan(C.double(value)) != 0 || C.check_isinf(C.double(value)) != 0 {
			return fmt.Errorf("字段包含无效数值 (NaN 或 Inf)")
		}
		C.OGR_F_SetFieldDouble(newFeature, C.int(targetIndex), value)

	case C.OFTString:
		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIndex))
		if value == nil {
			return fmt.Errorf("字符串字段为空指针")
		}
		C.OGR_F_SetFieldString(newFeature, C.int(targetIndex), value)

	case C.OFTDate:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(sourceIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(targetIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	case C.OFTTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(sourceIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(targetIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	case C.OFTDateTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(sourceIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(targetIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	default:
		// 对于不支持的字段类型，尝试作为字符串处理
		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIndex))
		if value != nil {
			C.OGR_F_SetFieldString(newFeature, C.int(targetIndex), value)
		}
	}

	return nil
}

// copyFieldValueWithTypeConversion 带类型转换的字段值复制
func (w *FileGeoWriter) copyFieldValueWithTypeConversion(sourceFeature, newFeature C.OGRFeatureH,
	sourceIndex, targetIndex int, sourceFieldType, targetFieldType C.OGRFieldType,
	fieldName string) error {

	// 处理常见的类型转换情况
	switch {
	case sourceFieldType == C.OFTInteger64 && targetFieldType == C.OFTReal:
		// Integer64 -> Real
		value := C.OGR_F_GetFieldAsInteger64(sourceFeature, C.int(sourceIndex))
		C.OGR_F_SetFieldDouble(newFeature, C.int(targetIndex), C.double(value))

	case sourceFieldType == C.OFTInteger && targetFieldType == C.OFTReal:
		// Integer -> Real
		value := C.OGR_F_GetFieldAsInteger(sourceFeature, C.int(sourceIndex))
		C.OGR_F_SetFieldDouble(newFeature, C.int(targetIndex), C.double(value))

	case sourceFieldType == C.OFTReal && targetFieldType == C.OFTInteger:
		// Real -> Integer (可能丢失精度)
		value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(sourceIndex))
		if C.check_isnan(C.double(value)) != 0 || C.check_isinf(C.double(value)) != 0 {
			return fmt.Errorf("无法将无效数值转换为整数")
		}
		C.OGR_F_SetFieldInteger(newFeature, C.int(targetIndex), C.int(value))

	case sourceFieldType == C.OFTReal && targetFieldType == C.OFTInteger64:
		// Real -> Integer64 (可能丢失精度)
		value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(sourceIndex))
		if C.check_isnan(C.double(value)) != 0 || C.check_isinf(C.double(value)) != 0 {
			return fmt.Errorf("无法将无效数值转换为整数")
		}
		C.OGR_F_SetFieldInteger64(newFeature, C.int(targetIndex), C.longlong(value))

	case targetFieldType == C.OFTString:
		// 任何类型 -> String
		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIndex))
		if value != nil {
			C.OGR_F_SetFieldString(newFeature, C.int(targetIndex), value)
		}

	default:
		// 不支持的类型转换，尝试作为字符串处理
		fmt.Printf("警告: 字段 '%s' 类型转换不支持 (%s -> %s)，尝试作为字符串处理\n",
			fieldName,
			C.GoString(C.OGR_GetFieldTypeName(sourceFieldType)),
			C.GoString(C.OGR_GetFieldTypeName(targetFieldType)))

		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(sourceIndex))
		if value != nil {
			C.OGR_F_SetFieldString(newFeature, C.int(targetIndex), value)
		}
	}

	return nil
}

// copyFieldValueSafely 安全地复制字段值
func (w *FileGeoWriter) copyFieldValueSafely(sourceFeature, newFeature C.OGRFeatureH, fieldIndex int, fieldType C.OGRFieldType) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("字段 %d 值复制时发生panic: %v\n", fieldIndex, r)
		}
	}()

	switch fieldType {
	case C.OFTInteger:
		value := C.OGR_F_GetFieldAsInteger(sourceFeature, C.int(fieldIndex))
		C.OGR_F_SetFieldInteger(newFeature, C.int(fieldIndex), value)

	case C.OFTInteger64:
		value := C.OGR_F_GetFieldAsInteger64(sourceFeature, C.int(fieldIndex))
		C.OGR_F_SetFieldInteger64(newFeature, C.int(fieldIndex), value)

	case C.OFTReal:
		value := C.OGR_F_GetFieldAsDouble(sourceFeature, C.int(fieldIndex))
		// 检查是否为无效数值
		if C.check_isnan(C.double(value)) != 0 || C.check_isinf(C.double(value)) != 0 {
			return fmt.Errorf("字段包含无效数值 (NaN 或 Inf)")
		}
		C.OGR_F_SetFieldDouble(newFeature, C.int(fieldIndex), value)

	case C.OFTString:
		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(fieldIndex))
		if value == nil {
			return fmt.Errorf("字符串字段为空指针")
		}
		C.OGR_F_SetFieldString(newFeature, C.int(fieldIndex), value)

	case C.OFTDate:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(fieldIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	case C.OFTTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(fieldIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	case C.OFTDateTime:
		var year, month, day, hour, minute, second, tzflag C.int
		result := C.OGR_F_GetFieldAsDateTime(sourceFeature, C.int(fieldIndex),
			&year, &month, &day, &hour, &minute, &second, &tzflag)
		if result != 0 {
			C.OGR_F_SetFieldDateTime(newFeature, C.int(fieldIndex),
				year, month, day, hour, minute, second, tzflag)
		}

	default:
		// 对于不支持的字段类型，尝试作为字符串处理
		value := C.OGR_F_GetFieldAsString(sourceFeature, C.int(fieldIndex))
		if value != nil {
			C.OGR_F_SetFieldString(newFeature, C.int(fieldIndex), value)
		}
	}

	return nil
}

// validateFeature 验证要素是否有效
func (w *FileGeoWriter) validateFeature(feature C.OGRFeatureH) error {
	// 检查几何是否有效（如果存在）
	geometry := C.OGR_F_GetGeometryRef(feature)
	if geometry != nil {
		if C.OGR_G_IsValid(geometry) == 0 {
			return fmt.Errorf("要素几何无效")
		}

		// 检查几何是否为空
		if C.OGR_G_IsEmpty(geometry) != 0 {
			// 空几何在某些情况下是允许的，这里可以根据需要调整
			fmt.Printf("警告: 要素包含空几何\n")
		}
	}

	return nil
}

// WriteShapeFileLayer 直接写入Shapefile图层
func WriteShapeFileLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteShapeFile(sourceLayer, layerName)
}

// WriteGDBLayer 直接写入GDB图层
func WriteGDBLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteGDBFile(sourceLayer, layerName)
}

// WriteGeospatialFile 通用写入地理空间文件
func WriteGeospatialFile(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteLayer(sourceLayer, layerName)
}

// CopyLayerToFile 复制图层到文件
func CopyLayerToFile(sourceLayer *GDALLayer, targetFilePath string, targetLayerName string, overwrite bool) error {
	return WriteGeospatialFile(sourceLayer, targetFilePath, targetLayerName, overwrite)
}

// ConvertFile 文件格式转换
func ConvertFile(sourceFilePath string, targetFilePath string, sourceLayerName string, targetLayerName string, overwrite bool) error {
	// 读取源文件
	sourceReader, err := NewFileGeoReader(sourceFilePath)
	if err != nil {
		return fmt.Errorf("无法读取源文件: %v", err)
	}

	sourceLayer, err := sourceReader.ReadLayer(sourceLayerName)
	if err != nil {
		return fmt.Errorf("无法读取源图层: %v", err)
	}
	defer sourceLayer.Close()

	// 写入目标文件
	err = WriteGeospatialFile(sourceLayer, targetFilePath, targetLayerName, overwrite)
	if err != nil {
		return fmt.Errorf("无法写入目标文件: %v", err)
	}

	return nil
}

// ReadGeoJSONFile 读取GeoJSON文件
func (r *FileGeoReader) ReadGeoJSONFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "geojson" {
		return nil, fmt.Errorf("文件类型不是GeoJSON: %s", r.FileType)
	}

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取GeoJSON驱动
	driver := C.OGRGetDriverByName(C.CString("GeoJSON"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取GeoJSON驱动")
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开GeoJSON文件: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ========== DXF 读取 ==========

// ReadDXFFile 读取DXF文件
func (r *FileGeoReader) ReadDXFFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "dxf" {
		return nil, fmt.Errorf("文件类型不是DXF: %s", r.FileType)
	}

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取DXF驱动
	driver := C.OGRGetDriverByName(C.CString("DXF"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取DXF驱动")
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开DXF文件: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ========== KML 读取 ==========

// ReadKMLFile 读取KML文件
func (r *FileGeoReader) ReadKMLFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "kml" {
		return nil, fmt.Errorf("文件类型不是KML: %s", r.FileType)
	}

	cFilePath := C.CString(r.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取KML驱动(LIBKML优先,否则使用KML)
	driver := C.OGRGetDriverByName(C.CString("LIBKML"))
	if driver == nil {
		driver = C.OGRGetDriverByName(C.CString("KML"))
		if driver == nil {
			return nil, fmt.Errorf("无法获取KML驱动")
		}
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开KML文件: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ========== KMZ 读取 ==========

// ReadKMZFile 读取KMZ文件(压缩的KML)
func (r *FileGeoReader) ReadKMZFile(layerName ...string) (*GDALLayer, error) {
	if r.FileType != "kmz" {
		return nil, fmt.Errorf("文件类型不是KMZ: %s", r.FileType)
	}

	// KMZ文件需要使用/vsizip/前缀访问
	kmzPath := fmt.Sprintf("/vsizip/%s", r.FilePath)
	cFilePath := C.CString(kmzPath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 获取KML驱动
	driver := C.OGRGetDriverByName(C.CString("LIBKML"))
	if driver == nil {
		driver = C.OGRGetDriverByName(C.CString("KML"))
		if driver == nil {
			return nil, fmt.Errorf("无法获取KML驱动")
		}
	}

	// 打开数据源
	dataset := C.OGROpen(cFilePath, C.int(0), nil)
	if dataset == nil {
		return nil, fmt.Errorf("无法打开KMZ文件: %s", r.FilePath)
	}

	var layer C.OGRLayerH

	if len(layerName) > 0 && layerName[0] != "" {
		cLayerName := C.CString(layerName[0])
		defer C.free(unsafe.Pointer(cLayerName))
		layer = C.OGR_DS_GetLayerByName(dataset, cLayerName)
	} else {
		if C.OGR_DS_GetLayerCount(dataset) > 0 {
			layer = C.OGR_DS_GetLayer(dataset, C.int(0))
		}
	}

	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("无法获取图层")
	}

	gdalLayer := &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}

	runtime.SetFinalizer(gdalLayer, (*GDALLayer).cleanup)
	return gdalLayer, nil
}

// ========== GeoJSON 写入 ==========

// WriteGeoJSONFile 写入GeoJSON文件
func (w *FileGeoWriter) WriteGeoJSONFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "geojson" {
		return fmt.Errorf("文件类型不是GeoJSON: %s", w.FileType)
	}

	// GeoJSON默认使用UTF-8编码
	C.CPLSetConfigOption(C.CString("ENCODING"), C.CString("UTF-8"))
	defer C.CPLSetConfigOption(C.CString("ENCODING"), nil)

	if w.Overwrite {
		if _, err := os.Stat(w.FilePath); err == nil {
			os.Remove(w.FilePath)
		}
	}

	// 获取GeoJSON驱动
	driver := C.OGRGetDriverByName(C.CString("GeoJSON"))
	if driver == nil {
		return fmt.Errorf("无法获取GeoJSON驱动")
	}

	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	// 创建数据源(带选项)
	// RFC7946=YES 确保符合GeoJSON规范
	options := []*C.char{
		C.CString("RFC7946=YES"),
		nil,
	}
	defer C.free(unsafe.Pointer(options[0]))

	dataset := C.OGR_Dr_CreateDataSource(driver, cFilePath, &options[0])
	if dataset == nil {
		return fmt.Errorf("无法创建GeoJSON文件: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	if err := w.copyFieldDefinitions(sourceDefn, newLayer); err != nil {
		return err
	}

	if err := w.copyFeatures(sourceLayer, newLayer); err != nil {
		return err
	}

	return nil
}

// ========== DXF 写入 ==========

// WriteDXFFile 写入DXF文件
func (w *FileGeoWriter) WriteDXFFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "dxf" {
		return fmt.Errorf("文件类型不是DXF: %s", w.FileType)
	}

	if w.Overwrite {
		if _, err := os.Stat(w.FilePath); err == nil {
			os.Remove(w.FilePath)
		}
	}

	// 获取DXF驱动
	driver := C.OGRGetDriverByName(C.CString("DXF"))
	if driver == nil {
		return fmt.Errorf("无法获取DXF驱动")
	}

	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建DXF文件: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	if err := w.copyFieldDefinitions(sourceDefn, newLayer); err != nil {
		return err
	}

	if err := w.copyFeatures(sourceLayer, newLayer); err != nil {
		return err
	}

	return nil
}

// ========== KML 写入 ==========

// WriteKMLFile 写入KML文件
func (w *FileGeoWriter) WriteKMLFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "kml" {
		return fmt.Errorf("文件类型不是KML: %s", w.FileType)
	}

	if w.Overwrite {
		if _, err := os.Stat(w.FilePath); err == nil {
			os.Remove(w.FilePath)
		}
	}

	// 获取KML驱动(LIBKML优先)
	driver := C.OGRGetDriverByName(C.CString("LIBKML"))
	if driver == nil {
		driver = C.OGRGetDriverByName(C.CString("KML"))
		if driver == nil {
			return fmt.Errorf("无法获取KML驱动")
		}
	}

	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建KML文件: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	// KML总是使用WGS84坐标系
	srs := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(srs)
	C.OSRImportFromEPSG(srs, 4326)

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	if err := w.copyFieldDefinitions(sourceDefn, newLayer); err != nil {
		return err
	}

	// 如果源坐标系不是WGS84,需要进行坐标转换
	if err := w.copyFeaturesWithTransform(sourceLayer, newLayer, srs); err != nil {
		return err
	}

	return nil
}

// ========== KMZ 写入 ==========

// WriteKMZFile 写入KMZ文件(压缩的KML)
func (w *FileGeoWriter) WriteKMZFile(sourceLayer *GDALLayer, layerName string) error {
	if w.FileType != "kmz" {
		return fmt.Errorf("文件类型不是KMZ: %s", w.FileType)
	}

	if w.Overwrite {
		if _, err := os.Stat(w.FilePath); err == nil {
			os.Remove(w.FilePath)
		}
	}

	// 获取LIBKML驱动(KMZ需要LIBKML支持)
	driver := C.OGRGetDriverByName(C.CString("LIBKML"))
	if driver == nil {
		return fmt.Errorf("无法获取LIBKML驱动(KMZ需要LIBKML支持)")
	}

	cFilePath := C.CString(w.FilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cFilePath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建KMZ文件: %s", w.FilePath)
	}
	defer C.OGR_DS_Destroy(dataset)

	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)

	// KMZ总是使用WGS84坐标系
	srs := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(srs)
	C.OSRImportFromEPSG(srs, 4326)

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	if err := w.copyFieldDefinitions(sourceDefn, newLayer); err != nil {
		return err
	}

	if err := w.copyFeaturesWithTransform(sourceLayer, newLayer, srs); err != nil {
		return err
	}

	return nil
}

// ========== 坐标转换辅助方法 ==========

// copyFeaturesWithTransform 复制要素并进行坐标转换
func (w *FileGeoWriter) copyFeaturesWithTransform(sourceLayer *GDALLayer, targetLayer C.OGRLayerH, targetSRS C.OGRSpatialReferenceH) error {
	sourceLayer.ResetReading()
	targetDefn := C.OGR_L_GetLayerDefn(targetLayer)
	sourceSRS := sourceLayer.GetSpatialRef()

	var transform C.OGRCoordinateTransformationH
	needsTransform := false

	// 检查是否需要坐标转换
	if sourceSRS != nil && targetSRS != nil {
		if C.OSRIsSame(sourceSRS, targetSRS) == 0 {
			transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
			if transform != nil {
				needsTransform = true
				defer C.OCTDestroyCoordinateTransformation(transform)
			}
		}
	}

	var totalFeatures, successCount, errorCount int

	for {
		sourceFeature := sourceLayer.GetNextFeatureRow()
		if sourceFeature == nil {
			break
		}

		totalFeatures++

		func() {
			defer func() {
				if r := recover(); r != nil {
					errorCount++
				}
				C.OGR_F_Destroy(sourceFeature)
			}()

			newFeature := C.OGR_F_Create(targetDefn)
			if newFeature == nil {
				errorCount++
				return
			}
			defer C.OGR_F_Destroy(newFeature)

			// 复制几何并进行坐标转换
			geometry := C.OGR_F_GetGeometryRef(sourceFeature)
			if geometry != nil {
				clonedGeom := C.OGR_G_Clone(geometry)
				if clonedGeom != nil {
					defer C.OGR_G_DestroyGeometry(clonedGeom)

					// 执行坐标转换
					if needsTransform {
						result := C.OGR_G_Transform(clonedGeom, transform)
						if result != C.OGRERR_NONE {
							errorCount++
							return
						}
					}

					C.OGR_F_SetGeometry(newFeature, clonedGeom)
				}
			}

			// 复制字段
			if err := w.copyFieldsSafely(sourceFeature, newFeature); err != nil {
				errorCount++
				return
			}

			// 添加要素
			result := C.OGR_L_CreateFeature(targetLayer, newFeature)
			if result != C.OGRERR_NONE {
				errorCount++
				return
			}

			successCount++
		}()
	}

	return nil
}

// ReadGeoJSONLayer 直接读取GeoJSON图层
func ReadGeoJSONLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadGeoJSONFile(layerName...)
}

// ReadDXFLayer 直接读取DXF图层
func ReadDXFLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadDXFFile(layerName...)
}

// ReadKMLLayer 直接读取KML图层
func ReadKMLLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadKMLFile(layerName...)
}

// ReadKMZLayer 直接读取KMZ图层
func ReadKMZLayer(filePath string, layerName ...string) (*GDALLayer, error) {
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return nil, err
	}
	return reader.ReadKMZFile(layerName...)
}

// WriteGeoJSONLayer 直接写入GeoJSON图层
func WriteGeoJSONLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteGeoJSONFile(sourceLayer, layerName)
}

// WriteDXFLayer 直接写入DXF图层
func WriteDXFLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteDXFFile(sourceLayer, layerName)
}

// WriteKMLLayer 直接写入KML图层
func WriteKMLLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteKMLFile(sourceLayer, layerName)
}

// WriteKMZLayer 直接写入KMZ图层
func WriteKMZLayer(sourceLayer *GDALLayer, filePath string, layerName string, overwrite bool) error {
	writer, err := NewFileGeoWriter(filePath, overwrite)
	if err != nil {
		return err
	}
	return writer.WriteKMZFile(sourceLayer, layerName)
}
