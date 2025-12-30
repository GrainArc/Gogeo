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
#include <stdlib.h>
#include <string.h>

// 使用GDALOpenEx打开数据集
static GDALDatasetH openDatasetEx(const char* path, unsigned int flags) {
    return GDALOpenEx(path, flags, NULL, NULL, NULL);
}

*/
import "C"
import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"
)

// GDBLayerFieldInfo 图层字段信息
type GDBLayerFieldInfo struct {
	Name       string `json:"name" xml:"Name"`                     // 字段名称
	AliasName  string `json:"alias_name" xml:"AliasName"`          // 字段别名（中文名）
	FieldType  string `json:"field_type" xml:"FieldType"`          // 字段类型
	IsNullable bool   `json:"is_nullable" xml:"IsNullable"`        // 是否可空
	Length     int    `json:"length,omitempty" xml:"Length"`       // 字段长度（字符串类型）
	Precision  int    `json:"precision,omitempty" xml:"Precision"` // 精度（数值类型）
	Scale      int    `json:"scale,omitempty" xml:"Scale"`         // 小数位数
}

// GDBLayerMetaData 图层元数据
type GDBLayerMetaData struct {
	// 基本信息
	Name        string `json:"name"`         // 图层名称
	AliasName   string `json:"alias_name"`   // 图层别名
	Path        string `json:"path"`         // 图层路径
	CatalogPath string `json:"catalog_path"` // 目录路径
	UUID        string `json:"uuid"`         // 唯一标识
	TypeUUID    string `json:"type_uuid"`    // 类型UUID
	// 数据集信息
	DatasetName string `json:"dataset_name"` // 所属要素数据集名称
	DatasetType string `json:"dataset_type"` // 数据集类型 (esriDTFeatureClass等)
	DSID        int    `json:"dsid"`         // 数据集ID
	FeatureType string `json:"feature_type"` // 要素类型 (esriFTSimple等)
	// 几何信息
	ShapeType       string `json:"shape_type"`        // 几何类型 (esriGeometryPoint/Polygon/Polyline等)
	ShapeFieldName  string `json:"shape_field_name"`  // 几何字段名
	HasM            bool   `json:"has_m"`             // 是否有M值
	HasZ            bool   `json:"has_z"`             // 是否有Z值
	HasSpatialIndex bool   `json:"has_spatial_index"` // 是否有空间索引
	// OID信息
	HasOID       bool   `json:"has_oid"`        // 是否有OID
	OIDFieldName string `json:"oid_field_name"` // OID字段名
	// GlobalID信息
	HasGlobalID       bool   `json:"has_global_id"`        // 是否有GlobalID
	GlobalIDFieldName string `json:"global_id_field_name"` // GlobalID字段名
	// 字段信息
	Fields []GDBLayerFieldInfo `json:"fields"` // 字段列表
}

// GDBLayerMetadataCollection GDB图层元数据集合
type GDBLayerMetadataCollection struct {
	GDBPath string              `json:"gdb_path"` // GDB路径
	GDBName string              `json:"gdb_name"` // GDB名称
	Layers  []*GDBLayerMetaData `json:"layers"`   // 图层列表
	// 按数据集分组
	DatasetGroups map[string][]*GDBLayerMetaData `json:"dataset_groups"` // 数据集名称 -> 图层列表
	// 统计信息
	TotalLayers int `json:"total_layers"` // 总图层数
}

// DEFeatureClassInfo XML解析结构 - 用于解析definition字段
type DEFeatureClassInfo struct {
	XMLName           xml.Name       `xml:"DEFeatureClassInfo"`
	CatalogPath       string         `xml:"CatalogPath"`
	Name              string         `xml:"Name"`
	DatasetType       string         `xml:"DatasetType"`
	DSID              string         `xml:"DSID"`
	HasOID            string         `xml:"HasOID"`
	OIDFieldName      string         `xml:"OIDFieldName"`
	AliasName         string         `xml:"AliasName"`
	HasGlobalID       string         `xml:"HasGlobalID"`
	GlobalIDFieldName string         `xml:"GlobalIDFieldName"`
	FeatureType       string         `xml:"FeatureType"`
	ShapeType         string         `xml:"ShapeType"`
	ShapeFieldName    string         `xml:"ShapeFieldName"`
	HasM              string         `xml:"HasM"`
	HasZ              string         `xml:"HasZ"`
	HasSpatialIndex   string         `xml:"HasSpatialIndex"`
	GPFieldInfoExs    GPFieldInfoExs `xml:"GPFieldInfoExs"`
}

// GPFieldInfoExs 字段信息数组
type GPFieldInfoExs struct {
	GPFieldInfoEx []GPFieldInfoEx `xml:"GPFieldInfoEx"`
}

// GPFieldInfoEx 单个字段信息
type GPFieldInfoEx struct {
	Name       string `xml:"Name"`
	AliasName  string `xml:"AliasName"`
	ModelName  string `xml:"ModelName"`
	FieldType  string `xml:"FieldType"`
	IsNullable string `xml:"IsNullable"`
	Length     string `xml:"Length"`
	Precision  string `xml:"Precision"`
	Scale      string `xml:"Scale"`
	Required   string `xml:"Required"`
	Editable   string `xml:"Editable"`
}

// ReadGDBLayerMetadata 读取GDB文件的所有图层元数据
// gdbPath: GDB文件路径
// 返回: GDBLayerMetadataCollection 包含所有图层的元数据信息
func ReadGDBLayerMetadata(gdbPath string) (*GDBLayerMetadataCollection, error) {
	// 初始化GDAL
	InitializeGDAL()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	// 打开GDB数据集
	hDS := C.openDatasetEx(cPath, C.uint(0x04|0x00)) // GDAL_OF_VECTOR | GDAL_OF_READONLY
	if hDS == nil {
		return nil, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 创建结果集合
	collection := &GDBLayerMetadataCollection{
		GDBPath:       gdbPath,
		GDBName:       filepath.Base(gdbPath),
		Layers:        make([]*GDBLayerMetaData, 0),
		DatasetGroups: make(map[string][]*GDBLayerMetaData),
	}

	// 从GDB_Items表提取元数据
	itemsData, err := extractGDBItemsMetadata(hDS)
	if err != nil {
		return nil, fmt.Errorf("提取GDB_Items元数据失败: %w", err)
	}

	// 解析每个图层的元数据
	for _, item := range itemsData {
		// 只处理要素类（FeatureClass）
		if !isFeatureClassItem(item) {
			continue
		}

		layerMeta, err := parseLayerMetadata(item)
		if err != nil {
			fmt.Printf("警告: 解析图层 %s 元数据失败: %v\n", item["Name"], err)
			continue
		}

		collection.Layers = append(collection.Layers, layerMeta)

		// 按数据集分组
		datasetName := layerMeta.DatasetName
		if datasetName == "" {
			datasetName = "_standalone_" // 独立图层
		}
		if _, exists := collection.DatasetGroups[datasetName]; !exists {
			collection.DatasetGroups[datasetName] = make([]*GDBLayerMetaData, 0)
		}
		collection.DatasetGroups[datasetName] = append(collection.DatasetGroups[datasetName], layerMeta)
	}

	collection.TotalLayers = len(collection.Layers)

	return collection, nil
}

// extractGDBItemsMetadata 从GDB_Items表提取元数据
func extractGDBItemsMetadata(hDS C.GDALDatasetH) ([]map[string]string, error) {
	// 使用SQL查询获取GDB_Items表数据
	sql := "SELECT * FROM GDB_Items"
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	hResultLayer := C.GDALDatasetExecuteSQL(hDS, cSQL, nil, nil)
	if hResultLayer == nil {
		return nil, fmt.Errorf("SQL查询失败，未找到GDB_Items表")
	}
	defer C.GDALDatasetReleaseResultSet(hDS, hResultLayer)

	// 获取字段定义
	defn := C.OGR_L_GetLayerDefn(hResultLayer)
	if defn == nil {
		return nil, fmt.Errorf("无法获取表定义")
	}

	fieldCount := int(C.OGR_FD_GetFieldCount(defn))
	fieldNames := make([]string, fieldCount)
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(defn, C.int(i))
		if fieldDefn != nil {
			fieldNames[i] = C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		}
	}

	// 读取所有记录
	records := make([]map[string]string, 0)
	C.OGR_L_ResetReading(hResultLayer)

	for {
		hFeature := C.OGR_L_GetNextFeature(hResultLayer)
		if hFeature == nil {
			break
		}

		record := make(map[string]string)
		for i := 0; i < fieldCount; i++ {
			if C.OGR_F_IsFieldSet(hFeature, C.int(i)) != 0 && C.OGR_F_IsFieldNull(hFeature, C.int(i)) == 0 {
				fieldValue := C.OGR_F_GetFieldAsString(hFeature, C.int(i))
				if fieldValue != nil {
					record[fieldNames[i]] = C.GoString(fieldValue)
				}
			}
		}
		records = append(records, record)
		C.OGR_F_Destroy(hFeature)
	}

	return records, nil
}

// isFeatureClassItem 判断是否为要素类项
func isFeatureClassItem(item map[string]string) bool {
	definition := item["Definition"]
	if definition == "" {
		return false
	}
	// 检查是否包含DEFeatureClassInfo标签
	return strings.Contains(definition, "DEFeatureClassInfo")
}

// parseLayerMetadata 解析单个图层的元数据
func parseLayerMetadata(item map[string]string) (*GDBLayerMetaData, error) {
	layerMeta := &GDBLayerMetaData{
		Name:     item["Name"],
		Path:     item["Path"],
		UUID:     item["UUID"],
		TypeUUID: item["Type"],
		Fields:   make([]GDBLayerFieldInfo, 0),
	}

	// 从Path中提取数据集名称
	layerMeta.DatasetName = extractDatasetNameFromPath(item["Path"])

	// 解析Definition XML
	definition := item["Definition"]
	if definition != "" {
		err := parseDefinitionXML(definition, layerMeta)
		if err != nil {
			return layerMeta, fmt.Errorf("解析Definition XML失败: %w", err)
		}
	}

	return layerMeta, nil
}

// extractDatasetNameFromPath 从路径中提取数据集名称
// 例如: \XHDataset\YLJGA -> XHDataset
func extractDatasetNameFromPath(path string) string {
	if path == "" {
		return ""
	}

	// 移除开头的反斜杠
	path = strings.TrimPrefix(path, "\\")
	path = strings.TrimPrefix(path, "/")

	// 分割路径
	parts := strings.Split(path, "\\")
	if len(parts) < 2 {
		parts = strings.Split(path, "/")
	}

	// 如果路径有多个部分，第一个部分是数据集名称
	if len(parts) >= 2 {
		return parts[0]
	}

	return ""
}

// parseDefinitionXML 解析Definition字段的XML内容
func parseDefinitionXML(definition string, layerMeta *GDBLayerMetaData) error {
	// 预处理XML - 处理命名空间
	definition = preprocessXML(definition)

	var deInfo DEFeatureClassInfo
	err := xml.Unmarshal([]byte(definition), &deInfo)
	if err != nil {
		// 尝试使用备用解析方法
		return parseDefinitionXMLFallback(definition, layerMeta)
	}

	// 填充基本信息
	layerMeta.CatalogPath = deInfo.CatalogPath
	layerMeta.AliasName = deInfo.AliasName
	layerMeta.DatasetType = deInfo.DatasetType
	layerMeta.DSID = parseIntSafe(deInfo.DSID)
	layerMeta.FeatureType = deInfo.FeatureType
	layerMeta.ShapeType = deInfo.ShapeType
	layerMeta.ShapeFieldName = deInfo.ShapeFieldName
	layerMeta.HasM = parseBoolSafe(deInfo.HasM)
	layerMeta.HasZ = parseBoolSafe(deInfo.HasZ)
	layerMeta.HasSpatialIndex = parseBoolSafe(deInfo.HasSpatialIndex)
	layerMeta.HasOID = parseBoolSafe(deInfo.HasOID)
	layerMeta.OIDFieldName = deInfo.OIDFieldName
	layerMeta.HasGlobalID = parseBoolSafe(deInfo.HasGlobalID)
	layerMeta.GlobalIDFieldName = deInfo.GlobalIDFieldName

	// 解析字段信息
	for _, fieldInfo := range deInfo.GPFieldInfoExs.GPFieldInfoEx {
		field := GDBLayerFieldInfo{
			Name:       fieldInfo.Name,
			AliasName:  fieldInfo.AliasName,
			FieldType:  fieldInfo.FieldType,
			IsNullable: parseBoolSafe(fieldInfo.IsNullable),
			Length:     parseIntSafe(fieldInfo.Length),
			Precision:  parseIntSafe(fieldInfo.Precision),
			Scale:      parseIntSafe(fieldInfo.Scale),
		}

		// 如果别名为空，使用字段名作为别名
		if field.AliasName == "" {
			field.AliasName = field.Name
		}

		layerMeta.Fields = append(layerMeta.Fields, field)
	}

	return nil
}

// preprocessXML 预处理XML字符串
func preprocessXML(xmlStr string) string {
	// 移除XML声明（如果有）
	if idx := strings.Index(xmlStr, "?>"); idx != -1 {
		xmlStr = xmlStr[idx+2:]
	}

	// 处理命名空间前缀
	xmlStr = strings.TrimSpace(xmlStr)

	return xmlStr
}

// parseDefinitionXMLFallback 备用XML解析方法（使用字符串解析）
func parseDefinitionXMLFallback(definition string, layerMeta *GDBLayerMetaData) error {
	// 提取AliasName
	layerMeta.AliasName = extractXMLValue(definition, "AliasName")
	layerMeta.CatalogPath = extractXMLValue(definition, "CatalogPath")
	layerMeta.DatasetType = extractXMLValue(definition, "DatasetType")
	layerMeta.DSID = parseIntSafe(extractXMLValue(definition, "DSID"))
	layerMeta.FeatureType = extractXMLValue(definition, "FeatureType")
	layerMeta.ShapeType = extractXMLValue(definition, "ShapeType")
	layerMeta.ShapeFieldName = extractXMLValue(definition, "ShapeFieldName")
	layerMeta.HasM = parseBoolSafe(extractXMLValue(definition, "HasM"))
	layerMeta.HasZ = parseBoolSafe(extractXMLValue(definition, "HasZ"))
	layerMeta.HasSpatialIndex = parseBoolSafe(extractXMLValue(definition, "HasSpatialIndex"))
	layerMeta.HasOID = parseBoolSafe(extractXMLValue(definition, "HasOID"))
	layerMeta.OIDFieldName = extractXMLValue(definition, "OIDFieldName")
	layerMeta.HasGlobalID = parseBoolSafe(extractXMLValue(definition, "HasGlobalID"))
	layerMeta.GlobalIDFieldName = extractXMLValue(definition, "GlobalIDFieldName")

	// 解析字段信息
	layerMeta.Fields = parseFieldInfosFallback(definition)

	return nil
}

// extractXMLValue 从XML字符串中提取指定标签的值
func extractXMLValue(xmlStr, tagName string) string {
	startTag := "<" + tagName + ">"
	endTag := "</" + tagName + ">"

	startIdx := strings.Index(xmlStr, startTag)
	if startIdx == -1 {
		// 尝试带属性的标签
		startTag = "<" + tagName + " "
		startIdx = strings.Index(xmlStr, startTag)
		if startIdx == -1 {
			return ""
		}
		// 找到标签结束位置
		closeIdx := strings.Index(xmlStr[startIdx:], ">")
		if closeIdx == -1 {
			return ""
		}
		startIdx = startIdx + closeIdx + 1
	} else {
		startIdx += len(startTag)
	}

	endIdx := strings.Index(xmlStr[startIdx:], endTag)
	if endIdx == -1 {
		return ""
	}

	return strings.TrimSpace(xmlStr[startIdx : startIdx+endIdx])
}

// parseFieldInfosFallback 备用字段解析方法
func parseFieldInfosFallback(definition string) []GDBLayerFieldInfo {
	fields := make([]GDBLayerFieldInfo, 0)

	// 查找所有GPFieldInfoEx块
	searchStr := definition
	for {
		startTag := "<GPFieldInfoEx"
		endTag := "</GPFieldInfoEx>"

		startIdx := strings.Index(searchStr, startTag)
		if startIdx == -1 {
			break
		}

		endIdx := strings.Index(searchStr[startIdx:], endTag)
		if endIdx == -1 {
			break
		}

		fieldXML := searchStr[startIdx : startIdx+endIdx+len(endTag)]

		field := GDBLayerFieldInfo{
			Name:       extractXMLValue(fieldXML, "Name"),
			AliasName:  extractXMLValue(fieldXML, "AliasName"),
			FieldType:  extractXMLValue(fieldXML, "FieldType"),
			IsNullable: parseBoolSafe(extractXMLValue(fieldXML, "IsNullable")),
			Length:     parseIntSafe(extractXMLValue(fieldXML, "Length")),
			Precision:  parseIntSafe(extractXMLValue(fieldXML, "Precision")),
			Scale:      parseIntSafe(extractXMLValue(fieldXML, "Scale")),
		}

		// 如果别名为空，使用字段名作为别名
		if field.AliasName == "" {
			field.AliasName = field.Name
		}

		fields = append(fields, field)

		// 继续搜索下一个
		searchStr = searchStr[startIdx+endIdx+len(endTag):]
	}

	return fields
}

// parseIntSafe 安全解析整数
func parseIntSafe(s string) int {
	if s == "" {
		return 0
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return val
}

// parseBoolSafe 安全解析布尔值
func parseBoolSafe(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes"
}

// ReadGDBLayerMetadataByName 读取指定图层的元数据
func ReadGDBLayerMetadataByName(gdbPath string, layerName string) (*GDBLayerMetaData, error) {
	collection, err := ReadGDBLayerMetadata(gdbPath)
	if err != nil {
		return nil, err
	}

	for _, layer := range collection.Layers {
		if layer.Name == layerName {
			return layer, nil
		}
	}

	return nil, fmt.Errorf("未找到图层: %s", layerName)
}

// GetFieldAliasMap 获取字段别名映射 (字段名 -> 别名)
func (m *GDBLayerMetaData) GetFieldAliasMap() map[string]string {
	aliasMap := make(map[string]string)
	for _, field := range m.Fields {
		aliasMap[field.Name] = field.AliasName
	}
	return aliasMap
}

// GetFieldByName 根据字段名获取字段信息
func (m *GDBLayerMetaData) GetFieldByName(fieldName string) *GDBLayerFieldInfo {
	for i := range m.Fields {
		if m.Fields[i].Name == fieldName {
			return &m.Fields[i]
		}
	}
	return nil
}

// GetFieldByAlias 根据别名获取字段信息
func (m *GDBLayerMetaData) GetFieldByAlias(aliasName string) *GDBLayerFieldInfo {
	for i := range m.Fields {
		if m.Fields[i].AliasName == aliasName {
			return &m.Fields[i]
		}
	}
	return nil
}

// PrintMetadata 打印图层元数据信息
func (m *GDBLayerMetaData) PrintMetadata() {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("图层名称: %s\n", m.Name)
	fmt.Printf("图层别名: %s\n", m.AliasName)
	fmt.Printf("图层路径: %s\n", m.Path)
	fmt.Printf("目录路径: %s\n", m.CatalogPath)
	fmt.Printf("所属数据集: %s\n", m.DatasetName)
	fmt.Printf("数据集类型: %s\n", m.DatasetType)
	fmt.Printf("要素类型: %s\n", m.FeatureType)
	fmt.Printf("几何类型: %s\n", m.ShapeType)
	fmt.Printf("几何字段: %s\n", m.ShapeFieldName)
	fmt.Printf("HasZ: %v, HasM: %v\n", m.HasZ, m.HasM)
	fmt.Printf("OID字段: %s (HasOID: %v)\n", m.OIDFieldName, m.HasOID)

	fmt.Printf("\n字段列表 (%d 个):\n", len(m.Fields))
	fmt.Printf("%-4s %-20s %-25s %-20s %-8s\n", "序号", "字段名", "别名", "类型", "可空")
	fmt.Println(strings.Repeat("-", 85))

	for i, field := range m.Fields {
		fmt.Printf("%-4d %-20s %-25s %-20s %-8v\n",
			i+1, field.Name, field.AliasName, field.FieldType, field.IsNullable)
	}
	fmt.Println(strings.Repeat("=", 80))
}

// PrintCollection 打印元数据集合信息
func (c *GDBLayerMetadataCollection) PrintCollection() {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("GDB文件: %s\n", c.GDBPath)
	fmt.Printf("GDB名称: %s\n", c.GDBName)
	fmt.Printf("总图层数: %d\n", c.TotalLayers)
	fmt.Println(strings.Repeat("=", 80))

	// 按数据集分组打印
	fmt.Printf("\n按数据集分组:\n")
	for datasetName, layers := range c.DatasetGroups {
		displayName := datasetName
		if datasetName == "_standalone_" {
			displayName = "独立图层"
		}
		fmt.Printf("\n[%s] (%d 个图层)\n", displayName, len(layers))
		for _, layer := range layers {
			fmt.Printf("  - %s (%s) - %s\n", layer.Name, layer.AliasName, layer.ShapeType)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
}

// GetLayerByName 根据图层名获取图层元数据
func (c *GDBLayerMetadataCollection) GetLayerByName(layerName string) *GDBLayerMetaData {
	for _, layer := range c.Layers {
		if layer.Name == layerName {
			return layer
		}
	}
	return nil
}

// GetLayerByAlias 根据图层别名获取图层元数据
func (c *GDBLayerMetadataCollection) GetLayerByAlias(aliasName string) *GDBLayerMetaData {
	for _, layer := range c.Layers {
		if layer.AliasName == aliasName {
			return layer
		}
	}
	return nil
}

// GetLayersByDataset 获取指定数据集下的所有图层
func (c *GDBLayerMetadataCollection) GetLayersByDataset(datasetName string) []*GDBLayerMetaData {
	if layers, exists := c.DatasetGroups[datasetName]; exists {
		return layers
	}
	return nil
}

// GetAllFieldAliases 获取所有图层的字段别名映射
// 返回: map[图层名]map[字段名]别名
func (c *GDBLayerMetadataCollection) GetAllFieldAliases() map[string]map[string]string {
	result := make(map[string]map[string]string)
	for _, layer := range c.Layers {
		result[layer.Name] = layer.GetFieldAliasMap()
	}
	return result
}

// SaveGDBDefinitionsToFile 读取GDB_Items表中的Definition字段并保存到本地txt文件
// gdbPath: GDB文件路径
// outputPath: 输出文件路径（如果为空，则在GDB同级目录生成）
// 返回: 保存的文件路径和错误信息
func SaveGDBDefinitionsToFile(gdbPath string, outputPath string) (string, error) {
	// 初始化GDAL
	InitializeGDAL()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	// 打开GDB数据集
	hDS := C.openDatasetEx(cPath, C.uint(0x04|0x00)) // GDAL_OF_VECTOR | GDAL_OF_READONLY
	if hDS == nil {
		return "", fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 确定输出路径
	if outputPath == "" {
		gdbDir := filepath.Dir(gdbPath)
		gdbName := strings.TrimSuffix(filepath.Base(gdbPath), filepath.Ext(gdbPath))
		outputPath = filepath.Join(gdbDir, gdbName+"_definitions.txt")
	}

	// 使用SQL查询获取GDB_Items表数据
	sql := "SELECT Name, Path, Definition FROM GDB_Items"
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	hResultLayer := C.GDALDatasetExecuteSQL(hDS, cSQL, nil, nil)
	if hResultLayer == nil {
		return "", fmt.Errorf("SQL查询失败，未找到GDB_Items表")
	}
	defer C.GDALDatasetReleaseResultSet(hDS, hResultLayer)

	// 构建输出内容
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("GDB Path: %s\n", gdbPath))
	sb.WriteString(strings.Repeat("=", 80) + "\n\n")

	// 读取所有记录
	C.OGR_L_ResetReading(hResultLayer)
	itemIndex := 0

	for {
		hFeature := C.OGR_L_GetNextFeature(hResultLayer)
		if hFeature == nil {
			break
		}

		itemIndex++
		name := ""
		path := ""
		definition := ""

		// 读取Name
		if C.OGR_F_IsFieldSet(hFeature, 0) != 0 && C.OGR_F_IsFieldNull(hFeature, 0) == 0 {
			name = C.GoString(C.OGR_F_GetFieldAsString(hFeature, 0))
		}

		// 读取Path
		if C.OGR_F_IsFieldSet(hFeature, 1) != 0 && C.OGR_F_IsFieldNull(hFeature, 1) == 0 {
			path = C.GoString(C.OGR_F_GetFieldAsString(hFeature, 1))
		}

		// 读取Definition
		if C.OGR_F_IsFieldSet(hFeature, 2) != 0 && C.OGR_F_IsFieldNull(hFeature, 2) == 0 {
			definition = C.GoString(C.OGR_F_GetFieldAsString(hFeature, 2))
		}

		// 写入文件内容
		sb.WriteString(fmt.Sprintf("[%d] Name: %s\n", itemIndex, name))
		sb.WriteString(fmt.Sprintf("    Path: %s\n", path))
		sb.WriteString("    Definition:\n")
		if definition != "" {
			sb.WriteString(definition)
		} else {
			sb.WriteString("    (empty)")
		}
		sb.WriteString("\n\n")
		sb.WriteString(strings.Repeat("-", 80) + "\n\n")

		C.OGR_F_Destroy(hFeature)
	}

	// 写入文件
	err := os.WriteFile(outputPath, []byte(sb.String()), 0644)
	if err != nil {
		return "", fmt.Errorf("写入文件失败: %w", err)
	}

	return outputPath, nil
}
