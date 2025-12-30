package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
#include <string.h>

static GDALDatasetH openDatasetExUpdate(const char* path, unsigned int flags) {
    return GDALOpenEx(path, flags, NULL, NULL, NULL);
}
static void flushCache(GDALDatasetH hDS) {
    GDALFlushCache(hDS);
}
*/
import "C"
import (
	"crypto/rand"
	"fmt"
	"strings"
	"unsafe"
)

// =====================================================
// 要素数据集相关结构和常量
// =====================================================

// GDBFeatureDatasetMetadata 要素数据集元数据
type GDBFeatureDatasetMetadata struct {
	Name             string                    // 数据集名称
	DSID             int                       // 数据集ID
	SpatialReference *GDBSpatialReferenceWrite // 空间参考
}

// NewGDBFeatureDatasetMetadata 创建新的要素数据集元数据
func NewGDBFeatureDatasetMetadata(name string) *GDBFeatureDatasetMetadata {
	return &GDBFeatureDatasetMetadata{
		Name: name,
		DSID: 0, // 将在创建时自动分配
	}
}

// WithSpatialReference 设置空间参考
func (m *GDBFeatureDatasetMetadata) WithSpatialReference(sr *GDBSpatialReferenceWrite) *GDBFeatureDatasetMetadata {
	m.SpatialReference = sr
	return m
}

// WithSpatialReferenceFromGDB 从GDBSpatialReference设置空间参考
func (m *GDBFeatureDatasetMetadata) WithSpatialReferenceFromGDB(srs *GDBSpatialReference) *GDBFeatureDatasetMetadata {
	m.SpatialReference = ConvertGDBSpatialReferenceToWrite(srs)
	return m
}

// =====================================================
// GDBSpatialReference 到 GDBSpatialReferenceWrite 转换
// =====================================================

// ConvertGDBSpatialReferenceToWrite 将GDBSpatialReference转换为GDBSpatialReferenceWrite
func ConvertGDBSpatialReferenceToWrite(srs *GDBSpatialReference) *GDBSpatialReferenceWrite {
	if srs == nil {
		return nil
	}

	sr := NewGDBSpatialReferenceWrite()
	sr.WKID = srs.EPSG
	sr.LatestWKID = srs.EPSG
	sr.IsProjected = srs.Type == SRSTypeProjected

	// 根据EPSG获取WKT
	if srs.WKT != "" {
		sr.WKT = srs.WKT
	} else if srs.EPSG > 0 {
		// 使用GDAL获取WKT
		wkt, err := GetWKTFromEPSG(srs.EPSG)
		if err == nil {
			sr.WKT = wkt
		}
	}

	// 根据坐标系类型设置默认参数
	if sr.IsProjected {
		// 投影坐标系默认参数
		sr.XOrigin = -5123200
		sr.YOrigin = -9998100
		sr.XYScale = 10000
		sr.XYTolerance = 0.001
	} else {
		// 地理坐标系默认参数
		sr.XOrigin = -400
		sr.YOrigin = -400
		sr.XYScale = 999999999.99999988
		sr.XYTolerance = 8.9831528411952133e-09
	}

	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true

	return sr
}

// GetWKTFromEPSG 从EPSG代码获取WKT字符串
func GetWKTFromEPSG(epsg int) (string, error) {
	InitializeGDAL()

	hSRS := C.OSRNewSpatialReference(nil)
	if hSRS == nil {
		return "", fmt.Errorf("无法创建空间参考对象")
	}
	defer C.OSRDestroySpatialReference(hSRS)

	if C.OSRImportFromEPSG(hSRS, C.int(epsg)) != 0 {
		return "", fmt.Errorf("无法从EPSG %d导入空间参考", epsg)
	}

	var pszWKT *C.char
	C.OSRExportToWkt(hSRS, &pszWKT)
	if pszWKT == nil {
		return "", fmt.Errorf("无法导出WKT")
	}
	defer C.CPLFree(unsafe.Pointer(pszWKT))

	return C.GoString(pszWKT), nil
}

// =====================================================
// 要素数据集Definition XML生成
// =====================================================

// GenerateFeatureDatasetDefinitionXML 生成要素数据集的Definition XML
func (m *GDBFeatureDatasetMetadata) GenerateFeatureDatasetDefinitionXML() string {
	var sb strings.Builder
	// 根元素 - 注意命名空间版本
	sb.WriteString(`<DEFeatureDataset xsi:type="typens:DEFeatureDataset" `)
	sb.WriteString(`xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" `)
	sb.WriteString(`xmlns:xs="http://www.w3.org/2001/XMLSchema" `)
	sb.WriteString(`xmlns:typens="http://www.esri.com/schemas/ArcGIS/10.8">`) // 版本号可能需要调整
	// CatalogPath - 必须是完整路径
	catalogPath := "\\" + m.Name
	sb.WriteString(fmt.Sprintf("<CatalogPath>%s</CatalogPath>", escapeXMLString(catalogPath)))
	sb.WriteString(fmt.Sprintf("<Name>%s</Name>", escapeXMLString(m.Name)))
	sb.WriteString("<ChildrenExpanded>false</ChildrenExpanded>")
	sb.WriteString("<DatasetType>esriDTFeatureDataset</DatasetType>")

	// DSID 必须大于0
	sb.WriteString(fmt.Sprintf("<DSID>%d</DSID>", m.DSID))

	sb.WriteString("<Versioned>false</Versioned>")
	sb.WriteString("<CanVersion>false</CanVersion>")
	sb.WriteString("<ConfigurationKeyword></ConfigurationKeyword>")
	sb.WriteString("<RequiredGeodatabaseClientVersion>10.0</RequiredGeodatabaseClientVersion>")
	sb.WriteString("<HasOID>false</HasOID>") // 添加这个
	// Extent - ArcGIS 可能需要有效的 Extent 而不是 nil
	m.writeExtentForDataset(&sb)
	// SpatialReference - 这是关键部分
	if m.SpatialReference != nil {
		m.writeSpatialReferenceForDataset(&sb)
	}
	sb.WriteString("<ChangeTracked>false</ChangeTracked>")
	sb.WriteString("</DEFeatureDataset>")
	return sb.String()
}
func (m *GDBFeatureDatasetMetadata) writeExtentForDataset(sb *strings.Builder) {
	if m.SpatialReference == nil {
		sb.WriteString("<Extent xsi:nil=\"true\"/>")
		return
	}

	// 写入一个空的 Extent 但带有空间参考
	sb.WriteString("<Extent xsi:type=\"typens:EnvelopeN\">")
	sb.WriteString("<XMin>0</XMin>")
	sb.WriteString("<YMin>0</YMin>")
	sb.WriteString("<XMax>0</XMax>")
	sb.WriteString("<YMax>0</YMax>")
	// Extent 内也需要空间参考
	m.writeSpatialReferenceForDataset(sb)

	sb.WriteString("</Extent>")
}

// writeSpatialReferenceForDataset 写入要素数据集的空间参考
func (m *GDBFeatureDatasetMetadata) writeSpatialReferenceForDataset(sb *strings.Builder) {
	if m.SpatialReference == nil {
		return
	}
	sr := m.SpatialReference
	// 根据是否为投影坐标系选择类型
	srType := "typens:GeographicCoordinateSystem"
	if sr.IsProjected {
		srType = "typens:ProjectedCoordinateSystem"
	}
	sb.WriteString(fmt.Sprintf("<SpatialReference xsi:type=\"%s\">", srType))
	// WKT - 必须正确转义
	if sr.WKT != "" {
		sb.WriteString(fmt.Sprintf("<WKT>%s</WKT>", escapeXMLForWKT(sr.WKT)))
	}
	// 精度参数 - 顺序很重要！
	sb.WriteString(fmt.Sprintf("<XOrigin>%.15g</XOrigin>", sr.XOrigin))
	sb.WriteString(fmt.Sprintf("<YOrigin>%.15g</YOrigin>", sr.YOrigin))
	sb.WriteString(fmt.Sprintf("<XYScale>%.15g</XYScale>", sr.XYScale))
	sb.WriteString(fmt.Sprintf("<ZOrigin>%.15g</ZOrigin>", sr.ZOrigin))
	sb.WriteString(fmt.Sprintf("<ZScale>%.15g</ZScale>", sr.ZScale))
	sb.WriteString(fmt.Sprintf("<MOrigin>%.15g</MOrigin>", sr.MOrigin))
	sb.WriteString(fmt.Sprintf("<MScale>%.15g</MScale>", sr.MScale))
	sb.WriteString(fmt.Sprintf("<XYTolerance>%.15g</XYTolerance>", sr.XYTolerance))
	sb.WriteString(fmt.Sprintf("<ZTolerance>%.15g</ZTolerance>", sr.ZTolerance))
	sb.WriteString(fmt.Sprintf("<MTolerance>%.15g</MTolerance>", sr.MTolerance))
	sb.WriteString(fmt.Sprintf("<HighPrecision>%s</HighPrecision>", boolToString(sr.HighPrecision)))
	// 地理坐标系特有参数
	if !sr.IsProjected {
		sb.WriteString("<LeftLongitude>-180</LeftLongitude>")
	}
	// WKID - 必须在 LatestWKID 之前
	if sr.WKID > 0 {
		sb.WriteString(fmt.Sprintf("<WKID>%d</WKID>", sr.WKID))
	}
	if sr.LatestWKID > 0 {
		sb.WriteString(fmt.Sprintf("<LatestWKID>%d</LatestWKID>", sr.LatestWKID))
	}
	// VCS (垂直坐标系) - 可能需要
	sb.WriteString("<VCSWKID>0</VCSWKID>")
	sb.WriteString("<LatestVCSWKID>0</LatestVCSWKID>")
	sb.WriteString("</SpatialReference>")
}
func escapeXMLForWKT(wkt string) string {
	// 替换特殊字符
	wkt = strings.ReplaceAll(wkt, "&", "&amp;")
	wkt = strings.ReplaceAll(wkt, "<", "&lt;")
	wkt = strings.ReplaceAll(wkt, ">", "&gt;")
	wkt = strings.ReplaceAll(wkt, "\"", "&quot;")
	wkt = strings.ReplaceAll(wkt, "'", "&apos;")
	return wkt
}

// =====================================================
// 要素数据集创建函数
// =====================================================

// generateRandomUUID 生成随机UUID
func generateRandomUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// 如果随机数生成失败，使用时间戳
		return fmt.Sprintf("{%08X-%04X-%04X-%04X-%012X}",
			uint32(0x12345678),
			uint16(0x1234),
			uint16(0x4567),
			uint16(0x89AB),
			uint64(0xCDEF01234567))
	}

	// 设置版本号 (version 4)
	b[6] = (b[6] & 0x0f) | 0x40
	// 设置变体
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("{%08X-%04X-%04X-%04X-%012X}",
		uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
		uint16(b[8])<<8|uint16(b[9]),
		uint64(b[10])<<40|uint64(b[11])<<32|uint64(b[12])<<24|uint64(b[13])<<16|uint64(b[14])<<8|uint64(b[15]))
}

// GetNextDSID 获取下一个可用的DSID
func GetNextDSID(gdbPath string) (int, error) {
	InitializeGDAL()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return 0, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return 0, fmt.Errorf("无法获取GDB_Items表")
	}

	// 查找最大的DSID
	C.OGR_L_ResetReading(hLayer)

	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	cDefinition := C.CString("Definition")
	defer C.free(unsafe.Pointer(cDefinition))

	defIdx := C.OGR_FD_GetFieldIndex(hDefn, cDefinition)

	maxDSID := 0

	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		if defIdx >= 0 {
			defXML := C.GoString(C.OGR_F_GetFieldAsString(hFeature, defIdx))
			// 从Definition XML中提取DSID
			dsid := extractDSIDFromXML(defXML)
			if dsid > maxDSID {
				maxDSID = dsid
			}
		}

		C.OGR_F_Destroy(hFeature)
	}

	return maxDSID + 1, nil
}

// extractDSIDFromXML 从Definition XML中提取DSID
func extractDSIDFromXML(xml string) int {
	// 简单的字符串解析
	startTag := "<DSID>"
	endTag := "</DSID>"

	startIdx := strings.Index(xml, startTag)
	if startIdx == -1 {
		return 0
	}
	startIdx += len(startTag)

	endIdx := strings.Index(xml[startIdx:], endTag)
	if endIdx == -1 {
		return 0
	}

	dsidStr := xml[startIdx : startIdx+endIdx]
	var dsid int
	fmt.Sscanf(dsidStr, "%d", &dsid)
	return dsid
}

// FeatureDatasetExists 检查要素数据集是否存在
func FeatureDatasetExists(gdbPath string, datasetName string) (bool, error) {
	_, err := GetGDBItemUUID(gdbPath, datasetName)
	if err != nil {
		// 如果获取UUID失败，说明不存在
		if strings.Contains(err.Error(), "未找到项目") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CreateFeatureDataset 在GDB中创建要素数据集
func CreateFeatureDataset(gdbPath string, metadata *GDBFeatureDatasetMetadata) error {
	InitializeGDAL()

	// 检查是否已存在
	exists, err := FeatureDatasetExists(gdbPath, metadata.Name)
	if err != nil {
		return fmt.Errorf("检查要素数据集是否存在时出错: %w", err)
	}
	if exists {
		fmt.Printf("要素数据集 '%s' 已存在，跳过创建\n", metadata.Name)
		return nil
	}

	// 获取下一个DSID
	nextDSID, err := GetNextDSID(gdbPath)
	if err != nil {
		return fmt.Errorf("获取下一个DSID失败: %w", err)
	}
	metadata.DSID = nextDSID

	// 生成UUID
	datasetUUID := generateRandomUUID()

	// 生成Definition XML
	definitionXML := metadata.GenerateFeatureDatasetDefinitionXML()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return fmt.Errorf("无法以更新模式打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 获取GDB_Items表
	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return fmt.Errorf("无法获取GDB_Items表")
	}

	// 创建新要素
	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	hFeature := C.OGR_F_Create(hDefn)
	if hFeature == nil {
		return fmt.Errorf("无法创建要素")
	}
	defer C.OGR_F_Destroy(hFeature)

	// 设置字段值的辅助函数
	setStringField := func(fieldName, value string) error {
		cFieldName := C.CString(fieldName)
		defer C.free(unsafe.Pointer(cFieldName))
		idx := C.OGR_FD_GetFieldIndex(hDefn, cFieldName)
		if idx >= 0 {
			cValue := C.CString(value)
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, idx, cValue)
		}
		return nil
	}

	setIntField := func(fieldName string, value int) error {
		cFieldName := C.CString(fieldName)
		defer C.free(unsafe.Pointer(cFieldName))
		idx := C.OGR_FD_GetFieldIndex(hDefn, cFieldName)
		if idx >= 0 {
			C.OGR_F_SetFieldInteger(hFeature, idx, C.int(value))
		}
		return nil
	}

	// 设置各字段
	setStringField("UUID", datasetUUID)
	setStringField("Type", GDBItemTypeFeatureDataset)
	setStringField("Name", metadata.Name)
	setStringField("PhysicalName", strings.ToUpper(metadata.Name))
	setStringField("Path", "\\"+metadata.Name)
	setStringField("Url", "") // 空字符串而不是 NULL
	setStringField("Definition", definitionXML)
	setStringField("Documentation", "") // 空字符串
	setStringField("ItemInfo", "")      // 空字符串
	setIntField("Properties", 1)
	setStringField("Defaults", "")        // 空字符串
	setStringField("DatasetSubtype1", "") // 可能需要
	setStringField("DatasetSubtype2", "") // 可能需要
	setStringField("DatasetInfo1", "")    // 可能需要
	setStringField("DatasetInfo2", "")    // 可能需要

	// 创建要素
	if C.OGR_L_CreateFeature(hLayer, hFeature) != 0 {
		return fmt.Errorf("创建要素数据集记录失败")
	}

	C.OGR_L_SyncToDisk(hLayer)

	// 获取根目录UUID并创建关系
	rootUUID, err := GetGDBRootUUID(gdbPath)
	if err != nil {
		fmt.Printf("警告: 获取根目录UUID失败: %v\n", err)
	} else {
		// 添加要素数据集到根目录的关系
		err = AddGDBItemRelationship(gdbPath, rootUUID, datasetUUID, GDBRelTypeDatasetInFolder)
		if err != nil {
			fmt.Printf("警告: 添加要素数据集关系失败: %v\n", err)
		}
	}

	C.GDALFlushCache(hDS)

	fmt.Printf("成功创建要素数据集: %s (DSID: %d, UUID: %s)\n", metadata.Name, metadata.DSID, datasetUUID)
	return nil
}

// CreateFeatureDatasetWithSRS 使用GDBSpatialReference创建要素数据集
func CreateFeatureDatasetWithSRS(gdbPath string, datasetName string, srs *GDBSpatialReference) error {
	metadata := NewGDBFeatureDatasetMetadata(datasetName)
	metadata.WithSpatialReferenceFromGDB(srs)
	return CreateFeatureDataset(gdbPath, metadata)
}

// CreateFeatureDatasetWithSRSWrite 使用GDBSpatialReferenceWrite创建要素数据集
func CreateFeatureDatasetWithSRSWrite(gdbPath string, datasetName string, sr *GDBSpatialReferenceWrite) error {
	metadata := NewGDBFeatureDatasetMetadata(datasetName)
	metadata.SpatialReference = sr
	return CreateFeatureDataset(gdbPath, metadata)
}

// EnsureFeatureDatasetExists 确保要素数据集存在，如果不存在则创建
func EnsureFeatureDatasetExists(gdbPath string, datasetName string, srs *GDBSpatialReference) error {
	exists, err := FeatureDatasetExists(gdbPath, datasetName)
	if err != nil {
		return err
	}

	if !exists {
		fmt.Printf("要素数据集 '%s' 不存在，正在创建...\n", datasetName)
		return CreateFeatureDatasetWithSRS(gdbPath, datasetName, srs)
	}

	fmt.Printf("要素数据集 '%s' 已存在\n", datasetName)
	return nil
}

// =====================================================
// 更新 ImportPostGISToNewGDBLayerV3 以支持自动创建要素数据集
// =====================================================

// ImportPostGISToNewGDBLayerV3WithDataset 将PostGIS数据表导入到GDB文件，自动创建要素数据集
func ImportPostGISToNewGDBLayerV3WithDataset(postGISConfig *PostGISConfig, gdbPath string, layerName string, options *ImportToGDBOptionsV3) (*ImportResult, error) {
	// 如果指定了LayerPath，检查并创建要素数据集
	if options.LayerPath != "" {
		datasetName := extractDatasetName(options.LayerPath)
		if datasetName != "" {
			// 确定要使用的空间参考
			var srs *GDBSpatialReference
			if options.TargetSRS != nil {
				srs = options.TargetSRS
			} else {
				// 默认使用CGCS2000
				srs = SRS_CGCS2000
			}

			// 确保要素数据集存在
			err := EnsureFeatureDatasetExists(gdbPath, datasetName, srs)
			if err != nil {
				return nil, fmt.Errorf("创建要素数据集失败: %w", err)
			}
		}
	}

	// 调用原有的导入函数
	return ImportPostGISToNewGDBLayerV3(postGISConfig, gdbPath, layerName, options)
}

// =====================================================
// 便捷函数
// =====================================================

// QuickCreateFeatureDataset 快速创建要素数据集（使用CGCS2000坐标系）
func QuickCreateFeatureDataset(gdbPath string, datasetName string) error {
	return CreateFeatureDatasetWithSRS(gdbPath, datasetName, SRS_CGCS2000)
}

// QuickCreateFeatureDatasetWithEPSG 使用EPSG代码快速创建要素数据集
func QuickCreateFeatureDatasetWithEPSG(gdbPath string, datasetName string, epsg int) error {
	srs := NewGDBSpatialReferenceFromEPSG(epsg)
	return CreateFeatureDatasetWithSRS(gdbPath, datasetName, srs)
}

// ListFeatureDatasets 列出GDB中的所有要素数据集
func ListFeatureDatasets(gdbPath string) ([]string, error) {
	InitializeGDAL()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return nil, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return nil, fmt.Errorf("无法获取GDB_Items表")
	}

	// 过滤要素数据集类型
	filterSQL := fmt.Sprintf("Type = '%s'", GDBItemTypeFeatureDataset)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	cName := C.CString("Name")
	defer C.free(unsafe.Pointer(cName))
	nameIdx := C.OGR_FD_GetFieldIndex(hDefn, cName)

	var datasets []string

	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		if nameIdx >= 0 {
			name := C.GoString(C.OGR_F_GetFieldAsString(hFeature, nameIdx))
			datasets = append(datasets, name)
		}

		C.OGR_F_Destroy(hFeature)
	}

	C.OGR_L_SetAttributeFilter(hLayer, nil)

	return datasets, nil
}

// GetFeatureDatasetInfo 获取要素数据集的详细信息
func GetFeatureDatasetInfo(gdbPath string, datasetName string) (*GDBFeatureDatasetMetadata, error) {
	InitializeGDAL()

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return nil, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return nil, fmt.Errorf("无法获取GDB_Items表")
	}

	// 过滤指定名称
	filterSQL := fmt.Sprintf("Name = '%s' AND Type = '%s'", datasetName, GDBItemTypeFeatureDataset)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	hFeature := C.OGR_L_GetNextFeature(hLayer)
	if hFeature == nil {
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		return nil, fmt.Errorf("未找到要素数据集: %s", datasetName)
	}
	defer C.OGR_F_Destroy(hFeature)

	hDefn := C.OGR_L_GetLayerDefn(hLayer)

	// 获取Definition字段
	cDefinition := C.CString("Definition")
	defer C.free(unsafe.Pointer(cDefinition))
	defIdx := C.OGR_FD_GetFieldIndex(hDefn, cDefinition)

	metadata := NewGDBFeatureDatasetMetadata(datasetName)

	if defIdx >= 0 {
		defXML := C.GoString(C.OGR_F_GetFieldAsString(hFeature, defIdx))
		metadata.DSID = extractDSIDFromXML(defXML) // 可以进一步解析空间参考等信息
	}

	C.OGR_L_SetAttributeFilter(hLayer, nil)

	return metadata, nil
}

// DeleteFeatureDataset 删除要素数据集
// 注意：这只会删除GDB_Items中的记录，不会删除数据集中的要素类
func DeleteFeatureDataset(gdbPath string, datasetName string) error {
	InitializeGDAL()

	// 获取要素数据集的UUID
	datasetUUID, err := GetGDBItemUUID(gdbPath, datasetName)
	if err != nil {
		return fmt.Errorf("获取要素数据集UUID失败: %w", err)
	}

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return fmt.Errorf("无法以更新模式打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 删除GDB_Items中的记录
	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return fmt.Errorf("无法获取GDB_Items表")
	}

	filterSQL := fmt.Sprintf("Name = '%s' AND Type = '%s'", datasetName, GDBItemTypeFeatureDataset)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	hFeature := C.OGR_L_GetNextFeature(hLayer)
	if hFeature != nil {
		fid := C.OGR_F_GetFID(hFeature)
		C.OGR_F_Destroy(hFeature)
		C.OGR_L_DeleteFeature(hLayer, fid)
	}

	C.OGR_L_SetAttributeFilter(hLayer, nil)
	C.OGR_L_SyncToDisk(hLayer)

	// 删除GDB_ItemRelationships中的相关记录
	// 删除以该数据集为源的关系（子要素类的关系）
	cGDBItemRels := C.CString("GDB_ItemRelationships")
	defer C.free(unsafe.Pointer(cGDBItemRels))

	hRelLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItemRels)
	if hRelLayer != nil {
		// 删除以该数据集为OriginID的关系
		filterSQL = fmt.Sprintf("OriginID = '%s'", datasetUUID)
		cFilter = C.CString(filterSQL)
		C.OGR_L_SetAttributeFilter(hRelLayer, cFilter)
		C.OGR_L_ResetReading(hRelLayer)
		C.free(unsafe.Pointer(cFilter))

		var fidsToDelete []C.GIntBig
		for {
			hFeature = C.OGR_L_GetNextFeature(hRelLayer)
			if hFeature == nil {
				break
			}
			fidsToDelete = append(fidsToDelete, C.OGR_F_GetFID(hFeature))
			C.OGR_F_Destroy(hFeature)
		}

		for _, fid := range fidsToDelete {
			C.OGR_L_DeleteFeature(hRelLayer, fid)
		}

		// 删除以该数据集为DestID的关系
		filterSQL = fmt.Sprintf("DestID = '%s'", datasetUUID)
		cFilter = C.CString(filterSQL)
		C.OGR_L_SetAttributeFilter(hRelLayer, cFilter)
		C.OGR_L_ResetReading(hRelLayer)
		C.free(unsafe.Pointer(cFilter))

		fidsToDelete = nil
		for {
			hFeature = C.OGR_L_GetNextFeature(hRelLayer)
			if hFeature == nil {
				break
			}
			fidsToDelete = append(fidsToDelete, C.OGR_F_GetFID(hFeature))
			C.OGR_F_Destroy(hFeature)
		}

		for _, fid := range fidsToDelete {
			C.OGR_L_DeleteFeature(hRelLayer, fid)
		}

		C.OGR_L_SetAttributeFilter(hRelLayer, nil)
		C.OGR_L_SyncToDisk(hRelLayer)
	}

	C.GDALFlushCache(hDS)

	fmt.Printf("成功删除要素数据集: %s\n", datasetName)
	return nil
}

// =====================================================
// 更新原有的 ImportPostGISToNewGDBLayerV3 函数
// =====================================================

// 修改原有的 ImportPostGISToNewGDBLayerV3 函数，添加自动创建要素数据集的逻辑
// 这里提供一个包装函数，保持原函数不变

// ImportPostGISToGDBV3Auto 自动处理要素数据集的导入函数
func ImportPostGISToGDBV3Auto(postGISConfig *PostGISConfig, gdbPath string, layerName string, options *ImportToGDBOptionsV3) (*ImportResult, error) {
	// 如果指定了LayerPath，检查并创建要素数据集
	if options.LayerPath != "" {
		datasetName := extractDatasetName(options.LayerPath)
		if datasetName != "" {
			// 确定要使用的空间参考
			var srs *GDBSpatialReference
			if options.TargetSRS != nil {
				srs = options.TargetSRS
			} else {
				// 默认使用CGCS2000
				srs = SRS_CGCS2000
			}

			// 确保要素数据集存在
			err := EnsureFeatureDatasetExists(gdbPath, datasetName, srs)
			if err != nil {
				return nil, fmt.Errorf("创建要素数据集失败: %w", err)
			}
		}
	}

	// 调用原有的导入函数
	return ImportPostGISToNewGDBLayerV3(postGISConfig, gdbPath, layerName, options)
}

// =====================================================
// 批量操作函数
// =====================================================

// BatchCreateFeatureDatasets 批量创建要素数据集
func BatchCreateFeatureDatasets(gdbPath string, datasetNames []string, srs *GDBSpatialReference) error {
	var errors []string

	for _, name := range datasetNames {
		err := CreateFeatureDatasetWithSRS(gdbPath, name, srs)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", name, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("批量创建要素数据集时发生错误:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// MoveLayerToFeatureDataset 将图层移动到指定的要素数据集
func MoveLayerToFeatureDataset(gdbPath string, layerName string, datasetName string, srs *GDBSpatialReference) error {
	// 确保要素数据集存在
	if datasetName != "" {
		err := EnsureFeatureDatasetExists(gdbPath, datasetName, srs)
		if err != nil {
			return fmt.Errorf("确保要素数据集存在失败: %w", err)
		}
	}

	// 更新图层的关系
	err := UpdateGDBItemRelationship(gdbPath, layerName, datasetName)
	if err != nil {
		return fmt.Errorf("更新图层关系失败: %w", err)
	}

	// 更新图层的元数据（Path和CatalogPath）
	meta, err := CreateMetadataWriteFromLayer(gdbPath, layerName)
	if err != nil {
		return fmt.Errorf("读取图层元数据失败: %w", err)
	}

	if datasetName != "" {
		meta.WithLayerPath("\\" + datasetName)
	} else {
		meta.LayerPath = ""
		meta.CatalogPath = "\\" + layerName
	}

	err = WriteGDBLayerMetadata(gdbPath, layerName, meta)
	if err != nil {
		return fmt.Errorf("更新图层元数据失败: %w", err)
	}

	fmt.Printf("成功将图层 '%s' 移动到要素数据集 '%s'\n", layerName, datasetName)
	return nil
}

// GetLayersInFeatureDataset 获取要素数据集中的所有图层
func GetLayersInFeatureDataset(gdbPath string, datasetName string) ([]string, error) {
	InitializeGDAL()

	// 获取要素数据集的UUID
	datasetUUID, err := GetGDBItemUUID(gdbPath, datasetName)
	if err != nil {
		return nil, fmt.Errorf("获取要素数据集UUID失败: %w", err)
	}

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return nil, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 查询GDB_ItemRelationships表
	cGDBItemRels := C.CString("GDB_ItemRelationships")
	defer C.free(unsafe.Pointer(cGDBItemRels))

	hRelLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItemRels)
	if hRelLayer == nil {
		return nil, fmt.Errorf("无法获取GDB_ItemRelationships表")
	}

	// 查找以该数据集为OriginID的关系
	filterSQL := fmt.Sprintf("OriginID = '%s' AND Type = '%s'", datasetUUID, GDBRelTypeDatasetInFeatureDataset)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hRelLayer, cFilter)
	C.OGR_L_ResetReading(hRelLayer)

	hRelDefn := C.OGR_L_GetLayerDefn(hRelLayer)
	cDestID := C.CString("DestID")
	defer C.free(unsafe.Pointer(cDestID))
	destIDIdx := C.OGR_FD_GetFieldIndex(hRelDefn, cDestID)

	var layerUUIDs []string
	for {
		hFeature := C.OGR_L_GetNextFeature(hRelLayer)
		if hFeature == nil {
			break
		}

		if destIDIdx >= 0 {
			destID := C.GoString(C.OGR_F_GetFieldAsString(hFeature, destIDIdx))
			layerUUIDs = append(layerUUIDs, destID)
		}

		C.OGR_F_Destroy(hFeature)
	}

	C.OGR_L_SetAttributeFilter(hRelLayer, nil)

	// 根据UUID获取图层名称
	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hItemsLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hItemsLayer == nil {
		return nil, fmt.Errorf("无法获取GDB_Items表")
	}

	hItemsDefn := C.OGR_L_GetLayerDefn(hItemsLayer)
	cName := C.CString("Name")
	defer C.free(unsafe.Pointer(cName))
	nameIdx := C.OGR_FD_GetFieldIndex(hItemsDefn, cName)

	cUUID := C.CString("UUID")
	defer C.free(unsafe.Pointer(cUUID))

	var layerNames []string
	for _, uuid := range layerUUIDs {
		filterSQL = fmt.Sprintf("UUID = '%s'", uuid)
		cFilter = C.CString(filterSQL)
		C.OGR_L_SetAttributeFilter(hItemsLayer, cFilter)
		C.OGR_L_ResetReading(hItemsLayer)
		C.free(unsafe.Pointer(cFilter))

		hFeature := C.OGR_L_GetNextFeature(hItemsLayer)
		if hFeature != nil && nameIdx >= 0 {
			name := C.GoString(C.OGR_F_GetFieldAsString(hFeature, nameIdx))
			layerNames = append(layerNames, name)
			C.OGR_F_Destroy(hFeature)
		}
	}

	C.OGR_L_SetAttributeFilter(hItemsLayer, nil)

	return layerNames, nil
}

// =====================================================
// 打印和调试函数
// =====================================================

// PrintFeatureDatasetInfo 打印要素数据集信息
func PrintFeatureDatasetInfo(gdbPath string, datasetName string) error {
	metadata, err := GetFeatureDatasetInfo(gdbPath, datasetName)
	if err != nil {
		return err
	}

	fmt.Println("========== 要素数据集信息 ==========")
	fmt.Printf("名称: %s\n", metadata.Name)
	fmt.Printf("DSID: %d\n", metadata.DSID)

	if metadata.SpatialReference != nil {
		fmt.Printf("空间参考 WKID: %d\n", metadata.SpatialReference.WKID)
		fmt.Printf("是否投影坐标系: %v\n", metadata.SpatialReference.IsProjected)
	}

	// 获取数据集中的图层
	layers, err := GetLayersInFeatureDataset(gdbPath, datasetName)
	if err == nil {
		fmt.Printf("包含图层数: %d\n", len(layers))
		for i, layer := range layers {
			fmt.Printf("  %d. %s\n", i+1, layer)
		}
	}

	fmt.Println("====================================")
	return nil
}

// PrintAllFeatureDatasets 打印GDB中所有要素数据集
func PrintAllFeatureDatasets(gdbPath string) error {
	datasets, err := ListFeatureDatasets(gdbPath)
	if err != nil {
		return err
	}

	fmt.Println("========== GDB要素数据集列表 ==========")
	fmt.Printf("共 %d 个要素数据集:\n", len(datasets))
	for i, ds := range datasets {
		fmt.Printf("  %d. %s\n", i+1, ds)
	}
	fmt.Println("========================================")

	return nil
}
