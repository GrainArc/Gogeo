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
	"fmt"
	"strings"
	"unsafe"
)

// =====================================================
// GDB元数据写入相关结构和函数
// =====================================================

// GDBFieldMetadata 字段元数据（用于写入）
type GDBFieldMetadata struct {
	Name       string // 字段名称
	AliasName  string // 字段别名（中文名）
	ModelName  string // 模型名称
	FieldType  string // 字段类型 (esriFieldTypeString, esriFieldTypeInteger, esriFieldTypeDouble等)
	IsNullable bool   // 是否可空
	Length     int    // 字段长度（字符串类型）
	Precision  int    // 精度（数值类型）
	Scale      int    // 小数位数
	Required   bool   // 是否必需
	Editable   bool   // 是否可编辑
}

// GDBExtentInfo 范围信息
type GDBExtentInfo struct {
	XMin float64
	YMin float64
	XMax float64
	YMax float64
}

// GDBSpatialReferenceWrite 空间参考写入结构
type GDBSpatialReferenceWrite struct {
	WKT           string  // WKT字符串
	WKID          int     // 空间参考ID
	LatestWKID    int     // 最新WKID
	XOrigin       float64 // X原点
	YOrigin       float64 // Y原点
	XYScale       float64 // XY比例
	ZOrigin       float64 // Z原点
	ZScale        float64 // Z比例
	MOrigin       float64 // M原点
	MScale        float64 // M比例
	XYTolerance   float64 // XY容差
	ZTolerance    float64 // Z容差
	MTolerance    float64 // M容差
	HighPrecision bool    // 是否高精度
	IsProjected   bool    // 是否为投影坐标系
}

// GDBLayerMetadataWrite 图层元数据（用于写入）
type GDBLayerMetadataWrite struct {
	// 基本信息
	Name        string // 图层名称
	AliasName   string // 图层别名
	LayerPath   string // 图层路径（用于GDB_Items的Path字段，不包含图层名）
	CatalogPath string // 目录路径DSID        int    // 数据集ID
	FeatureType string
	DSID        int
	// 数据集信息
	DatasetType                      string // 数据集类型 (esriDTFeatureClass)FeatureType                    string // 要素类型 (esriFTSimple)
	Versioned                        bool   // 是否版本化
	CanVersion                       bool   // 是否可版本化
	ConfigurationKeyword             string // 配置关键字
	RequiredGeodatabaseClientVersion string // 所需地理数据库客户端版本

	// 几何信息
	ShapeType       string // 几何类型 (esriGeometryPoint/Polygon/Polyline等)
	ShapeFieldName  string // 几何字段名 (默认SHAPE)
	HasM            bool   // 是否有M值
	HasZ            bool   // 是否有Z值
	HasSpatialIndex bool   // 是否有空间索引

	// OID信息
	HasOID       bool   // 是否有OID
	OIDFieldName string // OID字段名 (默认OBJECTID)

	// GlobalID信息
	HasGlobalID       bool   // 是否有GlobalID
	GlobalIDFieldName string // GlobalID字段名

	// CLSID信息
	CLSID    string // 类ID
	EXTCLSID string // 扩展类ID

	// 字段信息
	Fields []GDBFieldMetadata // 字段列表

	// 空间参考
	SpatialReference *GDBSpatialReferenceWrite // 空间参考系统

	// 范围信息
	Extent *GDBExtentInfo // 数据范围

	// 面积和长度字段
	AreaFieldName   string // 面积字段名
	LengthFieldName string // 长度字段名

	// 编辑追踪
	EditorTrackingEnabled bool   // 是否启用编辑追踪
	CreatorFieldName      string // 创建者字段名
	CreatedAtFieldName    string // 创建时间字段名
	EditorFieldName       string // 编辑者字段名
	EditedAtFieldName     string // 编辑时间字段名
	IsTimeInUTC           bool   // 时间是否为UTC

	// 其他
	ChangeTracked         bool   // 是否追踪变更
	FieldFilteringEnabled bool   // 是否启用字段过滤
	RasterFieldName       string // 栅格字段名
}

// GDB字段类型常量
const (
	GDBFieldTypeOID      = "esriFieldTypeOID"
	GDBFieldTypeString   = "esriFieldTypeString"
	GDBFieldTypeInteger  = "esriFieldTypeInteger"
	GDBFieldTypeSmallInt = "esriFieldTypeSmallInteger"
	GDBFieldTypeDouble   = "esriFieldTypeDouble"
	GDBFieldTypeSingle   = "esriFieldTypeSingle"
	GDBFieldTypeDate     = "esriFieldTypeDate"
	GDBFieldTypeGeometry = "esriFieldTypeGeometry"
	GDBFieldTypeBlob     = "esriFieldTypeBlob"
	GDBFieldTypeGlobalID = "esriFieldTypeGlobalID"
	GDBFieldTypeGUID     = "esriFieldTypeGUID"
)

// GDB几何类型常量
const (
	GDBGeometryPoint      = "esriGeometryPoint"
	GDBGeometryMultipoint = "esriGeometryMultipoint"
	GDBGeometryPolyline   = "esriGeometryPolyline"
	GDBGeometryPolygon    = "esriGeometryPolygon"
)

// GDB数据集类型常量
const (
	GDBDatasetTypeFeatureClass = "esriDTFeatureClass"
	GDBDatasetTypeTable        = "esriDTTable"
)

// GDB要素类型常量
const (
	GDBFeatureTypeSimple     = "esriFTSimple"
	GDBFeatureTypeAnnotation = "esriFTAnnotation"
)

// GDB CLSID常量
const (
	GDBCLSIDFeatureClass = "{52353152-891A-11D0-BEC6-00805F7C4268}"
)

func (m *GDBLayerMetadataWrite) WithLayerPath(path string) *GDBLayerMetadataWrite {
	// 统一使用反斜杠
	m.LayerPath = strings.ReplaceAll(path, "/", "\\")
	// 确保LayerPath以反斜杠开头
	if m.LayerPath != "" && !strings.HasPrefix(m.LayerPath, "\\") {
		m.LayerPath = "\\" + m.LayerPath
	}
	// 自动更新CatalogPath
	m.CatalogPath = m.LayerPath + "\\" + m.Name
	return m
}

// NewGDBLayerMetadataWrite 创建新的图层元数据写入对象
func NewGDBLayerMetadataWrite(layerName string) *GDBLayerMetadataWrite {
	return &GDBLayerMetadataWrite{
		Name:                             layerName,
		AliasName:                        layerName,
		CatalogPath:                      "\\" + layerName,
		LayerPath:                        "",
		DSID:                             1,
		DatasetType:                      GDBDatasetTypeFeatureClass,
		FeatureType:                      GDBFeatureTypeSimple,
		Versioned:                        false,
		CanVersion:                       false,
		ConfigurationKeyword:             "",
		RequiredGeodatabaseClientVersion: "10.0",
		ShapeType:                        GDBGeometryPolygon,
		ShapeFieldName:                   "SHAPE",
		HasM:                             false,
		HasZ:                             false,
		HasSpatialIndex:                  true,
		HasOID:                           true,
		OIDFieldName:                     "OBJECTID",
		HasGlobalID:                      false,
		GlobalIDFieldName:                "",
		CLSID:                            GDBCLSIDFeatureClass,
		EXTCLSID:                         "",
		Fields:                           make([]GDBFieldMetadata, 0),
		EditorTrackingEnabled:            false,
		IsTimeInUTC:                      true,
		ChangeTracked:                    false,
		FieldFilteringEnabled:            false}
}

// NewGDBSpatialReferenceWrite 创建新的空间参考写入对象
func NewGDBSpatialReferenceWrite() *GDBSpatialReferenceWrite {
	return &GDBSpatialReferenceWrite{
		XOrigin:       -400,
		YOrigin:       -400,
		XYScale:       1000000000,
		ZOrigin:       -100000,
		ZScale:        10000,
		MOrigin:       -100000,
		MScale:        10000,
		XYTolerance:   0.001,
		ZTolerance:    0.001,
		MTolerance:    0.001,
		HighPrecision: true,
		IsProjected:   false,
	}
}

// WithWKT 设置WKT
func (sr *GDBSpatialReferenceWrite) WithWKT(wkt string) *GDBSpatialReferenceWrite {
	sr.WKT = wkt
	return sr
}

// WithWKID 设置WKID
func (sr *GDBSpatialReferenceWrite) WithWKID(wkid int) *GDBSpatialReferenceWrite {
	sr.WKID = wkid
	sr.LatestWKID = wkid
	return sr
}

// WithOrigin 设置原点
func (sr *GDBSpatialReferenceWrite) WithOrigin(xOrigin, yOrigin float64) *GDBSpatialReferenceWrite {
	sr.XOrigin = xOrigin
	sr.YOrigin = yOrigin
	return sr
}

// WithXYScale 设置XY比例
func (sr *GDBSpatialReferenceWrite) WithXYScale(scale float64) *GDBSpatialReferenceWrite {
	sr.XYScale = scale
	return sr
}

// WithXYTolerance 设置XY容差
func (sr *GDBSpatialReferenceWrite) WithXYTolerance(tolerance float64) *GDBSpatialReferenceWrite {
	sr.XYTolerance = tolerance
	return sr
}

// WithZParams 设置Z参数
func (sr *GDBSpatialReferenceWrite) WithZParams(origin, scale, tolerance float64) *GDBSpatialReferenceWrite {
	sr.ZOrigin = origin
	sr.ZScale = scale
	sr.ZTolerance = tolerance
	return sr
}

// WithMParams 设置M参数
func (sr *GDBSpatialReferenceWrite) WithMParams(origin, scale, tolerance float64) *GDBSpatialReferenceWrite {
	sr.MOrigin = origin
	sr.MScale = scale
	sr.MTolerance = tolerance
	return sr
}

// WithIsProjected 设置是否为投影坐标系
func (sr *GDBSpatialReferenceWrite) WithIsProjected(isProjected bool) *GDBSpatialReferenceWrite {
	sr.IsProjected = isProjected
	return sr
}

// WithAliasName 设置图层别名
func (m *GDBLayerMetadataWrite) WithAliasName(alias string) *GDBLayerMetadataWrite {
	m.AliasName = alias
	return m
}

// WithShapeType 设置几何类型
func (m *GDBLayerMetadataWrite) WithShapeType(shapeType string) *GDBLayerMetadataWrite {
	m.ShapeType = shapeType
	return m
}

// WithHasZ 设置是否有Z值
func (m *GDBLayerMetadataWrite) WithHasZ(hasZ bool) *GDBLayerMetadataWrite {
	m.HasZ = hasZ
	return m
}

// WithHasM 设置是否有M值
func (m *GDBLayerMetadataWrite) WithHasM(hasM bool) *GDBLayerMetadataWrite {
	m.HasM = hasM
	return m
}

// WithSpatialReference 设置空间参考
func (m *GDBLayerMetadataWrite) WithSpatialReference(srs *GDBSpatialReferenceWrite) *GDBLayerMetadataWrite {
	m.SpatialReference = srs
	return m
}

// WithExtent 设置范围
func (m *GDBLayerMetadataWrite) WithExtent(xMin, yMin, xMax, yMax float64) *GDBLayerMetadataWrite {
	m.Extent = &GDBExtentInfo{
		XMin: xMin,
		YMin: yMin,
		XMax: xMax,
		YMax: yMax,
	}
	return m
}

// WithDSID 设置数据集ID
func (m *GDBLayerMetadataWrite) WithDSID(dsid int) *GDBLayerMetadataWrite {
	m.DSID = dsid
	return m
}

// WithAreaAndLengthFields 设置面积和长度字段名
func (m *GDBLayerMetadataWrite) WithAreaAndLengthFields(areaField, lengthField string) *GDBLayerMetadataWrite {
	m.AreaFieldName = areaField
	m.LengthFieldName = lengthField
	return m
}

// WithShapeFieldName 设置几何字段名
func (m *GDBLayerMetadataWrite) WithShapeFieldName(name string) *GDBLayerMetadataWrite {
	m.ShapeFieldName = name
	return m
}

// WithOIDFieldName 设置OID字段名
func (m *GDBLayerMetadataWrite) WithOIDFieldName(name string) *GDBLayerMetadataWrite {
	m.OIDFieldName = name
	return m
}

// AddField 添加字段
func (m *GDBLayerMetadataWrite) AddField(field GDBFieldMetadata) *GDBLayerMetadataWrite {
	// 如果别名为空，使用字段名
	if field.AliasName == "" {
		field.AliasName = field.Name
	}
	// 如果模型名为空，使用字段名
	if field.ModelName == "" {
		field.ModelName = field.Name
	}
	m.Fields = append(m.Fields, field)
	return m
}

// AddStringField 添加字符串字段
func (m *GDBLayerMetadataWrite) AddStringField(name, alias string, length int, nullable bool) *GDBLayerMetadataWrite {
	return m.AddField(GDBFieldMetadata{
		Name:       name,
		AliasName:  alias,
		ModelName:  name,
		FieldType:  GDBFieldTypeString,
		IsNullable: nullable,
		Length:     length,
		Editable:   true,
	})
}

// AddIntegerField 添加整数字段
func (m *GDBLayerMetadataWrite) AddIntegerField(name, alias string, nullable bool) *GDBLayerMetadataWrite {
	return m.AddField(GDBFieldMetadata{
		Name:       name,
		AliasName:  alias,
		ModelName:  name,
		FieldType:  GDBFieldTypeInteger,
		IsNullable: nullable,
		Editable:   true,
	})
}

// AddDoubleField 添加双精度浮点字段
func (m *GDBLayerMetadataWrite) AddDoubleField(name, alias string, precision, scale int, nullable bool) *GDBLayerMetadataWrite {
	return m.AddField(GDBFieldMetadata{
		Name:       name,
		AliasName:  alias,
		ModelName:  name,
		FieldType:  GDBFieldTypeDouble,
		IsNullable: nullable,
		Precision:  precision,
		Scale:      scale,
		Editable:   true,
	})
}

// AddDateField 添加日期字段
func (m *GDBLayerMetadataWrite) AddDateField(name, alias string, nullable bool) *GDBLayerMetadataWrite {
	return m.AddField(GDBFieldMetadata{
		Name:       name,
		AliasName:  alias,
		ModelName:  name,
		FieldType:  GDBFieldTypeDate,
		IsNullable: nullable,
		Editable:   true,
	})
}

// =====================================================
// XML生成函数（手动构建XML字符串）
// =====================================================

// escapeXMLString 转义XML特殊字符
func escapeXMLString(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

// escapeWKT 转义WKT中的引号
func escapeWKT(wkt string) string {
	return strings.ReplaceAll(wkt, "\"", "&quot;")
}

// boolToString 将布尔值转换为字符串
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// GenerateDefinitionXML 生成Definition字段的XML内容
func (m *GDBLayerMetadataWrite) GenerateDefinitionXML() (string, error) {
	var sb strings.Builder

	// 根元素开始
	sb.WriteString("<DEFeatureClassInfo xsi:type='typens:DEFeatureClassInfo' ")
	sb.WriteString("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
	sb.WriteString("xmlns:xs='http://www.w3.org/2001/XMLSchema' ")
	sb.WriteString("xmlns:typens='http://www.esri.com/schemas/ArcGIS/10.1'>")

	// 基本信息
	sb.WriteString(fmt.Sprintf("<CatalogPath>%s</CatalogPath>", escapeXMLString(m.CatalogPath)))
	sb.WriteString(fmt.Sprintf("<Name>%s</Name>", escapeXMLString(m.Name)))
	sb.WriteString("<ChildrenExpanded>false</ChildrenExpanded>")
	sb.WriteString(fmt.Sprintf("<DatasetType>%s</DatasetType>", m.DatasetType))
	sb.WriteString(fmt.Sprintf("<DSID>%d</DSID>", m.DSID))
	sb.WriteString(fmt.Sprintf("<Versioned>%s</Versioned>", boolToString(m.Versioned)))
	sb.WriteString(fmt.Sprintf("<CanVersion>%s</CanVersion>", boolToString(m.CanVersion)))
	sb.WriteString(fmt.Sprintf("<ConfigurationKeyword>%s</ConfigurationKeyword>", m.ConfigurationKeyword))
	sb.WriteString(fmt.Sprintf("<RequiredGeodatabaseClientVersion>%s</RequiredGeodatabaseClientVersion>", m.RequiredGeodatabaseClientVersion))
	sb.WriteString(fmt.Sprintf("<HasOID>%s</HasOID>", boolToString(m.HasOID)))
	sb.WriteString(fmt.Sprintf("<OIDFieldName>%s</OIDFieldName>", m.OIDFieldName))

	// GPFieldInfoExs
	sb.WriteString("<GPFieldInfoExs xsi:type='typens:ArrayOfGPFieldInfoEx'>")
	m.writeGPFieldInfoExs(&sb)
	sb.WriteString("</GPFieldInfoExs>")

	// CLSID
	sb.WriteString(fmt.Sprintf("<CLSID>%s</CLSID>", m.CLSID))
	sb.WriteString(fmt.Sprintf("<EXTCLSID>%s</EXTCLSID>", m.EXTCLSID))

	// RelationshipClassNames
	sb.WriteString("<RelationshipClassNames xsi:type='typens:Names'></RelationshipClassNames>")

	// AliasName
	sb.WriteString(fmt.Sprintf("<AliasName>%s</AliasName>", escapeXMLString(m.AliasName)))
	sb.WriteString("<ModelName></ModelName>")

	// GlobalID
	sb.WriteString(fmt.Sprintf("<HasGlobalID>%s</HasGlobalID>", boolToString(m.HasGlobalID)))
	sb.WriteString(fmt.Sprintf("<GlobalIDFieldName>%s</GlobalIDFieldName>", m.GlobalIDFieldName))
	sb.WriteString(fmt.Sprintf("<RasterFieldName>%s</RasterFieldName>", m.RasterFieldName))

	// ExtensionProperties
	sb.WriteString("<ExtensionProperties xsi:type='typens:PropertySet'>")
	sb.WriteString("<PropertyArray xsi:type='typens:ArrayOfPropertySetProperty'></PropertyArray>")
	sb.WriteString("</ExtensionProperties>")

	// ControllerMemberships
	sb.WriteString("<ControllerMemberships xsi:type='typens:ArrayOfControllerMembership'></ControllerMemberships>")

	// EditorTracking
	sb.WriteString(fmt.Sprintf("<EditorTrackingEnabled>%s</EditorTrackingEnabled>", boolToString(m.EditorTrackingEnabled)))
	sb.WriteString(fmt.Sprintf("<CreatorFieldName>%s</CreatorFieldName>", m.CreatorFieldName))
	sb.WriteString(fmt.Sprintf("<CreatedAtFieldName>%s</CreatedAtFieldName>", m.CreatedAtFieldName))
	sb.WriteString(fmt.Sprintf("<EditorFieldName>%s</EditorFieldName>", m.EditorFieldName))
	sb.WriteString(fmt.Sprintf("<EditedAtFieldName>%s</EditedAtFieldName>", m.EditedAtFieldName))
	sb.WriteString(fmt.Sprintf("<IsTimeInUTC>%s</IsTimeInUTC>", boolToString(m.IsTimeInUTC)))

	// FeatureClass特有属性
	sb.WriteString(fmt.Sprintf("<FeatureType>%s</FeatureType>", m.FeatureType))
	sb.WriteString(fmt.Sprintf("<ShapeType>%s</ShapeType>", m.ShapeType))
	sb.WriteString(fmt.Sprintf("<ShapeFieldName>%s</ShapeFieldName>", m.ShapeFieldName))
	sb.WriteString(fmt.Sprintf("<HasM>%s</HasM>", boolToString(m.HasM)))
	sb.WriteString(fmt.Sprintf("<HasZ>%s</HasZ>", boolToString(m.HasZ)))
	sb.WriteString(fmt.Sprintf("<HasSpatialIndex>%s</HasSpatialIndex>", boolToString(m.HasSpatialIndex)))
	sb.WriteString(fmt.Sprintf("<AreaFieldName>%s</AreaFieldName>", m.AreaFieldName))
	sb.WriteString(fmt.Sprintf("<LengthFieldName>%s</LengthFieldName>", m.LengthFieldName))

	// Extent
	if m.Extent != nil && m.SpatialReference != nil {
		sb.WriteString("<Extent xsi:type='typens:EnvelopeN'>")
		sb.WriteString(fmt.Sprintf("<XMin>%v</XMin>", m.Extent.XMin))
		sb.WriteString(fmt.Sprintf("<YMin>%v</YMin>", m.Extent.YMin))
		sb.WriteString(fmt.Sprintf("<XMax>%v</XMax>", m.Extent.XMax))
		sb.WriteString(fmt.Sprintf("<YMax>%v</YMax>", m.Extent.YMax))
		m.writeSpatialReference(&sb)
		sb.WriteString("</Extent>")
	}

	// SpatialReference
	if m.SpatialReference != nil {
		m.writeSpatialReference(&sb)
	}

	// 其他属性
	sb.WriteString(fmt.Sprintf("<ChangeTracked>%s</ChangeTracked>", boolToString(m.ChangeTracked)))
	sb.WriteString(fmt.Sprintf("<FieldFilteringEnabled>%s</FieldFilteringEnabled>", boolToString(m.FieldFilteringEnabled)))
	sb.WriteString("<FilteredFieldNames xsi:type='typens:Names'></FilteredFieldNames>")

	// 根元素结束
	sb.WriteString("</DEFeatureClassInfo>")

	return sb.String(), nil
}

// writeGPFieldInfoExs 写入字段信息
func (m *GDBLayerMetadataWrite) writeGPFieldInfoExs(sb *strings.Builder) {
	// OID字段
	if m.HasOID {
		sb.WriteString("<GPFieldInfoEx xsi:type='typens:GPFieldInfoEx'>")
		sb.WriteString(fmt.Sprintf("<Name>%s</Name>", m.OIDFieldName))
		sb.WriteString(fmt.Sprintf("<AliasName>%s</AliasName>", m.OIDFieldName))
		sb.WriteString(fmt.Sprintf("<ModelName>%s</ModelName>", m.OIDFieldName))
		sb.WriteString(fmt.Sprintf("<FieldType>%s</FieldType>", GDBFieldTypeOID))
		sb.WriteString("<IsNullable>false</IsNullable>")
		sb.WriteString("<Required>true</Required>")
		sb.WriteString("<Editable>false</Editable>")
		sb.WriteString("</GPFieldInfoEx>")
	}

	// Shape字段
	sb.WriteString("<GPFieldInfoEx xsi:type='typens:GPFieldInfoEx'>")
	sb.WriteString(fmt.Sprintf("<Name>%s</Name>", m.ShapeFieldName))
	sb.WriteString(fmt.Sprintf("<AliasName>%s</AliasName>", m.ShapeFieldName))
	sb.WriteString(fmt.Sprintf("<ModelName>%s</ModelName>", m.ShapeFieldName))
	sb.WriteString(fmt.Sprintf("<FieldType>%s</FieldType>", GDBFieldTypeGeometry))
	sb.WriteString("<IsNullable>true</IsNullable>")
	sb.WriteString("<Required>true</Required>")
	sb.WriteString("</GPFieldInfoEx>")

	// 用户定义的字段
	for _, field := range m.Fields {
		sb.WriteString("<GPFieldInfoEx xsi:type='typens:GPFieldInfoEx'>")
		sb.WriteString(fmt.Sprintf("<Name>%s</Name>", escapeXMLString(field.Name)))
		if field.AliasName != "" {
			sb.WriteString(fmt.Sprintf("<AliasName>%s</AliasName>", escapeXMLString(field.AliasName)))
		}
		modelName := field.ModelName
		if modelName == "" {
			modelName = field.Name
		}
		sb.WriteString(fmt.Sprintf("<ModelName>%s</ModelName>", escapeXMLString(modelName)))
		sb.WriteString(fmt.Sprintf("<FieldType>%s</FieldType>", field.FieldType))
		sb.WriteString(fmt.Sprintf("<IsNullable>%s</IsNullable>", boolToString(field.IsNullable)))
		if field.Required {
			sb.WriteString("<Required>true</Required>")
		}
		if !field.Editable && field.Required {
			sb.WriteString("<Editable>false</Editable>")
		}
		sb.WriteString("</GPFieldInfoEx>")
	}

	// SHAPE_Length字段（线和面要素）
	if m.ShapeType == GDBGeometryPolygon || m.ShapeType == GDBGeometryPolyline {
		if m.LengthFieldName != "" {
			sb.WriteString("<GPFieldInfoEx xsi:type='typens:GPFieldInfoEx'>")
			sb.WriteString(fmt.Sprintf("<Name>%s</Name>", m.LengthFieldName))
			sb.WriteString(fmt.Sprintf("<ModelName>%s</ModelName>", m.LengthFieldName))
			sb.WriteString(fmt.Sprintf("<FieldType>%s</FieldType>", GDBFieldTypeDouble))
			sb.WriteString("<IsNullable>true</IsNullable>")
			sb.WriteString("<Required>true</Required>")
			sb.WriteString("<Editable>false</Editable>")
			sb.WriteString("</GPFieldInfoEx>")
		}
	}

	// SHAPE_Area字段（仅面要素）
	if m.ShapeType == GDBGeometryPolygon {
		if m.AreaFieldName != "" {
			sb.WriteString("<GPFieldInfoEx xsi:type='typens:GPFieldInfoEx'>")
			sb.WriteString(fmt.Sprintf("<Name>%s</Name>", m.AreaFieldName))
			sb.WriteString(fmt.Sprintf("<ModelName>%s</ModelName>", m.AreaFieldName))
			sb.WriteString(fmt.Sprintf("<FieldType>%s</FieldType>", GDBFieldTypeDouble))
			sb.WriteString("<IsNullable>true</IsNullable>")
			sb.WriteString("<Required>true</Required>")
			sb.WriteString("<Editable>false</Editable>")
			sb.WriteString("</GPFieldInfoEx>")
		}
	}
}

// writeSpatialReference 写入空间参考
func (m *GDBLayerMetadataWrite) writeSpatialReference(sb *strings.Builder) {
	if m.SpatialReference == nil {
		return
	}

	sr := m.SpatialReference

	// 根据是否为投影坐标系选择类型
	srType := "typens:GeographicCoordinateSystem"
	if sr.IsProjected {
		srType = "typens:ProjectedCoordinateSystem"
	}

	sb.WriteString(fmt.Sprintf("<SpatialReference xsi:type='%s'>", srType))

	// WKT
	if sr.WKT != "" {
		sb.WriteString(fmt.Sprintf("<WKT>%s</WKT>", escapeWKT(sr.WKT)))
	}

	// 精度参数
	sb.WriteString(fmt.Sprintf("<XOrigin>%v</XOrigin>", sr.XOrigin))
	sb.WriteString(fmt.Sprintf("<YOrigin>%v</YOrigin>", sr.YOrigin))
	sb.WriteString(fmt.Sprintf("<XYScale>%v</XYScale>", sr.XYScale))
	sb.WriteString(fmt.Sprintf("<ZOrigin>%v</ZOrigin>", sr.ZOrigin))
	sb.WriteString(fmt.Sprintf("<ZScale>%v</ZScale>", sr.ZScale))
	sb.WriteString(fmt.Sprintf("<MOrigin>%v</MOrigin>", sr.MOrigin))
	sb.WriteString(fmt.Sprintf("<MScale>%v</MScale>", sr.MScale))
	sb.WriteString(fmt.Sprintf("<XYTolerance>%v</XYTolerance>", sr.XYTolerance))
	sb.WriteString(fmt.Sprintf("<ZTolerance>%v</ZTolerance>", sr.ZTolerance))
	sb.WriteString(fmt.Sprintf("<MTolerance>%v</MTolerance>", sr.MTolerance))
	sb.WriteString(fmt.Sprintf("<HighPrecision>%s</HighPrecision>", boolToString(sr.HighPrecision)))

	// WKID
	if sr.WKID > 0 {
		sb.WriteString(fmt.Sprintf("<WKID>%d</WKID>", sr.WKID))
	}
	if sr.LatestWKID > 0 {
		sb.WriteString(fmt.Sprintf("<LatestWKID>%d</LatestWKID>", sr.LatestWKID))
	}

	sb.WriteString("</SpatialReference>")
}

// =====================================================
// 元数据写入函数
// =====================================================

// WriteGDBLayerMetadata 将元数据写入GDB文件的GDB_Items表
// gdbPath: GDB文件路径
// layerName: 图层名称
// metadata: 要写入的元数据

func WriteGDBLayerMetadata(gdbPath string, layerName string, metadata *GDBLayerMetadataWrite) error {

	// 生成Definition XML
	definitionXML, err := metadata.GenerateDefinitionXML()
	if err != nil {
		return fmt.Errorf("生成Definition XML失败: %w", err)
	}

	// 计算完整的Path值
	fullPath := metadata.CatalogPath
	if fullPath == "" {
		fullPath = "\\" + layerName
	}

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	// 以更新模式打开GDB数据集
	// GDAL_OF_VECTOR | GDAL_OF_UPDATE = 0x04 | 0x01 = 0x05
	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return fmt.Errorf("无法以更新模式打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 获取GDB_Items图层
	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hGDBItemsLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hGDBItemsLayer == nil {
		return fmt.Errorf("无法获取GDB_Items表")
	}

	// 设置属性过滤器查找目标记录
	filterSQL := fmt.Sprintf("Name = '%s'", layerName)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	if C.OGR_L_SetAttributeFilter(hGDBItemsLayer, cFilter) != 0 {
		return fmt.Errorf("设置属性过滤器失败")
	}

	// 重置读取位置
	C.OGR_L_ResetReading(hGDBItemsLayer)

	// 获取目标要素
	hFeature := C.OGR_L_GetNextFeature(hGDBItemsLayer)
	if hFeature == nil {
		// 清除过滤器
		C.OGR_L_SetAttributeFilter(hGDBItemsLayer, nil)
		return fmt.Errorf("图层 '%s' 在GDB_Items表中不存在", layerName)
	}
	defer C.OGR_F_Destroy(hFeature)

	// 获取字段定义
	hFeatureDefn := C.OGR_L_GetLayerDefn(hGDBItemsLayer)

	// 获取Definition字段索引并设置值
	cDefinition := C.CString("Definition")
	defer C.free(unsafe.Pointer(cDefinition))

	definitionIdx := C.OGR_FD_GetFieldIndex(hFeatureDefn, cDefinition)
	if definitionIdx < 0 {
		C.OGR_L_SetAttributeFilter(hGDBItemsLayer, nil)
		return fmt.Errorf("GDB_Items表中不存在Definition字段")
	}

	cDefinitionXML := C.CString(definitionXML)
	defer C.free(unsafe.Pointer(cDefinitionXML))
	C.OGR_F_SetFieldString(hFeature, definitionIdx, cDefinitionXML)

	// 获取Path字段索引并设置值
	cPathField := C.CString("Path")
	defer C.free(unsafe.Pointer(cPathField))

	pathIdx := C.OGR_FD_GetFieldIndex(hFeatureDefn, cPathField)
	if pathIdx >= 0 {
		cFullPath := C.CString(fullPath)
		defer C.free(unsafe.Pointer(cFullPath))
		C.OGR_F_SetFieldString(hFeature, pathIdx, cFullPath)
	} else {
		fmt.Printf("警告: GDB_Items表中不存在Path字段\n")
	}

	// 更新要素
	err2 := C.OGR_L_SetFeature(hGDBItemsLayer, hFeature)
	if err2 != 0 {
		C.OGR_L_SetAttributeFilter(hGDBItemsLayer, nil)
		return fmt.Errorf("更新要素失败，错误码: %d", err2)
	}

	// 同步到磁盘
	err3 := C.OGR_L_SyncToDisk(hGDBItemsLayer)
	if err3 != 0 {
		fmt.Printf("警告: 同步到磁盘返回非零值: %d\n", err3)
	}

	// 清除过滤器
	C.OGR_L_SetAttributeFilter(hGDBItemsLayer, nil)

	// 刷新数据集
	C.GDALFlushCache(hDS)
	if metadata.LayerPath != "" {
		// 从LayerPath提取数据集名称
		datasetName := extractDatasetName(metadata.LayerPath)
		if datasetName != "" {
			fmt.Printf("正在更新图层 '%s' 到数据集 '%s' 的关系...\n", layerName, datasetName)
			relErr := UpdateGDBItemRelationship(gdbPath, layerName, datasetName)
			if relErr != nil {
				fmt.Printf("警告: 更新关系失败: %v\n", relErr)
			}
		}
	}
	fmt.Printf("成功更新图层 '%s' 的元数据，Path: %s\n", layerName, fullPath)
	return nil
}
func extractDatasetName(layerPath string) string {
	// LayerPath格式如 "\BCDataset" 或 "\Folder\BCDataset"
	path := strings.TrimPrefix(layerPath, "\\")
	parts := strings.Split(path, "\\")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return ""
}

// =====================================================
// 从现有图层读取信息并创建写入对象
// =====================================================

// CreateMetadataWriteFromLayer 从GDB图层读取信息创建元数据写入对象
func CreateMetadataWriteFromLayer(gdbPath string, layerName string) (*GDBLayerMetadataWrite, error) {

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	// 打开数据集
	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return nil, fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	// 获取图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cLayerName)
	if hLayer == nil {
		return nil, fmt.Errorf("无法获取图层: %s", layerName)
	}

	// 创建元数据对象
	meta := NewGDBLayerMetadataWrite(layerName)

	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)

	// 获取几何类型
	geomType := C.OGR_L_GetGeomType(hLayer)
	meta.ShapeType = mapOGRGeomTypeToEsri(int(geomType))

	// 检查是否有Z和M
	meta.HasZ = (geomType&0x80000000 != 0) || (geomType >= 1000 && geomType < 2000) || (geomType >= 3000)
	meta.HasM = (geomType >= 2000 && geomType < 3000) || (geomType >= 3000)

	// 获取空间参考
	hSRS := C.OGR_L_GetSpatialRef(hLayer)
	if hSRS != nil {
		meta.SpatialReference = extractSpatialReferenceWrite(hSRS)
	}

	// 获取范围
	var envelope C.OGREnvelope
	if C.OGR_L_GetExtent(hLayer, &envelope, 1) == 0 {
		meta.Extent = &GDBExtentInfo{
			XMin: float64(envelope.MinX),
			YMin: float64(envelope.MinY),
			XMax: float64(envelope.MaxX),
			YMax: float64(envelope.MaxY),
		}
	}

	// 获取字段信息
	fieldCount := int(C.OGR_FD_GetFieldCount(hLayerDefn))
	for i := 0; i < fieldCount; i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		if hFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(hFieldDefn))

		// 跳过系统字段
		if fieldName == meta.OIDFieldName || fieldName == meta.ShapeFieldName ||
			fieldName == "SHAPE_Length" || fieldName == "SHAPE_Area" {
			continue
		}

		fieldType := C.OGR_Fld_GetType(hFieldDefn)
		esriFieldType := mapOGRFieldTypeToEsri(int(fieldType))

		field := GDBFieldMetadata{
			Name:       fieldName,
			AliasName:  fieldName, // 默认别名等于字段名
			ModelName:  fieldName,
			FieldType:  esriFieldType,
			IsNullable: C.OGR_Fld_IsNullable(hFieldDefn) != 0,
			Editable:   true,
		}

		// 获取字段长度
		if fieldType == C.OFTString {
			field.Length = int(C.OGR_Fld_GetWidth(hFieldDefn))
		}

		// 获取精度和小数位数
		if fieldType == C.OFTReal {
			field.Precision = int(C.OGR_Fld_GetWidth(hFieldDefn))
			field.Scale = int(C.OGR_Fld_GetPrecision(hFieldDefn))
		}

		meta.Fields = append(meta.Fields, field)
	}

	// 设置面积和长度字段名
	if meta.ShapeType == GDBGeometryPolygon {
		meta.AreaFieldName = "SHAPE_Area"
		meta.LengthFieldName = "SHAPE_Length"
	} else if meta.ShapeType == GDBGeometryPolyline {
		meta.LengthFieldName = "SHAPE_Length"
	}

	return meta, nil
}

// extractSpatialReferenceWrite 从OGR空间参考提取写入结构
func extractSpatialReferenceWrite(hSRS C.OGRSpatialReferenceH) *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()

	// 获取WKT
	var pszWKT *C.char
	C.OSRExportToWkt(hSRS, &pszWKT)
	if pszWKT != nil {
		sr.WKT = C.GoString(pszWKT)
		C.CPLFree(unsafe.Pointer(pszWKT))
	}

	// 判断是否为投影坐标系
	sr.IsProjected = C.OSRIsProjected(hSRS) != 0

	// 尝试获取EPSG代码
	cAuthority := C.CString("EPSG")
	defer C.free(unsafe.Pointer(cAuthority))

	var authCode *C.char
	if sr.IsProjected {
		cProjCS := C.CString("PROJCS")
		defer C.free(unsafe.Pointer(cProjCS))
		authCode = C.OSRGetAuthorityCode(hSRS, cProjCS)
	} else {
		cGeogCS := C.CString("GEOGCS")
		defer C.free(unsafe.Pointer(cGeogCS))
		authCode = C.OSRGetAuthorityCode(hSRS, cGeogCS)
	}

	if authCode != nil {
		var epsg int
		fmt.Sscanf(C.GoString(authCode), "%d", &epsg)
		sr.WKID = epsg
		sr.LatestWKID = epsg
	}

	// 根据坐标系类型设置默认精度参数
	if sr.IsProjected {
		// 投影坐标系默认参数
		sr.XYTolerance = 0.001
		sr.XYScale = 10000

		// 尝试从WKT中提取更合适的原点
		// 这里使用通用默认值，实际应用中可能需要根据具体投影调整
		sr.XOrigin = -5123200
		sr.YOrigin = -9998100
	} else {
		// 地理坐标系默认参数
		sr.XOrigin = -400
		sr.YOrigin = -400
		sr.XYScale = 1000000000
		sr.XYTolerance = 8.983152841195215e-09
	}

	return sr
}

// mapOGRGeomTypeToEsri 将OGR几何类型映射到Esri类型
func mapOGRGeomTypeToEsri(ogrType int) string {
	// 去除Z和M标志
	baseType := ogrType & 0xFF

	switch baseType {
	case 1: // wkbPoint
		return GDBGeometryPoint
	case 2: // wkbLineString
		return GDBGeometryPolyline
	case 3: // wkbPolygon
		return GDBGeometryPolygon
	case 4: // wkbMultiPoint
		return GDBGeometryMultipoint
	case 5: // wkbMultiLineString
		return GDBGeometryPolyline
	case 6: // wkbMultiPolygon
		return GDBGeometryPolygon
	default:
		return GDBGeometryPolygon
	}
}

// mapOGRFieldTypeToEsri 将OGR字段类型映射到Esri类型
func mapOGRFieldTypeToEsri(ogrType int) string {
	switch ogrType {
	case 0: // OFTInteger
		return GDBFieldTypeInteger
	case 1: // OFTIntegerList
		return GDBFieldTypeInteger
	case 2: // OFTReal
		return GDBFieldTypeDouble
	case 3: // OFTRealList
		return GDBFieldTypeDouble
	case 4: // OFTString
		return GDBFieldTypeString
	case 5: // OFTStringList
		return GDBFieldTypeString
	case 8: // OFTBinary
		return GDBFieldTypeBlob
	case 9: // OFTDate
		return GDBFieldTypeDate
	case 10: // OFTTime
		return GDBFieldTypeDate
	case 11: // OFTDateTime
		return GDBFieldTypeDate
	case 12: // OFTInteger64
		return GDBFieldTypeInteger
	default:
		return GDBFieldTypeString
	}
}

// =====================================================
// 便捷更新函数
// =====================================================

// UpdateGDBLayerAlias 更新图层别名
// gdbPath: GDB文件路径
// layerName: 图层名称
// aliasName: 新的别名
func UpdateGDBLayerAlias(gdbPath string, layerName string, aliasName string) error {
	// 从现有图层创建元数据
	meta, err := CreateMetadataWriteFromLayer(gdbPath, layerName)
	if err != nil {
		return fmt.Errorf("读取现有元数据失败: %w", err)
	}

	// 更新别名
	meta.AliasName = aliasName

	return WriteGDBLayerMetadata(gdbPath, layerName, meta)
}

// UpdateGDBFieldAliases 批量更新字段别名
// gdbPath: GDB文件路径
// layerName: 图层名称
// fieldAliases: 字段别名映射 (字段名 -> 别名)
func UpdateGDBFieldAliases(gdbPath string, layerName string, fieldAliases map[string]string) error {
	// 从现有图层创建元数据
	meta, err := CreateMetadataWriteFromLayer(gdbPath, layerName)
	if err != nil {
		return fmt.Errorf("读取现有元数据失败: %w", err)
	}

	// 更新字段别名
	for i := range meta.Fields {
		if alias, exists := fieldAliases[meta.Fields[i].Name]; exists {
			meta.Fields[i].AliasName = alias
		}
	}

	return WriteGDBLayerMetadata(gdbPath, layerName, meta)
}

// UpdateGDBLayerAndFieldAliases 同时更新图层别名和字段别名
// gdbPath: GDB文件路径
// layerName: 图层名称
// layerAlias: 图层别名
// fieldAliases: 字段别名映射 (字段名 -> 别名)
func UpdateGDBLayerAndFieldAliases(gdbPath string, layerName string, layerAlias string, fieldAliases map[string]string) error {
	// 从现有图层创建元数据
	meta, err := CreateMetadataWriteFromLayer(gdbPath, layerName)
	if err != nil {
		return fmt.Errorf("读取现有元数据失败: %w", err)
	}

	// 更新图层别名
	meta.AliasName = layerAlias

	// 更新字段别名
	for i := range meta.Fields {
		if alias, exists := fieldAliases[meta.Fields[i].Name]; exists {
			meta.Fields[i].AliasName = alias
		}
	}

	return WriteGDBLayerMetadata(gdbPath, layerName, meta)
}

// =====================================================
// 批量操作函数
// =====================================================

// BatchUpdateGDBMetadata 批量更新多个图层的元数据
// gdbPath: GDB文件路径
// metadataList: 元数据列表 (图层名 -> 元数据)
func BatchUpdateGDBMetadata(gdbPath string, metadataList map[string]*GDBLayerMetadataWrite) error {
	var errors []string

	for layerName, metadata := range metadataList {
		err := WriteGDBLayerMetadata(gdbPath, layerName, metadata)
		if err != nil {
			errors = append(errors, fmt.Sprintf("图层 %s: %v", layerName, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("批量更新元数据时发生错误:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// GDBMetadataUpdateConfig 元数据更新配置
type GDBMetadataUpdateConfig struct {
	LayerName    string            // 图层名称
	LayerAlias   string            // 图层别名（可选，为空则不更新）
	FieldAliases map[string]string // 字段别名映射（可选）
}

// BatchUpdateGDBMetadataFromConfig 根据配置批量更新元数据
func BatchUpdateGDBMetadataFromConfig(gdbPath string, configs []GDBMetadataUpdateConfig) error {
	var errors []string

	for _, config := range configs {
		var err error

		if config.LayerAlias != "" && len(config.FieldAliases) > 0 {
			// 同时更新图层别名和字段别名
			err = UpdateGDBLayerAndFieldAliases(gdbPath, config.LayerName, config.LayerAlias, config.FieldAliases)
		} else if config.LayerAlias != "" {
			// 只更新图层别名
			err = UpdateGDBLayerAlias(gdbPath, config.LayerName, config.LayerAlias)
		} else if len(config.FieldAliases) > 0 {
			// 只更新字段别名
			err = UpdateGDBFieldAliases(gdbPath, config.LayerName, config.FieldAliases)
		}

		if err != nil {
			errors = append(errors, fmt.Sprintf("图层 %s: %v", config.LayerName, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("批量更新元数据时发生错误:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// =====================================================
// 导入时自动设置元数据
// =====================================================

// ImportToGDBOptionsV3 导入选项（V3版本，支持元数据设置）
type ImportToGDBOptionsV3 struct {
	ImportToGDBOptionsV2     // 继承V2选项
	LayerPath                string
	LayerAlias               string                    // 图层别名
	FieldAliases             map[string]string         // 字段别名映射
	AutoUpdateMetadata       bool                      // 导入后自动更新元数据
	SpatialReferenceOverride *GDBSpatialReferenceWrite // 空间参考覆盖设置
}

func (opts *ImportToGDBOptionsV3) WithLayerPath(path string) *ImportToGDBOptionsV3 {
	// 统一使用反斜杠
	opts.LayerPath = strings.ReplaceAll(path, "/", "\\")
	// 确保LayerPath以反斜杠开头
	if opts.LayerPath != "" && !strings.HasPrefix(opts.LayerPath, "\\") {
		opts.LayerPath = "\\" + opts.LayerPath
	}

	return opts
}

// NewImportToGDBOptionsV3 创建默认的V3导入选项
func NewImportToGDBOptionsV3() *ImportToGDBOptionsV3 {
	return &ImportToGDBOptionsV3{
		ImportToGDBOptionsV2: *NewImportToGDBOptionsV2(),
		FieldAliases:         make(map[string]string),
		AutoUpdateMetadata:   true,
	}
}

// WithTargetSRS 设置目标空间参考（继承方法）
func (opts *ImportToGDBOptionsV3) WithTargetSRS(srs *GDBSpatialReference) *ImportToGDBOptionsV3 {
	opts.ImportToGDBOptionsV2.TargetSRS = srs
	return opts
}

// WithLayerAlias 设置图层别名
func (opts *ImportToGDBOptionsV3) WithLayerAlias(alias string) *ImportToGDBOptionsV3 {
	opts.LayerAlias = alias
	return opts
}

// WithFieldAlias 添加字段别名
func (opts *ImportToGDBOptionsV3) WithFieldAlias(fieldName, alias string) *ImportToGDBOptionsV3 {
	if opts.FieldAliases == nil {
		opts.FieldAliases = make(map[string]string)
	}
	opts.FieldAliases[fieldName] = alias
	return opts
}

// WithFieldAliases 批量设置字段别名
func (opts *ImportToGDBOptionsV3) WithFieldAliases(aliases map[string]string) *ImportToGDBOptionsV3 {
	opts.FieldAliases = aliases
	return opts
}

// WithSpatialReferenceOverride 设置空间参考覆盖
func (opts *ImportToGDBOptionsV3) WithSpatialReferenceOverride(sr *GDBSpatialReferenceWrite) *ImportToGDBOptionsV3 {
	opts.SpatialReferenceOverride = sr
	return opts
}

// WithAutoUpdateMetadata 设置是否自动更新元数据
func (opts *ImportToGDBOptionsV3) WithAutoUpdateMetadata(auto bool) *ImportToGDBOptionsV3 {
	opts.AutoUpdateMetadata = auto
	return opts
}

// ImportPostGISToGDBV3 将PostGIS数据表导入到GDB文件（V3版本，支持元数据设置）
func ImportPostGISToGDBV3(postGISConfig *PostGISConfig, gdbPath string, gdbLayerName string, options *ImportToGDBOptionsV3) (*ImportResult, error) {
	// 使用V2版本进行导入
	v2Options := &options.ImportToGDBOptionsV2
	result, err := ImportPostGISToGDBV2(postGISConfig, gdbPath, gdbLayerName, v2Options)
	if err != nil {
		return result, err
	}

	// 如果启用了自动更新元数据
	if options.AutoUpdateMetadata && (options.LayerAlias != "" || options.LayerPath != "" || len(options.FieldAliases) > 0 || options.SpatialReferenceOverride != nil) {
		fmt.Println("正在更新图层元数据...")

		// 从导入的图层创建元数据
		meta, metaErr := CreateMetadataWriteFromLayer(gdbPath, gdbLayerName)
		if metaErr != nil {
			fmt.Printf("警告: 读取元数据失败: %v\n", metaErr)
			return result, nil
		}

		// 更新图层别名
		if options.LayerAlias != "" {
			meta.AliasName = options.LayerAlias
		}

		// 更新图层路径
		if options.LayerPath != "" {
			meta.WithLayerPath(options.LayerPath)
		}

		// 更新字段别名
		for i := range meta.Fields {
			if alias, exists := options.FieldAliases[meta.Fields[i].Name]; exists {
				meta.Fields[i].AliasName = alias
			}
		}

		// 覆盖空间参考设置
		if options.SpatialReferenceOverride != nil {
			meta.SpatialReference = options.SpatialReferenceOverride
		}

		// 写入元数据
		writeErr := WriteGDBLayerMetadata(gdbPath, gdbLayerName, meta)
		if writeErr != nil {
			fmt.Printf("警告: 更新元数据失败: %v\n", writeErr)
		} else {
			fmt.Println("元数据更新成功")
		}
	}

	return result, nil
}

// ImportPostGISToNewGDBLayerV3 将PostGIS数据表导入到GDB文件，创建新图层（V3版本）
func ImportPostGISToNewGDBLayerV3(postGISConfig *PostGISConfig, gdbPath string, layerName string, options *ImportToGDBOptionsV3) (*ImportResult, error) {
	// 使用V2版本进行导入
	v2Options := &options.ImportToGDBOptionsV2
	result, err := ImportPostGISToNewGDBLayerV2(postGISConfig, gdbPath, layerName, v2Options)
	if err != nil {
		return result, err
	}

	// 如果启用了自动更新元数据
	if options.AutoUpdateMetadata && (options.LayerAlias != "" || options.LayerPath != "" || len(options.FieldAliases) > 0 || options.SpatialReferenceOverride != nil) {
		fmt.Println("正在更新图层元数据...")

		// 从导入的图层创建元数据
		meta, metaErr := CreateMetadataWriteFromLayer(gdbPath, layerName)
		if metaErr != nil {
			fmt.Printf("警告: 读取元数据失败: %v\n", metaErr)
			return result, nil
		}

		// 更新图层别名
		if options.LayerAlias != "" {
			meta.AliasName = options.LayerAlias
		}

		// 更新图层路径
		if options.LayerPath != "" {
			meta.WithLayerPath(options.LayerPath)
		}

		// 更新字段别名
		for i := range meta.Fields {
			if alias, exists := options.FieldAliases[meta.Fields[i].Name]; exists {
				meta.Fields[i].AliasName = alias
			}
		}

		// 覆盖空间参考设置
		if options.SpatialReferenceOverride != nil {
			meta.SpatialReference = options.SpatialReferenceOverride
		}

		// 写入元数据
		writeErr := WriteGDBLayerMetadata(gdbPath, layerName, meta)
		if writeErr != nil {
			fmt.Printf("警告: 更新元数据失败: %v\n", writeErr)
		} else {
			fmt.Println("元数据更新成功")
		}
		// 【新增】更新关系
		if options.LayerPath != "" {
			datasetName := extractDatasetName(options.LayerPath)
			if datasetName != "" {
				fmt.Printf("正在更新图层到数据集 '%s' 的关系...\n", datasetName)
				relErr := UpdateGDBItemRelationship(gdbPath, layerName, datasetName)
				if relErr != nil {
					fmt.Printf("警告: 更新关系失败: %v\n", relErr)
				} else {
					fmt.Println("关系更新成功")
				}
			}
		}
	}

	return result, nil
}

// =====================================================
// 空间参考预设
// =====================================================

// NewCGCS2000SpatialReference 创建CGCS2000地理坐标系空间参考
func NewCGCS2000SpatialReference() *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()
	sr.WKID = 4490
	sr.LatestWKID = 4490
	sr.WKT = `GEOGCS["GCS_China_Geodetic_Coordinate_System_2000",DATUM["D_China_2000",SPHEROID["CGCS2000",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433],AUTHORITY["EPSG",4490]]`
	sr.IsProjected = false
	sr.XOrigin = -400
	sr.YOrigin = -400
	sr.XYScale = 1000000000
	sr.XYTolerance = 8.983152841195215e-09
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true
	return sr
}

// NewCGCS2000_3DegreeGK_Zone 创建CGCS2000 3度带高斯克吕格投影坐标系
// zone: 带号 (如35表示中央经线105度)
func NewCGCS2000_3DegreeGK_Zone(zone int) *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()

	// 计算WKID (CGCS2000 3度带从4513开始)
	wkid := 4513 + (zone - 25)
	sr.WKID = wkid
	sr.LatestWKID = wkid

	// 计算中央经线
	centralMeridian := float64(zone * 3)

	// 计算False Easting
	falseEasting := float64(zone)*1000000 + 500000

	sr.WKT = fmt.Sprintf(`PROJCS["CGCS2000_3_Degree_GK_Zone_%d",GEOGCS["GCS_China_Geodetic_Coordinate_System_2000",DATUM["D_China_2000",SPHEROID["CGCS2000",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Gauss_Kruger"],PARAMETER["False_Easting",%v],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",%v],PARAMETER["Scale_Factor",1.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0],AUTHORITY["EPSG",%d]]`,
		zone, falseEasting, centralMeridian, wkid)

	sr.IsProjected = true
	// 根据带号设置原点
	sr.XOrigin = float64(zone-1)*1000000 - 123200
	sr.YOrigin = -10002100
	sr.XYScale = 10000
	sr.XYTolerance = 0.001
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true

	return sr
}

// NewCGCS2000_6DegreeGK_Zone 创建CGCS2000 6度带高斯克吕格投影坐标系
// zone: 带号 (如18表示中央经线105度)
func NewCGCS2000_6DegreeGK_Zone(zone int) *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()

	// 计算WKID
	wkid := 4502 + (zone - 13)
	sr.WKID = wkid
	sr.LatestWKID = wkid

	// 计算中央经线
	centralMeridian := float64(zone*6 - 3)

	// 计算False Easting
	falseEasting := float64(zone)*1000000 + 500000

	sr.WKT = fmt.Sprintf(`PROJCS["CGCS2000_6_Degree_GK_Zone_%d",GEOGCS["GCS_China_Geodetic_Coordinate_System_2000",DATUM["D_China_2000",SPHEROID["CGCS2000",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Gauss_Kruger"],PARAMETER["False_Easting",%v],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",%v],PARAMETER["Scale_Factor",1.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0],AUTHORITY["EPSG",%d]]`,
		zone, falseEasting, centralMeridian, wkid)

	sr.IsProjected = true
	sr.XOrigin = float64(zone-1)*1000000 - 123200
	sr.YOrigin = -10002100
	sr.XYScale = 10000
	sr.XYTolerance = 0.001
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true

	return sr
}

// NewWGS84SpatialReference 创建WGS84地理坐标系空间参考
func NewWGS84SpatialReference() *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()
	sr.WKID = 4326
	sr.LatestWKID = 4326
	sr.WKT = `GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433],AUTHORITY["EPSG",4326]]`
	sr.IsProjected = false
	sr.XOrigin = -400
	sr.YOrigin = -400
	sr.XYScale = 1000000000
	sr.XYTolerance = 8.983152841195215e-09
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true
	return sr
}

// NewWebMercatorSpatialReference 创建Web墨卡托投影坐标系空间参考
func NewWebMercatorSpatialReference() *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()
	sr.WKID = 3857
	sr.LatestWKID = 3857
	sr.WKT = `PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0],AUTHORITY["EPSG",3857]]`
	sr.IsProjected = true
	sr.XOrigin = -20037700
	sr.YOrigin = -30241100
	sr.XYScale = 10000
	sr.XYTolerance = 0.001
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true
	return sr
}

// NewUTMSpatialReference 创建UTM投影坐标系空间参考
// zone: UTM带号 (1-60)
// isNorth: 是否为北半球
func NewUTMSpatialReference(zone int, isNorth bool) *GDBSpatialReferenceWrite {
	sr := NewGDBSpatialReferenceWrite()

	var wkid int
	var hemisphere string
	var falseNorthing float64

	if isNorth {
		wkid = 32600 + zone
		hemisphere = "N"
		falseNorthing = 0
	} else {
		wkid = 32700 + zone
		hemisphere = "S"
		falseNorthing = 10000000
	}

	sr.WKID = wkid
	sr.LatestWKID = wkid

	centralMeridian := float64(zone*6 - 183)

	sr.WKT = fmt.Sprintf(`PROJCS["WGS_1984_UTM_Zone_%d%s",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],PARAMETER["False_Northing",%v],PARAMETER["Central_Meridian",%v],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0],AUTHORITY["EPSG",%d]]`,
		zone, hemisphere, falseNorthing, centralMeridian, wkid)

	sr.IsProjected = true
	sr.XOrigin = -5120900
	sr.YOrigin = -9998100
	sr.XYScale = 10000
	sr.XYTolerance = 0.001
	sr.ZOrigin = -100000
	sr.ZScale = 10000
	sr.ZTolerance = 0.001
	sr.MOrigin = -100000
	sr.MScale = 10000
	sr.MTolerance = 0.001
	sr.HighPrecision = true

	return sr
}

// =====================================================
// 从EPSG代码创建空间参考
// =====================================================

// NewSpatialReferenceFromEPSG 从EPSG代码创建空间参考
func NewSpatialReferenceFromEPSG(epsg int) (*GDBSpatialReferenceWrite, error) {

	// 创建空间参考对象
	hSRS := C.OSRNewSpatialReference(nil)
	if hSRS == nil {
		return nil, fmt.Errorf("无法创建空间参考对象")
	}
	defer C.OSRDestroySpatialReference(hSRS)

	// 从EPSG导入
	if C.OSRImportFromEPSG(hSRS, C.int(epsg)) != 0 {
		return nil, fmt.Errorf("无法从EPSG %d导入空间参考", epsg)
	}

	// 提取空间参考信息
	sr := extractSpatialReferenceWrite(hSRS)
	sr.WKID = epsg
	sr.LatestWKID = epsg

	return sr, nil
}

// NewSpatialReferenceFromWKT 从WKT创建空间参考
func NewSpatialReferenceFromWKT(wkt string) (*GDBSpatialReferenceWrite, error) {

	// 创建空间参考对象
	hSRS := C.OSRNewSpatialReference(nil)
	if hSRS == nil {
		return nil, fmt.Errorf("无法创建空间参考对象")
	}
	defer C.OSRDestroySpatialReference(hSRS)

	// 从WKT导入
	cWKT := C.CString(wkt)
	defer C.free(unsafe.Pointer(cWKT))

	pWKT := &cWKT
	if C.OSRImportFromWkt(hSRS, pWKT) != 0 {
		return nil, fmt.Errorf("无法从WKT导入空间参考")
	}

	// 提取空间参考信息
	sr := extractSpatialReferenceWrite(hSRS)
	sr.WKT = wkt

	return sr, nil
}

// =====================================================
// 辅助函数：打印元数据信息
// =====================================================

// PrintMetadataInfo 打印元数据信息（用于调试）
func (m *GDBLayerMetadataWrite) PrintMetadataInfo() {
	fmt.Println("========== GDB图层元数据 ==========")
	fmt.Printf("图层名称: %s\n", m.Name)
	fmt.Printf("图层别名: %s\n", m.AliasName)
	fmt.Printf("目录路径: %s\n", m.CatalogPath)
	fmt.Printf("数据集ID: %d\n", m.DSID)
	fmt.Printf("数据集类型: %s\n", m.DatasetType)
	fmt.Printf("要素类型: %s\n", m.FeatureType)
	fmt.Printf("几何类型: %s\n", m.ShapeType)
	fmt.Printf("几何字段: %s\n", m.ShapeFieldName)
	fmt.Printf("OID字段: %s\n", m.OIDFieldName)
	fmt.Printf("HasZ: %v, HasM: %v\n", m.HasZ, m.HasM)
	fmt.Printf("HasSpatialIndex: %v\n", m.HasSpatialIndex)

	if m.SpatialReference != nil {
		fmt.Println("\n--- 空间参考 ---")
		fmt.Printf("WKID: %d\n", m.SpatialReference.WKID)
		fmt.Printf("是否投影: %v\n", m.SpatialReference.IsProjected)
		fmt.Printf("XY容差: %v\n", m.SpatialReference.XYTolerance)
		if len(m.SpatialReference.WKT) > 100 {
			fmt.Printf("WKT: %s...\n", m.SpatialReference.WKT[:100])
		} else {
			fmt.Printf("WKT: %s\n", m.SpatialReference.WKT)
		}
	}

	if m.Extent != nil {
		fmt.Println("\n--- 范围 ---")
		fmt.Printf("XMin: %v, YMin: %v\n", m.Extent.XMin, m.Extent.YMin)
		fmt.Printf("XMax: %v, YMax: %v\n", m.Extent.XMax, m.Extent.YMax)
	}

	fmt.Println("\n--- 字段列表 ---")
	fmt.Printf("字段数量: %d\n", len(m.Fields))
	for i, field := range m.Fields {
		fmt.Printf("%d. %s (%s) - 别名: %s, 可空: %v\n",
			i+1, field.Name, field.FieldType, field.AliasName, field.IsNullable)
	}
	fmt.Println("====================================")
}

// GenerateDefinitionXMLFormatted 生成格式化的XML（用于调试）
func (m *GDBLayerMetadataWrite) GenerateDefinitionXMLFormatted() (string, error) {
	xml, err := m.GenerateDefinitionXML()
	if err != nil {
		return "", err
	}
	// 简单格式化：在主要标签后添加换行
	formatted := xml
	formatted = strings.ReplaceAll(formatted, "><", ">\n<")

	return formatted, nil
}

// =====================================================
// 验证函数
// =====================================================

// ValidateMetadata 验证元数据完整性
func (m *GDBLayerMetadataWrite) ValidateMetadata() []string {
	var errors []string

	if m.Name == "" {
		errors = append(errors, "图层名称不能为空")
	}

	if m.ShapeType == "" {
		errors = append(errors, "几何类型不能为空")
	}

	if m.ShapeFieldName == "" {
		errors = append(errors, "几何字段名不能为空")
	}

	if m.HasOID && m.OIDFieldName == "" {
		errors = append(errors, "启用OID时OID字段名不能为空")
	}

	// 验证字段
	fieldNames := make(map[string]bool)
	for _, field := range m.Fields {
		if field.Name == "" {
			errors = append(errors, "字段名不能为空")
			continue
		}

		if fieldNames[field.Name] {
			errors = append(errors, fmt.Sprintf("字段名重复: %s", field.Name))
		}
		fieldNames[field.Name] = true
		if field.FieldType == "" {
			errors = append(errors, fmt.Sprintf("字段 %s 的类型不能为空", field.Name))
		}
	}

	return errors
}

// =====================================================
// 复制和克隆函数
// =====================================================

// Clone 克隆元数据对象
func (m *GDBLayerMetadataWrite) Clone() *GDBLayerMetadataWrite {
	clone := &GDBLayerMetadataWrite{
		Name:                             m.Name,
		AliasName:                        m.AliasName,
		CatalogPath:                      m.CatalogPath,
		DSID:                             m.DSID,
		LayerPath:                        m.LayerPath,
		DatasetType:                      m.DatasetType,
		FeatureType:                      m.FeatureType,
		Versioned:                        m.Versioned,
		CanVersion:                       m.CanVersion,
		ConfigurationKeyword:             m.ConfigurationKeyword,
		RequiredGeodatabaseClientVersion: m.RequiredGeodatabaseClientVersion,
		ShapeType:                        m.ShapeType,
		ShapeFieldName:                   m.ShapeFieldName,
		HasM:                             m.HasM,
		HasZ:                             m.HasZ,
		HasSpatialIndex:                  m.HasSpatialIndex,
		HasOID:                           m.HasOID,
		OIDFieldName:                     m.OIDFieldName,
		HasGlobalID:                      m.HasGlobalID,
		GlobalIDFieldName:                m.GlobalIDFieldName,
		CLSID:                            m.CLSID,
		EXTCLSID:                         m.EXTCLSID,
		AreaFieldName:                    m.AreaFieldName,
		LengthFieldName:                  m.LengthFieldName,
		EditorTrackingEnabled:            m.EditorTrackingEnabled,
		CreatorFieldName:                 m.CreatorFieldName,
		CreatedAtFieldName:               m.CreatedAtFieldName,
		EditorFieldName:                  m.EditorFieldName,
		EditedAtFieldName:                m.EditedAtFieldName,
		IsTimeInUTC:                      m.IsTimeInUTC,
		ChangeTracked:                    m.ChangeTracked,
		FieldFilteringEnabled:            m.FieldFilteringEnabled,
		RasterFieldName:                  m.RasterFieldName,
	}

	// 复制字段
	clone.Fields = make([]GDBFieldMetadata, len(m.Fields))
	copy(clone.Fields, m.Fields)

	// 复制空间参考
	if m.SpatialReference != nil {
		clone.SpatialReference = &GDBSpatialReferenceWrite{
			WKT:           m.SpatialReference.WKT,
			WKID:          m.SpatialReference.WKID,
			LatestWKID:    m.SpatialReference.LatestWKID,
			XOrigin:       m.SpatialReference.XOrigin,
			YOrigin:       m.SpatialReference.YOrigin,
			XYScale:       m.SpatialReference.XYScale,
			ZOrigin:       m.SpatialReference.ZOrigin,
			ZScale:        m.SpatialReference.ZScale,
			MOrigin:       m.SpatialReference.MOrigin,
			MScale:        m.SpatialReference.MScale,
			XYTolerance:   m.SpatialReference.XYTolerance,
			ZTolerance:    m.SpatialReference.ZTolerance,
			MTolerance:    m.SpatialReference.MTolerance,
			HighPrecision: m.SpatialReference.HighPrecision,
			IsProjected:   m.SpatialReference.IsProjected,
		}
	}

	// 复制范围
	if m.Extent != nil {
		clone.Extent = &GDBExtentInfo{
			XMin: m.Extent.XMin,
			YMin: m.Extent.YMin,
			XMax: m.Extent.XMax,
			YMax: m.Extent.YMax,
		}
	}

	return clone
}

// =====================================================
// 新增：GDB_ItemRelationships 相关函数
// =====================================================

// GDBItemRelationship 表示GDB_ItemRelationships表中的一条记录
type GDBItemRelationship struct {
	ObjectID   int    // 对象ID
	UUID       string // 关系UUID
	Type       string // 关系类型UUID
	OriginID   string // 源项UUID (父级，如数据集)
	DestID     string // 目标项UUID (子级，如要素类)
	Attributes string // 属性
	Properties int    // 属性值
}

// GDB关系类型UUID常量
const (
	// DatasetInFolder - 数据集在文件夹中的关系
	GDBRelTypeDatasetInFolder = "{dc78f1ab-34e4-43ac-ba81-bc99dbe3e549}"
	// DatasetInFeatureDataset - 要素类在要素数据集中的关系
	GDBRelTypeDatasetInFeatureDataset = "{a1633a59-46ba-4448-8706-d8abe2b2b02e}"
	// ItemInFolder - 项目在文件夹中的关系
	GDBRelTypeItemInFolder = "{5dd0c1af-cb3d-4fea-8c51-cb3ba8d77cdb}"
)

// GDB_Items类型UUID常量
const (
	GDBItemTypeFeatureDataset = "{74737149-DCB5-4257-8904-B9724E32A530}"
	GDBItemTypeFeatureClass   = "{70737809-852C-4A03-9E22-2CECEA5B9BFA}"
	GDBItemTypeTable          = "{CD06BC3B-789D-4C51-AAFA-A467912B8965}"
	GDBItemTypeWorkspace      = "{C673FE0F-7280-404F-8532-20755DD8FC06}"
)

// GetGDBItemUUID 获取GDB_Items表中指定项目的UUID
func GetGDBItemUUID(gdbPath string, itemName string) (string, error) {

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return "", fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return "", fmt.Errorf("无法获取GDB_Items表")
	}

	// 设置过滤器
	filterSQL := fmt.Sprintf("Name = '%s'", itemName)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	hFeature := C.OGR_L_GetNextFeature(hLayer)
	if hFeature == nil {
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		return "", fmt.Errorf("未找到项目: %s", itemName)
	}
	defer C.OGR_F_Destroy(hFeature)

	// 获取UUID字段
	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	cUUID := C.CString("UUID")
	defer C.free(unsafe.Pointer(cUUID))

	uuidIdx := C.OGR_FD_GetFieldIndex(hDefn, cUUID)
	if uuidIdx < 0 {
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		return "", fmt.Errorf("GDB_Items表中不存在UUID字段")
	}

	uuid := C.GoString(C.OGR_F_GetFieldAsString(hFeature, uuidIdx))
	C.OGR_L_SetAttributeFilter(hLayer, nil)

	return uuid, nil
}

// GetGDBRootUUID 获取GDB根目录的UUID
func GetGDBRootUUID(gdbPath string) (string, error) {

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04))
	if hDS == nil {
		return "", fmt.Errorf("无法打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItems := C.CString("GDB_Items")
	defer C.free(unsafe.Pointer(cGDBItems))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItems)
	if hLayer == nil {
		return "", fmt.Errorf("无法获取GDB_Items表")
	}

	// 查找根目录 (Type = Workspace类型 或 Path = '\')
	filterSQL := fmt.Sprintf("Type = '%s'", GDBItemTypeWorkspace)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	hFeature := C.OGR_L_GetNextFeature(hLayer)
	if hFeature == nil {
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		return "", fmt.Errorf("未找到GDB根目录")
	}
	defer C.OGR_F_Destroy(hFeature)

	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	cUUID := C.CString("UUID")
	defer C.free(unsafe.Pointer(cUUID))

	uuidIdx := C.OGR_FD_GetFieldIndex(hDefn, cUUID)
	if uuidIdx < 0 {
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		return "", fmt.Errorf("GDB_Items表中不存在UUID字段")
	}

	uuid := C.GoString(C.OGR_F_GetFieldAsString(hFeature, uuidIdx))
	C.OGR_L_SetAttributeFilter(hLayer, nil)

	return uuid, nil
}

// AddGDBItemRelationship 在GDB_ItemRelationships表中添加关系
func AddGDBItemRelationship(gdbPath string, originUUID string, destUUID string, relationType string) error {

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return fmt.Errorf("无法以更新模式打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItemRels := C.CString("GDB_ItemRelationships")
	defer C.free(unsafe.Pointer(cGDBItemRels))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItemRels)
	if hLayer == nil {
		return fmt.Errorf("无法获取GDB_ItemRelationships表")
	}

	// 检查关系是否已存在
	filterSQL := fmt.Sprintf("OriginID = '%s' AND DestID = '%s' AND Type = '%s'",
		originUUID, destUUID, relationType)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	existingFeature := C.OGR_L_GetNextFeature(hLayer)
	if existingFeature != nil {
		C.OGR_F_Destroy(existingFeature)
		C.OGR_L_SetAttributeFilter(hLayer, nil)
		fmt.Println("关系已存在，跳过添加")
		return nil
	}
	C.OGR_L_SetAttributeFilter(hLayer, nil)

	// 创建新要素
	hDefn := C.OGR_L_GetLayerDefn(hLayer)
	hFeature := C.OGR_F_Create(hDefn)
	if hFeature == nil {
		return fmt.Errorf("无法创建要素")
	}
	defer C.OGR_F_Destroy(hFeature)

	// 生成新的UUID
	newUUID := generateUUID()

	// 设置字段值
	setStringField := func(fieldName, value string) {
		cFieldName := C.CString(fieldName)
		defer C.free(unsafe.Pointer(cFieldName))
		idx := C.OGR_FD_GetFieldIndex(hDefn, cFieldName)
		if idx >= 0 {
			cValue := C.CString(value)
			defer C.free(unsafe.Pointer(cValue))
			C.OGR_F_SetFieldString(hFeature, idx, cValue)
		}
	}

	setIntField := func(fieldName string, value int) {
		cFieldName := C.CString(fieldName)
		defer C.free(unsafe.Pointer(cFieldName))
		idx := C.OGR_FD_GetFieldIndex(hDefn, cFieldName)
		if idx >= 0 {
			C.OGR_F_SetFieldInteger(hFeature, idx, C.int(value))
		}
	}

	setStringField("UUID", newUUID)
	setStringField("Type", relationType)
	setStringField("OriginID", originUUID)
	setStringField("DestID", destUUID)
	setIntField("Properties", 1)

	// 创建要素
	if C.OGR_L_CreateFeature(hLayer, hFeature) != 0 {
		return fmt.Errorf("创建关系记录失败")
	}

	C.OGR_L_SyncToDisk(hLayer)
	C.GDALFlushCache(hDS)

	fmt.Printf("成功添加关系: %s -> %s\n", originUUID, destUUID)
	return nil
}

// UpdateGDBItemRelationship 更新要素类的父级关系
// 将要素类从当前位置移动到指定的数据集中
func UpdateGDBItemRelationship(gdbPath string, featureClassName string, datasetName string) error {
	// 获取要素类的UUID
	featureClassUUID, err := GetGDBItemUUID(gdbPath, featureClassName)
	if err != nil {
		return fmt.Errorf("获取要素类UUID失败: %w", err)
	}

	var parentUUID string
	var relationType string

	if datasetName == "" {
		// 移动到根目录
		parentUUID, err = GetGDBRootUUID(gdbPath)
		if err != nil {
			return fmt.Errorf("获取根目录UUID失败: %w", err)
		}
		relationType = GDBRelTypeDatasetInFolder
	} else {
		// 移动到指定数据集
		parentUUID, err = GetGDBItemUUID(gdbPath, datasetName)
		if err != nil {
			return fmt.Errorf("获取数据集UUID失败: %w", err)
		}
		relationType = GDBRelTypeDatasetInFeatureDataset
	}

	// 先删除现有的父级关系
	err = RemoveGDBItemRelationship(gdbPath, featureClassUUID)
	if err != nil {
		fmt.Printf("警告: 删除现有关系失败: %v\n", err)
	}

	// 添加新的父级关系
	return AddGDBItemRelationship(gdbPath, parentUUID, featureClassUUID, relationType)
}

// RemoveGDBItemRelationship 删除指定目标项的所有父级关系
func RemoveGDBItemRelationship(gdbPath string, destUUID string) error {

	cPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cPath))

	hDS := C.openDatasetExUpdate(cPath, C.uint(0x04|0x01))
	if hDS == nil {
		return fmt.Errorf("无法以更新模式打开GDB数据集: %s", gdbPath)
	}
	defer C.closeDataset(hDS)

	cGDBItemRels := C.CString("GDB_ItemRelationships")
	defer C.free(unsafe.Pointer(cGDBItemRels))

	hLayer := C.GDALDatasetGetLayerByName(hDS, cGDBItemRels)
	if hLayer == nil {
		return fmt.Errorf("无法获取GDB_ItemRelationships表")
	}

	// 查找并删除所有以destUUID为目标的关系
	filterSQL := fmt.Sprintf("DestID = '%s'", destUUID)
	cFilter := C.CString(filterSQL)
	defer C.free(unsafe.Pointer(cFilter))

	C.OGR_L_SetAttributeFilter(hLayer, cFilter)
	C.OGR_L_ResetReading(hLayer)

	var fidsToDelete []int64
	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}
		fid := int64(C.OGR_F_GetFID(hFeature))
		fidsToDelete = append(fidsToDelete, fid)
		C.OGR_F_Destroy(hFeature)
	}

	C.OGR_L_SetAttributeFilter(hLayer, nil)

	// 删除找到的要素
	for _, fid := range fidsToDelete {
		if C.OGR_L_DeleteFeature(hLayer, C.GIntBig(fid)) != 0 {
			fmt.Printf("警告: 删除FID %d 失败\n", fid)
		}
	}

	C.OGR_L_SyncToDisk(hLayer)
	C.GDALFlushCache(hDS)

	fmt.Printf("已删除 %d 条关系记录\n", len(fidsToDelete))
	return nil
}

// generateUUID 生成UUID字符串
func generateUUID() string {
	// 简单的UUID生成，实际使用中建议使用uuid库
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(i * 17 % 256)
	}
	return fmt.Sprintf("{%08X-%04X-%04X-%04X-%012X}",
		uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
		uint16(b[8])<<8|uint16(b[9]),
		uint64(b[10])<<40|uint64(b[11])<<32|uint64(b[12])<<24|uint64(b[13])<<16|uint64(b[14])<<8|uint64(b[15]))
}
