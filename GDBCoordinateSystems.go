package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type GDBSpatialReferenceType int

const (
	SRSTypeGeographic GDBSpatialReferenceType = iota // 地理坐标系
	SRSTypeProjected                                 // 投影坐标系
)

// GDBSpatialReference 空间参考系统结构
type GDBSpatialReference struct {
	EPSG        int                     // EPSG代码
	Name        string                  // 坐标系名称
	Type        GDBSpatialReferenceType // 坐标系类型
	Description string                  // 描述信息
	WKT         string                  // WKT定义（可选，用于自定义坐标系）
	Proj4       string                  // Proj4定义（可选）
}

// =====================================================
// 预定义坐标系常量
// =====================================================
// 地理坐标系
var (
	// WGS84 地理坐标系
	SRS_WGS84 = &GDBSpatialReference{
		EPSG:        4326,
		Name:        "WGS 84",
		Type:        SRSTypeGeographic,
		Description: "WGS 84 地理坐标系"}
	// CGCS2000 地理坐标系
	SRS_CGCS2000 = &GDBSpatialReference{
		EPSG:        4490,
		Name:        "China Geodetic Coordinate System 2000",
		Type:        SRSTypeGeographic,
		Description: "中国2000国家大地坐标系（地理坐标系）",
	}
)

// CGCS2000 3度带投影坐标系 (EPSG: 4513-4533)
// 中央经线从75°到135°，每3度一个带
var (
	// 25带 中央经线75°
	SRS_CGCS2000_3_25 = &GDBSpatialReference{
		EPSG:        4513,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 25",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 25带 (中央经线75°)",
	}
	// 26带 中央经线78°
	SRS_CGCS2000_3_26 = &GDBSpatialReference{
		EPSG:        4514,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 26",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 26带 (中央经线78°)",
	}
	// 27带 中央经线81°
	SRS_CGCS2000_3_27 = &GDBSpatialReference{
		EPSG:        4515,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 27",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 27带 (中央经线81°)",
	}
	// 28带 中央经线84°
	SRS_CGCS2000_3_28 = &GDBSpatialReference{
		EPSG:        4516,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 28",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 28带 (中央经线84°)",
	}
	// 29带 中央经线87°
	SRS_CGCS2000_3_29 = &GDBSpatialReference{
		EPSG:        4517,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 29",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 29带 (中央经线87°)",
	}
	// 30带 中央经线90°
	SRS_CGCS2000_3_30 = &GDBSpatialReference{
		EPSG:        4518,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 30",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 30带 (中央经线90°)",
	}
	// 31带 中央经线93°
	SRS_CGCS2000_3_31 = &GDBSpatialReference{
		EPSG:        4519,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 31",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 31带 (中央经线93°)",
	}
	// 32带 中央经线96°
	SRS_CGCS2000_3_32 = &GDBSpatialReference{
		EPSG:        4520,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 32",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 32带 (中央经线96°)",
	}
	// 33带 中央经线99°
	SRS_CGCS2000_3_33 = &GDBSpatialReference{
		EPSG:        4521,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 33",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 33带 (中央经线99°)",
	}
	// 34带 中央经线102°
	SRS_CGCS2000_3_34 = &GDBSpatialReference{
		EPSG:        4522,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 34",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 34带 (中央经线102°)",
	}
	// 35带 中央经线105°
	SRS_CGCS2000_3_35 = &GDBSpatialReference{
		EPSG:        4523,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 35",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 35带 (中央经线105°)",
	}
	// 36带 中央经线108°
	SRS_CGCS2000_3_36 = &GDBSpatialReference{
		EPSG:        4524,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 36",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 36带 (中央经线108°)",
	}
	// 37带 中央经线111°
	SRS_CGCS2000_3_37 = &GDBSpatialReference{
		EPSG:        4525,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 37",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 37带 (中央经线111°)",
	}
	// 38带 中央经线114°
	SRS_CGCS2000_3_38 = &GDBSpatialReference{
		EPSG:        4526,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 38",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 38带 (中央经线114°)",
	}
	// 39带 中央经线117°
	SRS_CGCS2000_3_39 = &GDBSpatialReference{
		EPSG:        4527,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 39",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 39带 (中央经线117°)",
	}
	// 40带 中央经线120°
	SRS_CGCS2000_3_40 = &GDBSpatialReference{
		EPSG:        4528,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 40",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 40带 (中央经线120°)",
	}
	// 41带 中央经线123°
	SRS_CGCS2000_3_41 = &GDBSpatialReference{
		EPSG:        4529,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 41",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 41带 (中央经线123°)",
	}
	// 42带 中央经线126°
	SRS_CGCS2000_3_42 = &GDBSpatialReference{
		EPSG:        4530,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 42",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 42带 (中央经线126°)",
	}
	// 43带 中央经线129°
	SRS_CGCS2000_3_43 = &GDBSpatialReference{
		EPSG:        4531,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 43",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 43带 (中央经线129°)",
	}
	// 44带 中央经线132°
	SRS_CGCS2000_3_44 = &GDBSpatialReference{
		EPSG:        4532,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 44",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 44带 (中央经线132°)",
	}
	// 45带 中央经线135°
	SRS_CGCS2000_3_45 = &GDBSpatialReference{
		EPSG:        4533,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger zone 45",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 45带 (中央经线135°)",
	}
)

// CGCS2000 3度带投影坐标系（带带号前缀）(EPSG: 4534-4554)
var (
	// 25带 中央经线75° (带带号前缀)
	SRS_CGCS2000_3_CM_75E = &GDBSpatialReference{
		EPSG:        4534,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 75E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线75° (带带号前缀)",
	}
	// 26带 中央经线78° (带带号前缀)
	SRS_CGCS2000_3_CM_78E = &GDBSpatialReference{
		EPSG:        4535,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 78E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线78° (带带号前缀)",
	}
	// 27带 中央经线81° (带带号前缀)
	SRS_CGCS2000_3_CM_81E = &GDBSpatialReference{
		EPSG:        4536,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 81E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线81° (带带号前缀)",
	}
	// 28带 中央经线84° (带带号前缀)
	SRS_CGCS2000_3_CM_84E = &GDBSpatialReference{
		EPSG:        4537,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 84E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线84° (带带号前缀)",
	}
	// 29带 中央经线87° (带带号前缀)
	SRS_CGCS2000_3_CM_87E = &GDBSpatialReference{
		EPSG:        4538,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 87E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线87° (带带号前缀)",
	}
	// 30带 中央经线90° (带带号前缀)
	SRS_CGCS2000_3_CM_90E = &GDBSpatialReference{
		EPSG:        4539,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 90E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线90° (带带号前缀)",
	}
	// 31带 中央经线93° (带带号前缀)
	SRS_CGCS2000_3_CM_93E = &GDBSpatialReference{
		EPSG:        4540,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 93E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线93° (带带号前缀)",
	}
	// 32带 中央经线96° (带带号前缀)
	SRS_CGCS2000_3_CM_96E = &GDBSpatialReference{
		EPSG:        4541,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 96E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线96° (带带号前缀)",
	}
	// 33带 中央经线99° (带带号前缀)
	SRS_CGCS2000_3_CM_99E = &GDBSpatialReference{
		EPSG:        4542,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 99E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线99° (带带号前缀)",
	}
	// 34带 中央经线102° (带带号前缀)
	SRS_CGCS2000_3_CM_102E = &GDBSpatialReference{
		EPSG:        4543,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 102E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线102° (带带号前缀)",
	}
	// 35带 中央经线105° (带带号前缀)
	SRS_CGCS2000_3_CM_105E = &GDBSpatialReference{
		EPSG:        4544,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 105E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线105° (带带号前缀)",
	}
	// 36带 中央经线108° (带带号前缀)
	SRS_CGCS2000_3_CM_108E = &GDBSpatialReference{
		EPSG:        4545,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 108E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线108° (带带号前缀)",
	}
	// 37带 中央经线111° (带带号前缀)
	SRS_CGCS2000_3_CM_111E = &GDBSpatialReference{
		EPSG:        4546,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 111E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线111° (带带号前缀)",
	}
	// 38带 中央经线114° (带带号前缀)
	SRS_CGCS2000_3_CM_114E = &GDBSpatialReference{
		EPSG:        4547,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 114E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线114° (带带号前缀)",
	}
	// 39带 中央经线117° (带带号前缀)
	SRS_CGCS2000_3_CM_117E = &GDBSpatialReference{
		EPSG:        4548,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 117E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线117° (带带号前缀)",
	}
	// 40带 中央经线120° (带带号前缀)
	SRS_CGCS2000_3_CM_120E = &GDBSpatialReference{
		EPSG:        4549,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 120E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线120° (带带号前缀)",
	}
	// 41带 中央经线123° (带带号前缀)
	SRS_CGCS2000_3_CM_123E = &GDBSpatialReference{
		EPSG:        4550,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 123E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线123° (带带号前缀)",
	}
	// 42带 中央经线126° (带带号前缀)
	SRS_CGCS2000_3_CM_126E = &GDBSpatialReference{
		EPSG:        4551,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 126E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线126° (带带号前缀)",
	}
	// 43带 中央经线129° (带带号前缀)
	SRS_CGCS2000_3_CM_129E = &GDBSpatialReference{
		EPSG:        4552,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 129E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线129° (带带号前缀)",
	}
	// 44带 中央经线132° (带带号前缀)
	SRS_CGCS2000_3_CM_132E = &GDBSpatialReference{
		EPSG:        4553,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 132E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线132° (带带号前缀)",
	}
	// 45带 中央经线135° (带带号前缀)
	SRS_CGCS2000_3_CM_135E = &GDBSpatialReference{
		EPSG:        4554,
		Name:        "CGCS2000 / 3-degree Gauss-Kruger CM 135E",
		Type:        SRSTypeProjected,
		Description: "CGCS2000 3度带 中央经线135° (带带号前缀)",
	}
)

// =====================================================
// 坐标系辅助函数
// =====================================================
// CGCS2000_3DegreeZoneMap CGCS2000 3度带映射表
var CGCS2000_3DegreeZoneMap = map[int]*GDBSpatialReference{
	25: SRS_CGCS2000_3_25,
	26: SRS_CGCS2000_3_26,
	27: SRS_CGCS2000_3_27,
	28: SRS_CGCS2000_3_28,
	29: SRS_CGCS2000_3_29,
	30: SRS_CGCS2000_3_30,
	31: SRS_CGCS2000_3_31,
	32: SRS_CGCS2000_3_32,
	33: SRS_CGCS2000_3_33,
	34: SRS_CGCS2000_3_34,
	35: SRS_CGCS2000_3_35,
	36: SRS_CGCS2000_3_36,
	37: SRS_CGCS2000_3_37,
	38: SRS_CGCS2000_3_38,
	39: SRS_CGCS2000_3_39,
	40: SRS_CGCS2000_3_40,
	41: SRS_CGCS2000_3_41,
	42: SRS_CGCS2000_3_42,
	43: SRS_CGCS2000_3_43,
	44: SRS_CGCS2000_3_44,
	45: SRS_CGCS2000_3_45,
}

// CGCS2000_3DegreeCMMap CGCS2000 3度带（带带号前缀）按中央经线映射表
var CGCS2000_3DegreeCMMap = map[int]*GDBSpatialReference{
	75:  SRS_CGCS2000_3_CM_75E,
	78:  SRS_CGCS2000_3_CM_78E,
	81:  SRS_CGCS2000_3_CM_81E,
	84:  SRS_CGCS2000_3_CM_84E,
	87:  SRS_CGCS2000_3_CM_87E,
	90:  SRS_CGCS2000_3_CM_90E,
	93:  SRS_CGCS2000_3_CM_93E,
	96:  SRS_CGCS2000_3_CM_96E,
	99:  SRS_CGCS2000_3_CM_99E,
	102: SRS_CGCS2000_3_CM_102E,
	105: SRS_CGCS2000_3_CM_105E,
	108: SRS_CGCS2000_3_CM_108E,
	111: SRS_CGCS2000_3_CM_111E,
	114: SRS_CGCS2000_3_CM_114E,
	117: SRS_CGCS2000_3_CM_117E,
	120: SRS_CGCS2000_3_CM_120E,
	123: SRS_CGCS2000_3_CM_123E,
	126: SRS_CGCS2000_3_CM_126E,
	129: SRS_CGCS2000_3_CM_129E,
	132: SRS_CGCS2000_3_CM_132E,
	135: SRS_CGCS2000_3_CM_135E,
}

// GetCGCS2000_3DegreeZone 根据带号获取CGCS2000 3度带坐标系
// zone: 带号 (25-45)
func GetCGCS2000_3DegreeZone(zone int) (*GDBSpatialReference, error) {
	if srs, ok := CGCS2000_3DegreeZoneMap[zone]; ok {
		return srs, nil
	}
	return nil, fmt.Errorf("无效的CGCS2000 3度带带号: %d (有效范围: 25-45)", zone)
}

// GetCGCS2000_3DegreeByCentralMeridian 根据中央经线获取CGCS2000 3度带坐标系（带带号前缀）
// centralMeridian: 中央经线 (75, 78, 81, ..., 135)
func GetCGCS2000_3DegreeByCentralMeridian(centralMeridian int) (*GDBSpatialReference, error) {
	if srs, ok := CGCS2000_3DegreeCMMap[centralMeridian]; ok {
		return srs, nil
	}
	return nil, fmt.Errorf("无效的中央经线: %d (有效值: 75, 78, 81, ..., 135)", centralMeridian)
}

// GetCGCS2000_3DegreeByLongitude 根据经度自动计算并获取对应的CGCS2000 3度带坐标系
// longitude: 经度值
// withZonePrefix: 是否使用带带号前缀的坐标系
func GetCGCS2000_3DegreeByLongitude(longitude float64, withZonePrefix bool) (*GDBSpatialReference, error) {
	// 计算中央经线
	// 3度带中央经线 = 3 * 带号
	// 带号 = round(经度 / 3)
	zone := int((longitude + 1.5) / 3)
	centralMeridian := zone * 3
	if centralMeridian < 75 || centralMeridian > 135 {
		return nil, fmt.Errorf("经度 %.2f 超出CGCS2000 3度带覆盖范围", longitude)
	}
	if withZonePrefix {
		return GetCGCS2000_3DegreeByCentralMeridian(centralMeridian)
	}
	// 计算带号
	zoneNumber := centralMeridian / 3
	return GetCGCS2000_3DegreeZone(zoneNumber)
}

// NewGDBSpatialReferenceFromEPSG 根据EPSG代码创建空间参考
func NewGDBSpatialReferenceFromEPSG(epsg int) *GDBSpatialReference {
	return &GDBSpatialReference{
		EPSG:        epsg,
		Name:        fmt.Sprintf("EPSG:%d", epsg),
		Type:        SRSTypeProjected, // 默认为投影坐标系
		Description: fmt.Sprintf("EPSG代码: %d", epsg),
	}
}

// NewGDBSpatialReferenceFromWKT 根据WKT创建空间参考
func NewGDBSpatialReferenceFromWKT(wkt string, name string) *GDBSpatialReference {
	return &GDBSpatialReference{
		EPSG:        0,
		Name:        name,
		Type:        SRSTypeProjected,
		Description: "自定义WKT坐标系",
		WKT:         wkt,
	}
}

// NewGDBSpatialReferenceFromProj4 根据Proj4创建空间参考
func NewGDBSpatialReferenceFromProj4(proj4 string, name string) *GDBSpatialReference {
	return &GDBSpatialReference{
		EPSG:        0,
		Name:        name,
		Type:        SRSTypeProjected,
		Description: "自定义Proj4坐标系",
		Proj4:       proj4,
	}
}

// ToOGRGDBSpatialReference 将GDBSpatialReference转换为GDAL的OGRSpatialReferenceH
func (srs *GDBSpatialReference) ToOGRGDBSpatialReference() (C.OGRSpatialReferenceH, error) {
	ogrSRS := C.OSRNewSpatialReference(nil)
	if ogrSRS == nil {
		return nil, fmt.Errorf("无法创建OGRGDBSpatialReference")
	}
	var result C.OGRErr
	// 优先使用EPSG
	if srs.EPSG > 0 {
		result = C.OSRImportFromEPSG(ogrSRS, C.int(srs.EPSG))
		if result == C.OGRERR_NONE {
			return ogrSRS, nil
		}
	}
	// 尝试使用WKT
	if srs.WKT != "" {
		cWKT := C.CString(srs.WKT)
		defer C.free(unsafe.Pointer(cWKT))
		// 需要使用指向指针的指针
		result = C.OSRImportFromWkt(ogrSRS, &cWKT)
		if result == C.OGRERR_NONE {
			return ogrSRS, nil
		}
	}
	// 尝试使用Proj4
	if srs.Proj4 != "" {
		cProj4 := C.CString(srs.Proj4)
		defer C.free(unsafe.Pointer(cProj4))
		result = C.OSRImportFromProj4(ogrSRS, cProj4)
		if result == C.OGRERR_NONE {
			return ogrSRS, nil
		}
	}
	C.OSRDestroySpatialReference(ogrSRS)
	return nil, fmt.Errorf("无法创建空间参考系统: EPSG=%d, Name=%s", srs.EPSG, srs.Name)
}

// String 返回坐标系的字符串表示
func (srs *GDBSpatialReference) String() string {
	typeStr := "地理坐标系"
	if srs.Type == SRSTypeProjected {
		typeStr = "投影坐标系"
	}
	return fmt.Sprintf("%s (EPSG:%d) - %s", srs.Name, srs.EPSG, typeStr)
}

// GetAllCGCS2000_3DegreeZones 获取所有CGCS2000 3度带坐标系列表
func GetAllCGCS2000_3DegreeZones() []*GDBSpatialReference {
	zones := make([]*GDBSpatialReference, 0, 21)
	for i := 25; i <= 45; i++ {
		if srs, ok := CGCS2000_3DegreeZoneMap[i]; ok {
			zones = append(zones, srs)
		}
	}
	return zones
}

// GetAllCGCS2000_3DegreeCMZones 获取所有CGCS2000 3度带（带带号前缀）坐标系列表
func GetAllCGCS2000_3DegreeCMZones() []*GDBSpatialReference {
	zones := make([]*GDBSpatialReference, 0, 21)
	for cm := 75; cm <= 135; cm += 3 {
		if srs, ok := CGCS2000_3DegreeCMMap[cm]; ok {
			zones = append(zones, srs)
		}
	}
	return zones
}
