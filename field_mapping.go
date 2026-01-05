// Gogeo/field_mapping.go

package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// mapPostGISTypeToGDB 将PostGIS类型映射到GDB字段类型
// 返回: GDAL字段类型, 宽度, 精度
func mapPostGISTypeToGDB(pgType string) (FieldType, int, int) {
	// 转换为小写便于匹配
	pgType = strings.ToLower(strings.TrimSpace(pgType))

	// 解析类型定义
	baseType, params := parsePostGISType(pgType)

	// 类型映射
	switch baseType {
	// 整数类型
	case "smallint", "int2":
		return FieldTypeInteger, 0, 0

	case "integer", "int", "int4", "serial":
		return FieldTypeInteger, 0, 0

	case "bigint", "int8", "bigserial":
		return FieldTypeInteger64, 0, 0

	// 浮点数类型
	case "real", "float4":
		return FieldTypeReal, 0, 0

	case "double precision", "float8", "float":
		return FieldTypeReal, 0, 0

	case "numeric", "decimal":
		width := 18
		precision := 6
		if len(params) >= 1 {
			width = params[0]
		}
		if len(params) >= 2 {
			precision = params[1]
		}
		return FieldTypeReal, width, precision

	// 字符串类型
	case "character varying", "varchar":
		width := 254
		if len(params) >= 1 {
			width = params[0]
		}
		// GDB字符串最大长度限制
		if width > 2147483647 {
			width = 2147483647
		}
		return FieldTypeString, width, 0

	case "character", "char":
		width := 1
		if len(params) >= 1 {
			width = params[0]
		}
		return FieldTypeString, width, 0

	case "text":
		// text类型在GDB中使用较大的默认长度
		return FieldTypeString, 2147483647, 0

	// 日期时间类型
	case "date":
		return FieldTypeDate, 0, 0

	case "time", "time without time zone", "time with time zone", "timetz":
		return FieldTypeTime, 0, 0

	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return FieldTypeDateTime, 0, 0

	// 二进制类型
	case "bytea":
		return FieldTypeBinary, 0, 0

	// 布尔类型 - GDB不直接支持布尔，使用整数表示
	case "boolean", "bool":
		return FieldTypeInteger, 0, 0

	// UUID类型 - 作为字符串处理
	case "uuid":
		return FieldTypeString, 36, 0

	default:
		// 默认作为字符串处理
		width := 254
		if len(params) >= 1 {
			width = params[0]
		}
		return FieldTypeString, width, 0
	}
}

// parsePostGISType 解析PostgreSQL类型字符串
func parsePostGISType(pgType string) (baseType string, params []int) {
	// 使用正则表达式提取类型和参数
	re := regexp.MustCompile(`^([a-z\s]+)(?:$([^)]+)$)?$`)
	matches := re.FindStringSubmatch(pgType)

	if len(matches) < 2 {
		return pgType, nil
	}

	baseType = strings.TrimSpace(matches[1])

	if len(matches) >= 3 && matches[2] != "" {
		paramStrs := strings.Split(matches[2], ",")
		for _, p := range paramStrs {
			if val, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
				params = append(params, val)
			}
		}
	}

	return baseType, params
}

// mapGDBTypeToPostGIS 将GDB字段类型映射到PostgreSQL类型
func mapGDBTypeToPostGIS(gdalType FieldType, width, precision int) string {
	switch gdalType {
	case FieldTypeInteger:
		return "INTEGER"

	case FieldTypeInteger64:
		return "BIGINT"

	case FieldTypeReal:
		if precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", width, precision)
		}
		return "DOUBLE PRECISION"

	case FieldTypeString:
		if width > 0 && width <= 10485760 {
			return fmt.Sprintf("VARCHAR(%d)", width)
		}
		return "TEXT"

	case FieldTypeDate:
		return "DATE"

	case FieldTypeTime:
		return "TIME"

	case FieldTypeDateTime:
		return "TIMESTAMP"

	case FieldTypeBinary:
		return "BYTEA"

	default:
		return "TEXT"
	}
}

// FieldTypeCompatibility 字段类型兼容性检查结果
type FieldTypeCompatibility struct {
	IsCompatible    bool   // 是否兼容
	SourceType      string // 源类型
	TargetType      string // 目标类型
	ConversionNotes string // 转换说明
	DataLossRisk    bool   // 是否有数据丢失风险
}

// CheckFieldTypeCompatibility 检查字段类型在PostgreSQL和GDB之间的兼容性
func CheckFieldTypeCompatibility(pgType string) FieldTypeCompatibility {
	pgType = strings.ToLower(strings.TrimSpace(pgType))
	baseType, _ := parsePostGISType(pgType)

	result := FieldTypeCompatibility{
		SourceType: pgType,
	}

	switch baseType {
	case "smallint", "int2", "integer", "int", "int4", "serial":
		result.IsCompatible = true
		result.TargetType = "OFTInteger"
		result.ConversionNotes = "整数类型完全兼容"
		result.DataLossRisk = false

	case "bigint", "int8", "bigserial":
		result.IsCompatible = true
		result.TargetType = "OFTInteger64"
		result.ConversionNotes = "大整数类型完全兼容"
		result.DataLossRisk = false

	case "real", "float4":
		result.IsCompatible = true
		result.TargetType = "OFTReal"
		result.ConversionNotes = "单精度浮点数兼容"
		result.DataLossRisk = false

	case "double precision", "float8", "float":
		result.IsCompatible = true
		result.TargetType = "OFTReal"
		result.ConversionNotes = "双精度浮点数完全兼容"
		result.DataLossRisk = false

	case "numeric", "decimal":
		result.IsCompatible = true
		result.TargetType = "OFTReal"
		result.ConversionNotes = "精确数值类型转换为浮点数，可能有精度损失"
		result.DataLossRisk = true

	case "character varying", "varchar", "character", "char":
		result.IsCompatible = true
		result.TargetType = "OFTString"
		result.ConversionNotes = "字符串类型完全兼容"
		result.DataLossRisk = false

	case "text":
		result.IsCompatible = true
		result.TargetType = "OFTString"
		result.ConversionNotes = "长文本类型兼容，但GDB有长度限制"
		result.DataLossRisk = true

	case "date":
		result.IsCompatible = true
		result.TargetType = "OFTDate"
		result.ConversionNotes = "日期类型完全兼容"
		result.DataLossRisk = false

	case "time", "time without time zone", "time with time zone", "timetz":
		result.IsCompatible = true
		result.TargetType = "OFTTime"
		result.ConversionNotes = "时间类型兼容，时区信息可能丢失"
		result.DataLossRisk = true

	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		result.IsCompatible = true
		result.TargetType = "OFTDateTime"
		result.ConversionNotes = "时间戳类型兼容，时区信息可能丢失"
		result.DataLossRisk = true

	case "bytea":
		result.IsCompatible = true
		result.TargetType = "OFTBinary"
		result.ConversionNotes = "二进制类型完全兼容"
		result.DataLossRisk = false

	case "boolean", "bool":
		result.IsCompatible = true
		result.TargetType = "OFTInteger"
		result.ConversionNotes = "布尔类型转换为整数(0/1)"
		result.DataLossRisk = false

	case "uuid":
		result.IsCompatible = true
		result.TargetType = "OFTString"
		result.ConversionNotes = "UUID转换为36字符的字符串"
		result.DataLossRisk = false

	case "json", "jsonb":
		result.IsCompatible = false
		result.TargetType = ""
		result.ConversionNotes = "JSON类型不支持直接转换到GDB"
		result.DataLossRisk = true

	case "array":
		result.IsCompatible = false
		result.TargetType = ""
		result.ConversionNotes = "数组类型不支持直接转换到GDB"
		result.DataLossRisk = true

	default:
		// 检查是否是几何类型
		if strings.Contains(baseType, "geometry") || strings.Contains(baseType, "geography") {
			result.IsCompatible = false
			result.TargetType = ""
			result.ConversionNotes = "几何类型应通过几何字段处理，不作为属性字段"
			result.DataLossRisk = false
		} else {
			result.IsCompatible = true
			result.TargetType = "OFTString"
			result.ConversionNotes = "未知类型将转换为字符串"
			result.DataLossRisk = true
		}
	}

	return result
}

// GetAllSupportedTypeMappings 获取所有支持的类型映射
func GetAllSupportedTypeMappings() []map[string]interface{} {
	mappings := []map[string]interface{}{
		// 整数类型
		{"pg_type": "smallint", "gdal_type": "OFTInteger", "category": "integer", "description": "小整数"},
		{"pg_type": "integer", "gdal_type": "OFTInteger", "category": "integer", "description": "整数"},
		{"pg_type": "bigint", "gdal_type": "OFTInteger64", "category": "integer", "description": "大整数"},
		{"pg_type": "serial", "gdal_type": "OFTInteger", "category": "integer", "description": "自增整数"},
		{"pg_type": "bigserial", "gdal_type": "OFTInteger64", "category": "integer", "description": "自增大整数"},

		// 浮点数类型
		{"pg_type": "real", "gdal_type": "OFTReal", "category": "float", "description": "单精度浮点数"},
		{"pg_type": "double precision", "gdal_type": "OFTReal", "category": "float", "description": "双精度浮点数"},
		{"pg_type": "numeric", "gdal_type": "OFTReal", "category": "float", "description": "精确数值"},
		{"pg_type": "decimal", "gdal_type": "OFTReal", "category": "float", "description": "精确数值"},

		// 字符串类型
		{"pg_type": "varchar", "gdal_type": "OFTString", "category": "string", "description": "变长字符串"},
		{"pg_type": "char", "gdal_type": "OFTString", "category": "string", "description": "定长字符串"},
		{"pg_type": "text", "gdal_type": "OFTString", "category": "string", "description": "长文本"},
		// 日期时间类型
		{"pg_type": "date", "gdal_type": "OFTDate", "category": "datetime", "description": "日期"},
		{"pg_type": "time", "gdal_type": "OFTTime", "category": "datetime", "description": "时间"},
		{"pg_type": "timestamp", "gdal_type": "OFTDateTime", "category": "datetime", "description": "时间戳"},
		{"pg_type": "timestamptz", "gdal_type": "OFTDateTime", "category": "datetime", "description": "带时区时间戳"},

		// 二进制类型
		{"pg_type": "bytea", "gdal_type": "OFTBinary", "category": "binary", "description": "二进制数据"},

		// 布尔类型
		{"pg_type": "boolean", "gdal_type": "OFTInteger", "category": "boolean", "description": "布尔值(转为0/1)"},

		// UUID类型
		{"pg_type": "uuid", "gdal_type": "OFTString", "category": "string", "description": "UUID标识符"},
	}

	return mappings
}
