package Gogeo

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"gorm.io/gorm"
	"log"
	"strings"
)

/*
#include "osgeo_utils.h"
*/
import "C"

// GDALLayer 结构体定义

// SaveGDALLayerToPG 将GDALLayer保存到PostgreSQL数据库
func SaveGDALLayerToPG(DB *gorm.DB, gdalLayer *GDALLayer, tableName string, schema string, srid int) error {
	if gdalLayer == nil || gdalLayer.layer == nil {
		return fmt.Errorf("无效的GDALLayer")
	}

	// 获取数据库连接

	db, err := DB.DB()
	if err != nil {
		return fmt.Errorf("获取数据库连接失败: %v", err)
	}

	// 如果schema为空，使用默认的public
	if schema == "" {
		schema = "public"
	}

	// 如果srid为0，使用默认的4490
	if srid == 0 {
		srid = 4236
	}

	// 分析图层结构
	layerInfo, err := analyzeGDALLayer(gdalLayer, srid)
	if err != nil {
		return fmt.Errorf("分析图层结构失败: %v", err)
	}

	// 创建表
	err = createTableFromLayerInfo(db, layerInfo, tableName, schema, srid)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 插入数据
	err = insertDataFromLayerInfo(db, layerInfo, tableName, schema)
	if err != nil {
		return fmt.Errorf("插入数据失败: %v", err)
	}

	log.Printf("成功将图层数据保存到表: %s.%s", schema, tableName)
	return nil
}

// LayerAnalysisResult 图层分析结果
type LayerAnalysisResult struct {
	LayerName    string
	GeomType     string
	Fields       []FieldAnalysisResult
	Features     []FeatureAnalysisResult
	FeatureCount int
}

// FieldAnalysisResult 字段分析结果
type FieldAnalysisResult struct {
	Name      string
	Type      string
	Width     int
	Precision int
	DBType    string
}

// FeatureAnalysisResult 要素分析结果
type FeatureAnalysisResult struct {
	Properties map[string]interface{}
	WKBHex     string
}

// analyzeGDALLayer 分析GDALLayer结构和数据
func analyzeGDALLayer(gdalLayer *GDALLayer, targetSRID int) (*LayerAnalysisResult, error) {
	hLayer := gdalLayer.layer

	// 获取图层名称
	layerNamePtr := C.OGR_L_GetName(hLayer)
	layerName := C.GoString(layerNamePtr)

	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return nil, fmt.Errorf("无法获取图层定义")
	}

	// 获取几何类型
	geomType := C.OGR_FD_GetGeomType(hLayerDefn)
	geomTypeStr := convertOGRGeometryTypeForPG(geomType)

	// 分析字段
	fields, err := analyzeFields(hLayerDefn)
	if err != nil {
		return nil, fmt.Errorf("分析字段失败: %v", err)
	}

	// 创建目标坐标系
	hTargetSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hTargetSRS)

	if C.OSRImportFromEPSG(hTargetSRS, C.int(targetSRID)) != C.OGRERR_NONE {
		return nil, fmt.Errorf("无法创建目标坐标系 EPSG:%d", targetSRID)
	}

	// 获取源坐标系和创建坐标转换
	hSourceSRS := C.OGR_L_GetSpatialRef(hLayer)
	var hTransform C.OGRCoordinateTransformationH

	if hSourceSRS != nil && C.OSRIsSame(hSourceSRS, hTargetSRS) == 0 {
		hTransform = C.OCTNewCoordinateTransformation(hSourceSRS, hTargetSRS)
		if hTransform != nil {
			defer C.OCTDestroyCoordinateTransformation(hTransform)
			log.Printf("创建坐标转换: 源坐标系 -> EPSG:%d", targetSRID)
		}
	}

	// 重置读取位置
	C.OGR_L_ResetReading(hLayer)

	// 读取要素数据
	var features []FeatureAnalysisResult
	featureCount := 0

	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil {
			break
		}

		feature, err := analyzeFeature(hFeature, fields, hTransform)
		if err != nil {
			log.Printf("分析要素失败: %v", err)
			C.OGR_F_Destroy(hFeature)
			continue
		}

		features = append(features, feature)
		featureCount++
		C.OGR_F_Destroy(hFeature)
	}

	result := &LayerAnalysisResult{
		LayerName:    layerName,
		GeomType:     geomTypeStr,
		Fields:       fields,
		Features:     features,
		FeatureCount: featureCount,
	}

	return result, nil
}

// analyzeFields 分析字段信息
func analyzeFields(hLayerDefn C.OGRFeatureDefnH) ([]FieldAnalysisResult, error) {
	fieldCount := int(C.OGR_FD_GetFieldCount(hLayerDefn))
	fields := make([]FieldAnalysisResult, 0, fieldCount)

	for i := 0; i < fieldCount; i++ {
		hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
		if hFieldDefn == nil {
			continue
		}

		// 获取字段名称
		fieldNamePtr := C.OGR_Fld_GetNameRef(hFieldDefn)
		fieldName := C.GoString(fieldNamePtr)

		// 获取字段类型
		fieldType := C.OGR_Fld_GetType(hFieldDefn)
		fieldTypeStr := getFieldTypeName(fieldType)

		// 获取字段宽度和精度
		width := int(C.OGR_Fld_GetWidth(hFieldDefn))
		precision := int(C.OGR_Fld_GetPrecision(hFieldDefn))

		// 转换为PostgreSQL类型
		dbType := convertFieldTypeToPostgreSQL(fieldType, width, precision)

		field := FieldAnalysisResult{
			Name:      fieldName,
			Type:      fieldTypeStr,
			Width:     width,
			Precision: precision,
			DBType:    dbType,
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// analyzeFeature 分析单个要素
func analyzeFeature(hFeature C.OGRFeatureH, fields []FieldAnalysisResult, hTransform C.OGRCoordinateTransformationH) (FeatureAnalysisResult, error) {
	feature := FeatureAnalysisResult{
		Properties: make(map[string]interface{}),
	}

	// 处理几何数据
	hGeometry := C.OGR_F_GetGeometryRef(hFeature)
	if hGeometry != nil {
		// 坐标转换
		if hTransform != nil {
			hGeomClone := C.OGR_G_Clone(hGeometry)
			defer C.OGR_G_DestroyGeometry(hGeomClone)

			if C.OGR_G_Transform(hGeomClone, hTransform) != C.OGRERR_NONE {
				return feature, fmt.Errorf("坐标转换失败")
			}
			hGeometry = hGeomClone
		}

		// 转换为WKB十六进制
		wkbHex, err := convertGeometryToWKBHex(hGeometry)
		if err != nil {
			return feature, fmt.Errorf("几何转换失败: %v", err)
		}
		feature.WKBHex = wkbHex
	}

	// 处理属性数据
	for i, field := range fields {
		fieldIndex := C.int(i)

		// 检查字段是否为空
		if C.OGR_F_IsFieldSet(hFeature, fieldIndex) == 0 {
			feature.Properties[field.Name] = nil
			continue
		}

		// 获取字段值
		value := getFeatureFieldValue(hFeature, fieldIndex, field.Type)
		feature.Properties[field.Name] = value
	}

	return feature, nil
}

// convertGeometryToWKBHex 将几何转换为WKB十六进制
func convertGeometryToWKBHex(hGeometry C.OGRGeometryH) (string, error) {
	// 获取WKB大小
	wkbSize := C.OGR_G_WkbSize(hGeometry)
	if wkbSize <= 0 {
		return "", fmt.Errorf("无效的几何对象")
	}

	// 分配内存
	wkbData := C.malloc(C.size_t(wkbSize))
	if wkbData == nil {
		return "", fmt.Errorf("内存分配失败")
	}
	defer C.free(wkbData)

	// 导出为WKB
	if C.OGR_G_ExportToWkb(hGeometry, C.wkbNDR, (*C.uchar)(wkbData)) != C.OGRERR_NONE {
		return "", fmt.Errorf("导出WKB失败")
	}

	// 转换为十六进制字符串
	wkbBytes := C.GoBytes(wkbData, wkbSize)
	return hex.EncodeToString(wkbBytes), nil
}

// getFeatureFieldValue 获取要素字段值
func getFeatureFieldValue(hFeature C.OGRFeatureH, fieldIndex C.int, fieldType string) interface{} {
	switch fieldType {
	case "Integer":
		return int(C.OGR_F_GetFieldAsInteger(hFeature, fieldIndex))
	case "Integer64":
		return int64(C.OGR_F_GetFieldAsInteger64(hFeature, fieldIndex))
	case "Real":
		return float64(C.OGR_F_GetFieldAsDouble(hFeature, fieldIndex))
	case "String":
		strPtr := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(strPtr)
	case "Date", "Time", "DateTime":
		strPtr := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(strPtr)
	default:
		strPtr := C.OGR_F_GetFieldAsString(hFeature, fieldIndex)
		return C.GoString(strPtr)
	}
}

// convertFieldTypeToPostgreSQL 转换字段类型为PostgreSQL类型
func convertFieldTypeToPostgreSQL(fieldType C.OGRFieldType, width, precision int) string {
	switch fieldType {
	case C.OFTInteger:
		return "INTEGER"
	case C.OFTInteger64:
		return "BIGINT"
	case C.OFTReal:
		if precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", width, precision)
		}
		return "DOUBLE PRECISION"
	case C.OFTString:
		if width > 0 && width <= 10485760 {
			return fmt.Sprintf("VARCHAR(%d)", width)
		}
		return "TEXT"
	case C.OFTDate:
		return "DATE"
	case C.OFTTime:
		return "TIME"
	case C.OFTDateTime:
		return "TIMESTAMP"
	case C.OFTBinary:
		return "BYTEA"
	default:
		return "TEXT"
	}
}

// convertOGRGeometryTypeForPG 转换OGR几何类型为PostGIS类型
func convertOGRGeometryTypeForPG(geomType C.OGRwkbGeometryType) string {
	switch geomType {
	case C.wkbPoint, C.wkbPoint25D:
		return "POINT"
	case C.wkbLineString, C.wkbLineString25D:
		return "LINESTRING"
	case C.wkbPolygon, C.wkbPolygon25D:
		return "POLYGON"
	case C.wkbMultiPoint, C.wkbMultiPoint25D:
		return "MULTIPOINT"
	case C.wkbMultiLineString, C.wkbMultiLineString25D:
		return "MULTILINESTRING"
	case C.wkbMultiPolygon, C.wkbMultiPolygon25D:
		return "MULTIPOLYGON"
	case C.wkbGeometryCollection, C.wkbGeometryCollection25D:
		return "GEOMETRYCOLLECTION"
	default:
		return "GEOMETRY"
	}
}

// createTableFromLayerInfo 根据图层信息创建表
func createTableFromLayerInfo(db *sql.DB, layerInfo *LayerAnalysisResult, tableName, schema string, srid int) error {
	// 检查表是否存在
	var exists bool
	checkQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)`

	err := db.QueryRow(checkQuery, schema, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("检查表存在性失败: %v", err)
	}

	if exists {
		// 删除已存在的表
		dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, schema, tableName)
		_, err = db.Exec(dropQuery)
		if err != nil {
			return fmt.Errorf("删除已存在表失败: %v", err)
		}
		log.Printf("删除已存在的表: %s.%s", schema, tableName)
	}

	// 构建CREATE TABLE语句
	var fieldDefs []string

	// 添加ID字段
	fieldDefs = append(fieldDefs, "id SERIAL PRIMARY KEY")

	// 添加属性字段
	for _, field := range layerInfo.Fields {
		fieldDef := fmt.Sprintf("%s %s", field.Name, field.DBType)
		fieldDefs = append(fieldDefs, fieldDef)
	}

	// 添加几何字段
	geomFieldDef := fmt.Sprintf("geom GEOMETRY(%s, %d)", layerInfo.GeomType, srid)
	fieldDefs = append(fieldDefs, geomFieldDef)

	createQuery := fmt.Sprintf(`
		CREATE TABLE %s.%s (
			%s
		)`, schema, tableName, strings.Join(fieldDefs, ",\n\t\t\t"))

	_, err = db.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 创建空间索引
	indexQuery := fmt.Sprintf(`
		CREATE INDEX idx_%s_geom ON %s.%s USING GIST (geom)`,
		tableName, schema, tableName)

	_, err = db.Exec(indexQuery)
	if err != nil {
		log.Printf("创建空间索引失败: %v", err)
	} else {
		log.Printf("成功创建表: %s.%s", schema, tableName)
	}

	return nil
}

func insertDataFromLayerInfo(db *sql.DB, layerInfo *LayerAnalysisResult, tableName, schema string) error {
	if len(layerInfo.Features) == 0 {
		log.Printf("没有要素数据需要插入")
		return nil
	}

	// 构建INSERT语句
	var fieldNames []string
	var placeholders []string

	for i, field := range layerInfo.Fields {
		fieldNames = append(fieldNames, field.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	fieldNames = append(fieldNames, "geom")
	placeholders = append(placeholders, fmt.Sprintf("ST_GeomFromWKB($%d)", len(fieldNames)))

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s (%s) VALUES (%s)`,
		schema, tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	successCount := 0
	batchSize := 1000 // 批次大小

	// 分批处理
	for i := 0; i < len(layerInfo.Features); i += batchSize {
		end := i + batchSize
		if end > len(layerInfo.Features) {
			end = len(layerInfo.Features)
		}

		// 处理一批数据
		batchSuccess, err := processBatch(db, insertQuery, layerInfo.Features[i:end], layerInfo.Fields, i)
		if err != nil {
			log.Printf("批次 %d-%d 处理失败: %v", i, end-1, err)
			// 继续处理下一批，不中断整个过程
		}
		successCount += batchSuccess
	}

	log.Printf("数据插入完成: 成功插入 %d/%d 条记录", successCount, len(layerInfo.Features))
	return nil
}

func processBatch(db *sql.DB, insertQuery string, features []FeatureAnalysisResult, fields []FieldAnalysisResult, startIndex int) (int, error) {
	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("开始事务失败: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// 准备语句
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return 0, fmt.Errorf("准备插入语句失败: %v", err)
	}
	defer stmt.Close()

	successCount := 0
	for i, feature := range features {
		// 准备参数
		var args []interface{}

		// 添加属性字段值
		for _, field := range fields {
			if value, exists := feature.Properties[field.Name]; exists {
				args = append(args, value)
			} else {
				args = append(args, nil)
			}
		}

		// 添加几何数据
		if feature.WKBHex != "" {
			wkbBytes, err := hex.DecodeString(feature.WKBHex)
			if err != nil {
				log.Printf("解码WKB失败，跳过要素 %d: %v", startIndex+i+1, err)
				continue
			}
			args = append(args, wkbBytes)
		} else {
			args = append(args, nil)
		}

		// 执行插入
		_, err = stmt.Exec(args...)
		if err != nil {
			log.Printf("插入要素 %d 失败: %v", startIndex+i+1, err)
			// 如果是严重错误，回滚整个批次
			if isSerialError(err) {
				return successCount, fmt.Errorf("严重错误，回滚批次: %v", err)
			}
			continue
		}

		successCount++
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return 0, fmt.Errorf("提交事务失败: %v", err)
	}

	log.Printf("批次处理完成: 成功插入 %d/%d 条记录", successCount, len(features))
	return successCount, nil
}

func isSerialError(err error) bool {
	// 判断是否是严重错误，需要回滚整个批次
	errStr := err.Error()
	return strings.Contains(errStr, "25P02") || // 事务终止
		strings.Contains(errStr, "connection") || // 连接问题
		strings.Contains(errStr, "timeout") // 超时
}

// SaveGDALLayerToPGBatch 批量保存GDALLayer到PostgreSQL（优化版本）
func SaveGDALLayerToPGBatch(DB *gorm.DB, gdalLayer *GDALLayer, tableName string, schema string, srid int, batchSize int) error {
	if gdalLayer == nil || gdalLayer.layer == nil {
		return fmt.Errorf("无效的GDALLayer")
	}

	// 获取数据库连接

	db, err := DB.DB()
	if err != nil {
		return fmt.Errorf("获取数据库连接失败: %v", err)
	}

	// 设置默认值
	if schema == "" {
		schema = "public"
	}
	if srid == 0 {
		srid = 4490
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	// 分析图层结构（只分析结构，不读取所有数据）
	layerInfo, err := analyzeGDALLayerStructure(gdalLayer, srid)
	if err != nil {
		return fmt.Errorf("分析图层结构失败: %v", err)
	}

	// 创建表
	err = createTableFromLayerInfo(db, layerInfo, tableName, schema, srid)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 批量插入数据
	err = insertDataFromGDALLayerBatch(db, gdalLayer, layerInfo, tableName, schema, srid, batchSize)
	if err != nil {
		return fmt.Errorf("批量插入数据失败: %v", err)
	}

	log.Printf("成功将图层数据保存到表: %s.%s", schema, tableName)
	return nil
}

// analyzeGDALLayerStructure 只分析图层结构，不读取所有数据
func analyzeGDALLayerStructure(gdalLayer *GDALLayer, targetSRID int) (*LayerAnalysisResult, error) {
	hLayer := gdalLayer.layer

	// 获取图层名称
	layerNamePtr := C.OGR_L_GetName(hLayer)
	layerName := C.GoString(layerNamePtr)

	// 获取图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn == nil {
		return nil, fmt.Errorf("无法获取图层定义")
	}

	// 获取几何类型
	geomType := C.OGR_FD_GetGeomType(hLayerDefn)
	geomTypeStr := convertOGRGeometryTypeForPG(geomType)

	// 分析字段
	fields, err := analyzeFields(hLayerDefn)
	if err != nil {
		return nil, fmt.Errorf("分析字段失败: %v", err)
	}

	// 获取要素数量
	featureCount := int(C.OGR_L_GetFeatureCount(hLayer, 1))

	result := &LayerAnalysisResult{
		LayerName:    layerName,
		GeomType:     geomTypeStr,
		Fields:       fields,
		Features:     nil, // 不预加载所有数据
		FeatureCount: featureCount,
	}

	return result, nil
}

// insertDataFromGDALLayerBatch 从GDALLayer批量插入数据
func insertDataFromGDALLayerBatch(db *sql.DB, gdalLayer *GDALLayer, layerInfo *LayerAnalysisResult,
	tableName, schema string, srid, batchSize int) error {

	hLayer := gdalLayer.layer

	// 创建目标坐标系和坐标转换
	hTargetSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hTargetSRS)

	if C.OSRImportFromEPSG(hTargetSRS, C.int(srid)) != C.OGRERR_NONE {
		return fmt.Errorf("无法创建目标坐标系 EPSG:%d", srid)
	}

	hSourceSRS := C.OGR_L_GetSpatialRef(hLayer)
	var hTransform C.OGRCoordinateTransformationH

	if hSourceSRS != nil && C.OSRIsSame(hSourceSRS, hTargetSRS) == 0 {
		hTransform = C.OCTNewCoordinateTransformation(hSourceSRS, hTargetSRS)
		if hTransform != nil {
			defer C.OCTDestroyCoordinateTransformation(hTransform)
		}
	}

	// 构建INSERT语句
	var fieldNames []string
	var placeholders []string

	for i, field := range layerInfo.Fields {
		fieldNames = append(fieldNames, field.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	fieldNames = append(fieldNames, "geom")
	placeholders = append(placeholders, fmt.Sprintf("ST_GeomFromWKB($%d)", len(fieldNames)))

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s (%s) VALUES (%s)`,
		schema, tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	// 重置读取位置
	C.OGR_L_ResetReading(hLayer)

	successCount := 0
	totalProcessed := 0

	var batch []FeatureAnalysisResult
	var batchFeatureIndexes []int

	// 批量处理
	for {
		hFeature := C.OGR_L_GetNextFeature(hLayer)
		if hFeature == nil && len(batch) == 0 {
			break // 没有更多数据且批次为空
		}

		if hFeature != nil {
			totalProcessed++

			// 分析要素
			feature, err := analyzeFeature(hFeature, layerInfo.Fields, hTransform)
			C.OGR_F_Destroy(hFeature)

			if err != nil {
				log.Printf("分析要素 %d 失败: %v", totalProcessed, err)
				continue
			}

			// 验证几何数据
			if feature.WKBHex != "" && !isValidWKB(feature.WKBHex) {
				log.Printf("要素 %d 包含无效几何，跳过", totalProcessed)
				continue
			}

			batch = append(batch, feature)
			batchFeatureIndexes = append(batchFeatureIndexes, totalProcessed)
		}

		// 处理批次（当批次满了或没有更多数据时）
		if len(batch) >= batchSize || (hFeature == nil && len(batch) > 0) {
			batchSuccess := processBatchWithFallback(db, insertQuery, batch, batchFeatureIndexes, layerInfo.Fields)
			successCount += batchSuccess

			// 清空批次
			batch = batch[:0]
			batchFeatureIndexes = batchFeatureIndexes[:0]
		}
	}

	log.Printf("批量处理完成: 总共处理 %d 条记录，成功插入 %d 条", totalProcessed, successCount)
	return nil
}
func processBatchWithFallback(db *sql.DB, insertQuery string, features []FeatureAnalysisResult,
	featureIndexes []int, fields []FieldAnalysisResult) int {

	// 先尝试批量插入
	success, err := processBatchTransaction(db, insertQuery, features, fields)
	if err == nil {
		return success
	}
	// 回退到逐条处理
	successCount := 0
	for i, feature := range features {
		err := insertSingleFeature(db, insertQuery, feature, fields, featureIndexes[i])
		if err != nil {

		} else {
			successCount++
		}
	}

	return successCount
}

// 批量事务处理
func processBatchTransaction(db *sql.DB, insertQuery string, features []FeatureAnalysisResult, fields []FieldAnalysisResult) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	successCount := 0
	for _, feature := range features {
		var args []interface{}

		for _, field := range fields {
			if value, exists := feature.Properties[field.Name]; exists {
				args = append(args, value)
			} else {
				args = append(args, nil)
			}
		}

		if feature.WKBHex != "" {
			wkbBytes, err := hex.DecodeString(feature.WKBHex)
			if err != nil {
				return successCount, err
			}
			args = append(args, wkbBytes)
		} else {
			args = append(args, nil)
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			return successCount, err
		}

		successCount++
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return successCount, nil
}

// GetGDALLayerInfo 获取GDALLayer的基本信息
func GetGDALLayerInfo(gdalLayer *GDALLayer) (map[string]interface{}, error) {
	if gdalLayer == nil || gdalLayer.layer == nil {
		return nil, fmt.Errorf("无效的GDALLayer")
	}

	hLayer := gdalLayer.layer
	info := make(map[string]interface{})

	// 图层名称
	layerNamePtr := C.OGR_L_GetName(hLayer)
	info["layer_name"] = C.GoString(layerNamePtr)

	// 要素数量
	featureCount := int(C.OGR_L_GetFeatureCount(hLayer, 1))
	info["feature_count"] = featureCount

	// 图层定义
	hLayerDefn := C.OGR_L_GetLayerDefn(hLayer)
	if hLayerDefn != nil {
		// 几何类型
		geomType := C.OGR_FD_GetGeomType(hLayerDefn)
		info["geometry_type"] = getGeometryTypeName(geomType)

		// 字段数量
		fieldCount := int(C.OGR_FD_GetFieldCount(hLayerDefn))
		info["field_count"] = fieldCount

		// 字段信息
		var fields []map[string]interface{}
		for i := 0; i < fieldCount; i++ {
			hFieldDefn := C.OGR_FD_GetFieldDefn(hLayerDefn, C.int(i))
			if hFieldDefn != nil {
				fieldInfo := make(map[string]interface{})
				fieldInfo["name"] = C.GoString(C.OGR_Fld_GetNameRef(hFieldDefn))
				fieldInfo["type"] = getFieldTypeName(C.OGR_Fld_GetType(hFieldDefn))
				fieldInfo["width"] = int(C.OGR_Fld_GetWidth(hFieldDefn))
				fieldInfo["precision"] = int(C.OGR_Fld_GetPrecision(hFieldDefn))
				fields = append(fields, fieldInfo)
			}
		}
		info["fields"] = fields
	}

	// 坐标系信息
	hSRS := C.OGR_L_GetSpatialRef(hLayer)
	if hSRS != nil {
		var authName *C.char
		var authCode *C.char

		authName = C.OSRGetAuthorityName(hSRS, nil)
		authCode = C.OSRGetAuthorityCode(hSRS, nil)

		if authName != nil && authCode != nil {
			info["srs_authority"] = C.GoString(authName)
			info["srs_code"] = C.GoString(authCode)
		}
	}

	return info, nil
}

// 插入单条要素
func insertSingleFeature(db *sql.DB, insertQuery string, feature FeatureAnalysisResult, fields []FieldAnalysisResult, featureIndex int) error {
	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// 准备参数
	var args []interface{}

	for _, field := range fields {
		if value, exists := feature.Properties[field.Name]; exists {
			args = append(args, value)
		} else {
			args = append(args, nil)
		}
	}

	// 添加几何数据
	if feature.WKBHex != "" {
		wkbBytes, err := hex.DecodeString(feature.WKBHex)
		if err != nil {
			return fmt.Errorf("解码WKB失败: %v", err)
		}
		args = append(args, wkbBytes)
	} else {
		args = append(args, nil)
	}

	// 执行插入
	_, err = tx.Exec(insertQuery, args...)
	if err != nil {
		return fmt.Errorf("执行插入失败: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	return nil
}

// 验证WKB数据是否有效
func isValidWKB(wkbHex string) bool {
	// 基本长度检查
	if len(wkbHex) < 10 {
		return false
	}

	// 解码检查
	wkbBytes, err := hex.DecodeString(wkbHex)
	if err != nil {
		return false
	}

	// 基本WKB格式检查
	if len(wkbBytes) < 5 {
		return false
	}

	// 可以添加更多的几何有效性检查
	return true
}
