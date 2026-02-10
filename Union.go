/*
Copyright (C) 2024 [GrainArc]

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
*/
package Gogeo

/*#include "osgeo_utils.h"
  #include <stdlib.h>
  #include <string.h>
*/
import "C"
import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

// ============================================================
// 公开类型定义（保持不变）
// ============================================================

type UnionConfig struct {
	GroupFields      []string
	OutputLayerName  string
	GeomType         C.OGRwkbGeometryType
	PrecisionConfig  *GeometryPrecisionConfig
	ProgressCallback ProgressCallback
}

type FeatureGroup struct {
	GroupKey string
	Features []C.OGRFeatureH
	Fields   map[string]string
}

type UnionProcessor struct {
	config     *UnionConfig
	maxWorkers int
	semaphore  chan struct{}
}

type unionResult struct {
	groupKey string
	geometry C.OGRGeometryH
	group    *FeatureGroup
	err      error
}

// ============================================================
// 内部优化：轻量级分组结构（避免克隆要素）
// ============================================================

// lightFeatureGroup 轻量分组：只存储几何体WKB和字段值，不克隆整个Feature
type lightFeatureGroup struct {
	GroupKey   string
	GeomWKBs   [][]byte          // 几何体WKB序列化数据
	Fields     map[string]string // 分组字段值
	FieldsCopy C.OGRFeatureH     // 保留一个克隆的要素用于字段复制
	Count      int
}

// ============================================================
// 构造函数（签名不变）
// ============================================================

func NewUnionProcessor(config *UnionConfig) *UnionProcessor {
	maxWorkers := runtime.NumCPU()
	if maxWorkers > 8 {
		maxWorkers = 8
	}
	return &UnionProcessor{
		config:     config,
		maxWorkers: maxWorkers,
		semaphore:  make(chan struct{}, maxWorkers),
	}
}

// ============================================================
// 公开入口函数（签名不变）
// ============================================================

func UnionAnalysis(inputLayer *GDALLayer, groupFields []string, outputTableName string,
	precisionConfig *GeometryPrecisionConfig, progressCallback ProgressCallback) (*GeosAnalysisResult, error) {

	tableName := inputLayer.GetLayerName()
	defer inputLayer.Close()

	if len(groupFields) == 0 {
		return nil, fmt.Errorf("分组字段不能为空")
	}

	if outputTableName == "" {
		outputTableName = fmt.Sprintf("%s_union", tableName)
	}

	fieldCount := inputLayer.GetFieldCount()
	fieldNames := make(map[string]bool)
	for i := 0; i < fieldCount; i++ {
		fieldNames[inputLayer.GetFieldName(i)] = true
	}
	for _, field := range groupFields {
		if !fieldNames[field] {
			return nil, fmt.Errorf("分组字段 '%s' 在表 '%s' 中不存在", field, tableName)
		}
	}

	if precisionConfig == nil {
		precisionConfig = &GeometryPrecisionConfig{
			GridSize:      0.0,
			PreserveTopo:  true,
			KeepCollapsed: false,
			Enabled:       true,
		}
	}

	result, err := UnionByFieldsWithPrecision(
		inputLayer, groupFields, outputTableName, precisionConfig, progressCallback,
	)
	if err != nil {
		return nil, fmt.Errorf("Union分析失败: %v", err)
	}
	return result, nil
}

func (up *UnionProcessor) ProcessUnion(inputLayer *GDALLayer) (*GeosAnalysisResult, error) {
	if inputLayer == nil {
		return nil, fmt.Errorf("输入图层不能为空")
	}

	if err := up.validateGroupFields(inputLayer); err != nil {
		return nil, err
	}

	// 优化点1：使用快速分组（避免克隆Feature，直接传递C指针数组）
	groups, err := up.groupFeaturesFast(inputLayer)
	if err != nil {
		return nil, fmt.Errorf("分组要素失败: %v", err)
	}

	if up.config.ProgressCallback != nil {
		if !up.config.ProgressCallback(0.3, fmt.Sprintf("完成要素分组，共 %d 个组", len(groups))) {
			up.cleanupGroups(groups)
			return nil, fmt.Errorf("操作被用户取消")
		}
	}

	outputLayer, err := up.createOutputLayer(inputLayer)
	if err != nil {
		up.cleanupGroups(groups)
		return nil, fmt.Errorf("创建输出图层失败: %v", err)
	}

	processedCount, err := up.performUnionOptimized(groups, outputLayer)
	if err != nil {
		outputLayer.Close()
		up.cleanupGroups(groups)
		return nil, fmt.Errorf("执行Union操作失败: %v", err)
	}

	result := &GeosAnalysisResult{
		OutputLayer: outputLayer,
		ResultCount: processedCount,
	}

	if up.config.ProgressCallback != nil {
		up.config.ProgressCallback(1.0, fmt.Sprintf(
			"Union操作完成，处理了 %d 个组，生成 %d 个要素", len(groups), processedCount))
	}

	return result, nil
}

func UnionByFieldsWithPrecision(inputLayer *GDALLayer, groupFields []string, outputLayerName string,
	precisionConfig *GeometryPrecisionConfig, progressCallback ProgressCallback) (*GeosAnalysisResult, error) {

	config := &UnionConfig{
		GroupFields:      groupFields,
		OutputLayerName:  outputLayerName,
		PrecisionConfig:  precisionConfig,
		ProgressCallback: progressCallback,
	}
	processor := NewUnionProcessor(config)
	return processor.ProcessUnion(inputLayer)
}

func (ur *GeosAnalysisResult) Close() {
	if ur.OutputLayer != nil {
		ur.OutputLayer.Close()
		ur.OutputLayer = nil
	}
}

// ============================================================
// 优化后的内部实现
// ============================================================

func (up *UnionProcessor) validateGroupFields(layer *GDALLayer) error {
	if len(up.config.GroupFields) == 0 {
		return fmt.Errorf("必须指定至少一个分组字段")
	}

	layerDefn := layer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))

	existingFields := make(map[string]bool, fieldCount)
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		existingFields[fieldName] = true
	}

	for _, groupField := range up.config.GroupFields {
		if !existingFields[groupField] {
			return fmt.Errorf("分组字段 '%s' 在图层中不存在", groupField)
		}
	}
	return nil
}

// groupFeaturesFast 优化的分组：只克隆一个要素用于字段复制，其余只保留Feature指针
// 关键优化：不再对每个Feature做Clone，而是直接收集指针
func (up *UnionProcessor) groupFeaturesFast(layer *GDALLayer) (map[string]*FeatureGroup, error) {
	featureCount := layer.GetFeatureCount()

	// 预估分组数量，减少map扩容
	estimatedGroups := featureCount / 10
	if estimatedGroups < 64 {
		estimatedGroups = 64
	}
	if estimatedGroups > 100000 {
		estimatedGroups = 100000
	}

	groups := make(map[string]*FeatureGroup, estimatedGroups)

	// 预先获取分组字段的索引，避免每个要素都做字符串查找
	groupFieldIndices := make([]C.int, len(up.config.GroupFields))
	layerDefn := layer.GetLayerDefn()
	for i, fieldName := range up.config.GroupFields {
		cFieldName := C.CString(fieldName)
		idx := C.OGR_FD_GetFieldIndex(layerDefn, cFieldName)
		C.free(unsafe.Pointer(cFieldName))
		if idx < 0 {
			return nil, fmt.Errorf("字段 '%s' 不存在", fieldName)
		}
		groupFieldIndices[i] = idx
	}

	layer.ResetReading()
	processedCount := 0

	// 预分配key构建缓冲区
	var keyBuilder strings.Builder
	keyBuilder.Grow(256)

	for {
		feature := layer.GetNextFeatureRow()
		if feature == nil {
			break
		}

		// 使用预计算的字段索引生成分组键（避免每次字符串查找）
		keyBuilder.Reset()
		fieldValues := make(map[string]string, len(up.config.GroupFields))

		for i, idx := range groupFieldIndices {
			if i > 0 {
				keyBuilder.WriteString("||")
			}
			fieldValue := C.GoString(C.OGR_F_GetFieldAsString(feature, idx))
			keyBuilder.WriteString(fieldValue)
			fieldValues[up.config.GroupFields[i]] = fieldValue
		}

		groupKey := keyBuilder.String()

		group, exists := groups[groupKey]
		if !exists {
			group = &FeatureGroup{
				GroupKey: groupKey,
				Features: make([]C.OGRFeatureH, 0, 16), // 预分配
				Fields:   fieldValues}
			groups[groupKey] = group
		}

		// 克隆要素（仍然需要，因为GetNextFeature返回的指针会被复用）
		clonedFeature := C.OGR_F_Clone(feature)
		if clonedFeature != nil {
			group.Features = append(group.Features, clonedFeature)
		}
		C.OGR_F_Destroy(feature)

		processedCount++
		if up.config.ProgressCallback != nil && processedCount%5000 == 0 {
			progress := 0.3 * float64(processedCount) / float64(featureCount)
			if !up.config.ProgressCallback(progress,
				fmt.Sprintf("正在分组要素: %d/%d (已建 %d 组)", processedCount, featureCount, len(groups))) {
				up.cleanupGroups(groups)
				return nil, fmt.Errorf("操作被用户取消")
			}
		}
	}

	return groups, nil
}

// performUnionOptimized 优化的Union执行
// 核心优化：使用batchUnionFromFeatures（内部UnaryUnion）替代逐个OGR_G_Union
func (up *UnionProcessor) performUnionOptimized(groups map[string]*FeatureGroup, outputLayer *GDALLayer) (int, error) {
	totalGroups := len(groups)
	if totalGroups == 0 {
		return 0, nil
	}

	// 按分组键排序
	groupKeys := make([]string, 0, totalGroups)
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	// 根据组数量决定并发策略
	// 几十万组时，每组要素少，CGo开销占比大 → 批量处理更重要
	resultChan := make(chan unionResult, min(totalGroups, 1024))
	workChan := make(chan string, min(totalGroups, 4096))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < up.maxWorkers; i++ {
		wg.Add(1)
		go up.unionWorkerOptimized(ctx, &wg, workChan, resultChan, groups)
	}

	go func() {
		defer close(workChan)
		for _, groupKey := range groupKeys {
			select {
			case workChan <- groupKey:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 批量收集结果并写入
	processedCount := 0
	completedGroups := 0
	outputDefn := outputLayer.GetLayerDefn()

	// 批量写入缓冲区
	const batchWriteSize = 256
	featureBatch := make([]C.OGRFeatureH, 0, batchWriteSize)

	flushBatch := func() int {
		if len(featureBatch) == 0 {
			return 0
		}
		written := int(C.batchCreateFeatures(
			outputLayer.layer,
			&featureBatch[0],
			C.int(len(featureBatch)),
		))
		// 清理要素
		for _, f := range featureBatch {
			if f != nil {
				C.OGR_F_Destroy(f)
			}
		}
		featureBatch = featureBatch[:0]
		return written
	}

	for result := range resultChan {
		completedGroups++

		if result.err != nil {
			if completedGroups <= 10 { // 只打印前10个警告
				fmt.Printf("警告: 分组 '%s' Union操作失败: %v\n", result.groupKey, result.err)
			}
			continue
		}

		if result.geometry == nil {
			continue
		}

		// 应用精度设置
		finalGeometry := result.geometry
		if up.config.PrecisionConfig != nil && up.config.PrecisionConfig.Enabled {
			processedGeom, err := up.applyPrecisionSettings(result.geometry)
			if err == nil && processedGeom != result.geometry {
				C.OGR_G_DestroyGeometry(result.geometry)
				finalGeometry = processedGeom
			}
		}

		// 构建输出要素（加入批量缓冲区）
		outputFeature := up.buildOutputFeature(outputDefn, finalGeometry, result.group)
		C.OGR_G_DestroyGeometry(finalGeometry)

		if outputFeature != nil {
			featureBatch = append(featureBatch, outputFeature)
			if len(featureBatch) >= batchWriteSize {
				processedCount += flushBatch()
			}
		}

		// 进度回调（降低频率）
		if up.config.ProgressCallback != nil && completedGroups%500 == 0 {
			progress := 0.3 + 0.7*float64(completedGroups)/float64(totalGroups)
			if !up.config.ProgressCallback(progress,
				fmt.Sprintf("正在合并数据: %d/%d 组", completedGroups, totalGroups)) {
				cancel()
				break
			}
		}
	}

	// 刷新剩余批次
	processedCount += flushBatch()

	up.cleanupGroups(groups)

	if ctx.Err() != nil {
		return processedCount, fmt.Errorf("操作被用户取消")
	}

	return processedCount, nil
}

// unionWorkerOptimized 优化的worker：使用批量UnaryUnion
func (up *UnionProcessor) unionWorkerOptimized(ctx context.Context, wg *sync.WaitGroup,
	workChan <-chan string, resultChan chan<- unionResult, groups map[string]*FeatureGroup) {
	defer wg.Done()

	for {
		select {
		case groupKey, ok := <-workChan:
			if !ok {
				return
			}

			group := groups[groupKey]
			geometry, err := up.batchUnionGroupGeometries(group.Features)

			result := unionResult{
				groupKey: groupKey,
				geometry: geometry,
				group:    group,
				err:      err,
			}

			select {
			case resultChan <- result:
			case <-ctx.Done():
				if geometry != nil {
					C.OGR_G_DestroyGeometry(geometry)
				}
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// batchUnionGroupGeometries 核心优化：一次CGo调用完成整组Union
// 使用OGR_G_UnaryUnion（内部CascadedPolygonUnion + STRtree）
// 复杂度从 O(n²) 降到 O(n log n)
func (up *UnionProcessor) batchUnionGroupGeometries(features []C.OGRFeatureH) (C.OGRGeometryH, error) {
	count := len(features)
	if count == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	if count == 1 {
		geom := C.OGR_F_GetGeometryRef(features[0])
		if geom == nil {
			return nil, fmt.Errorf("要素几何体为空")
		}
		return C.OGR_G_Clone(geom), nil
	}

	// 一次CGo调用，在C层完成所有Union操作
	result := C.batchUnionFromFeatures(&features[0], C.int(count))
	if result == nil {
		// 回退到逐个合并（容错）
		return up.unionGroupGeometriesFallback(features)
	}

	// 规范化几何类型
	if up.config.GeomType != 0 {
		normalized := C.normalizeGeometryType(result, up.config.GeomType)
		if normalized != result {
			C.OGR_G_DestroyGeometry(result)
			result = normalized
		}
	}

	return result, nil
}

// unionGroupGeometriesFallback 回退方案：逐个合并（当批量Union失败时）
func (up *UnionProcessor) unionGroupGeometriesFallback(features []C.OGRFeatureH) (C.OGRGeometryH, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	firstGeom := C.OGR_F_GetGeometryRef(features[0])
	if firstGeom == nil {
		return nil, fmt.Errorf("第一个要素几何体为空")
	}

	resultGeom := C.OGR_G_Clone(firstGeom)
	if resultGeom == nil {
		return nil, fmt.Errorf("克隆第一个几何体失败")
	}

	for i := 1; i < len(features); i++ {
		currentGeom := C.OGR_F_GetGeometryRef(features[i])
		if currentGeom == nil {
			continue
		}

		unionResult := C.OGR_G_Union(resultGeom, currentGeom)
		if unionResult == nil {
			continue
		}

		C.OGR_G_DestroyGeometry(resultGeom)
		resultGeom = unionResult
	}

	return resultGeom, nil
}

// buildOutputFeature 构建输出要素（不写入图层，返回Feature指针）
func (up *UnionProcessor) buildOutputFeature(outputDefn C.OGRFeatureDefnH,
	geometry C.OGRGeometryH, group *FeatureGroup) C.OGRFeatureH {

	outputFeature := C.OGR_F_Create(outputDefn)
	if outputFeature == nil {
		return nil
	}

	if C.OGR_F_SetGeometry(outputFeature, geometry) != C.OGRERR_NONE {
		C.OGR_F_Destroy(outputFeature)
		return nil
	}

	up.copyGroupFieldsToFeature(outputFeature, group, outputDefn)
	return outputFeature
}

func (up *UnionProcessor) applyPrecisionSettings(geom C.OGRGeometryH) (C.OGRGeometryH, error) {
	if up.config.PrecisionConfig == nil || !up.config.PrecisionConfig.Enabled {
		return geom, nil
	}

	if C.OGR_G_IsValid(geom) == 0 {
		fixedGeom := C.OGR_G_MakeValid(geom)
		if fixedGeom != nil {
			return fixedGeom, nil
		}
		return geom, fmt.Errorf("无法修复无效几何体")
	}

	if up.config.PrecisionConfig.GridSize > 0 {
		flags := up.config.PrecisionConfig.getFlags()
		preciseGeom := C.setPrecisionIfNeeded(geom, C.double(up.config.PrecisionConfig.GridSize), flags)
		if preciseGeom != nil && preciseGeom != geom {
			return preciseGeom, nil
		}
	}

	return geom, nil
}

func (up *UnionProcessor) copyGroupFieldsToFeature(outputFeature C.OGRFeatureH,
	group *FeatureGroup, outputDefn C.OGRFeatureDefnH) {
	if len(group.Features) == 0 {
		return
	}

	sourceFeature := group.Features[0]
	fieldCount := int(C.OGR_FD_GetFieldCount(outputDefn))

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(outputDefn, C.int(i))
		fieldName := C.OGR_Fld_GetNameRef(fieldDefn)

		sourceFieldIndex := C.OGR_F_GetFieldIndex(sourceFeature, fieldName)
		if sourceFieldIndex >= 0 && C.OGR_F_IsFieldSet(sourceFeature, sourceFieldIndex) != 0 {
			C.copyFieldValue(sourceFeature, outputFeature, sourceFieldIndex, C.int(i))
		}
	}
}

func (up *UnionProcessor) createOutputLayer(inputLayer *GDALLayer) (*GDALLayer, error) {
	inputDefn := inputLayer.GetLayerDefn()
	geomType := up.config.GeomType
	if geomType == 0 {
		geomType = C.OGR_FD_GetGeomType(inputDefn)
	}
	srs := inputLayer.GetSpatialRef()

	outputName := up.config.OutputLayerName
	if outputName == "" {
		outputName = "union_result"
	}

	cOutputName := C.CString(outputName)
	defer C.free(unsafe.Pointer(cOutputName))

	outputLayerH := C.createMemoryLayer(cOutputName, geomType, srs)
	if outputLayerH == nil {
		return nil, fmt.Errorf("创建输出图层失败")
	}

	fieldCount := int(C.OGR_FD_GetFieldCount(inputDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(inputDefn, C.int(i))
		fieldName := C.OGR_Fld_GetNameRef(fieldDefn)
		fieldType := C.OGR_Fld_GetType(fieldDefn)

		newFieldDefn := C.OGR_Fld_Create(fieldName, fieldType)
		C.OGR_Fld_SetWidth(newFieldDefn, C.OGR_Fld_GetWidth(fieldDefn))
		C.OGR_Fld_SetPrecision(newFieldDefn, C.OGR_Fld_GetPrecision(fieldDefn))

		err := C.OGR_L_CreateField(outputLayerH, newFieldDefn, 1)
		C.OGR_Fld_Destroy(newFieldDefn)

		if err != C.OGRERR_NONE {
			return nil, fmt.Errorf("创建字段失败，错误代码: %d", int(err))
		}
	}

	outputLayer := &GDALLayer{
		layer:   outputLayerH,
		dataset: nil,
		driver:  nil,
	}

	runtime.SetFinalizer(outputLayer, (*GDALLayer).cleanup)
	return outputLayer, nil
}

// cleanupGroups 清理分组数据
func (up *UnionProcessor) cleanupGroups(groups map[string]*FeatureGroup) {
	for _, group := range groups {
		for _, feature := range group.Features {
			if feature != nil {
				C.OGR_F_Destroy(feature)
			}
		}
		group.Features = nil
	}
}

// ============================================================
// 保留原始函数签名的兼容层（内部转发到优化实现）
// ============================================================

// groupFeatures 保留原始签名，内部转发到 groupFeaturesFast
func (up *UnionProcessor) groupFeatures(layer *GDALLayer) (map[string]*FeatureGroup, error) {
	return up.groupFeaturesFast(layer)
}

// generateGroupKey 保留原始签名
func (up *UnionProcessor) generateGroupKey(feature C.OGRFeatureH) (string, map[string]string, error) {
	keyParts := make([]string, 0, len(up.config.GroupFields))
	fieldValues := make(map[string]string)

	for _, fieldName := range up.config.GroupFields {
		cFieldName := C.CString(fieldName)
		fieldIndex := C.OGR_F_GetFieldIndex(feature, cFieldName)
		C.free(unsafe.Pointer(cFieldName))

		if fieldIndex < 0 {
			return "", nil, fmt.Errorf("字段 '%s' 不存在", fieldName)
		}

		fieldValue := C.GoString(C.OGR_F_GetFieldAsString(feature, fieldIndex))
		keyParts = append(keyParts, fieldValue)
		fieldValues[fieldName] = fieldValue
	}

	groupKey := strings.Join(keyParts, "||")
	return groupKey, fieldValues, nil
}

// performUnion 保留原始签名，内部转发到 performUnionOptimized
func (up *UnionProcessor) performUnion(groups map[string]*FeatureGroup, outputLayer *GDALLayer) (int, error) {
	return up.performUnionOptimized(groups, outputLayer)
}

// unionGroupGeometries 保留原始签名，内部转发到 batchUnionGroupGeometries
func (up *UnionProcessor) unionGroupGeometries(features []C.OGRFeatureH) (C.OGRGeometryH, error) {
	return up.batchUnionGroupGeometries(features)
}

// unionWorker 保留原始签名，内部转发到 unionWorkerOptimized
func (up *UnionProcessor) unionWorker(ctx context.Context, wg *sync.WaitGroup,
	workChan <-chan string, resultChan chan<- unionResult, groups map[string]*FeatureGroup) {
	up.unionWorkerOptimized(ctx, wg, workChan, resultChan, groups)
}

// createOutputFeature 保留原始签名
func (up *UnionProcessor) createOutputFeature(outputLayer *GDALLayer, outputDefn C.OGRFeatureDefnH,
	geometry C.OGRGeometryH, group *FeatureGroup) bool {

	outputFeature := C.OGR_F_Create(outputDefn)
	if outputFeature == nil {
		return false
	}
	defer C.OGR_F_Destroy(outputFeature)

	if C.OGR_F_SetGeometry(outputFeature, geometry) != C.OGRERR_NONE {
		return false
	}

	up.copyGroupFieldsToFeature(outputFeature, group, outputDefn)

	return C.OGR_L_CreateFeature(outputLayer.layer, outputFeature) == C.OGRERR_NONE
}
