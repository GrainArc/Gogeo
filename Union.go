/*
Copyright (C) 2024 [GrainArc]

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

// UnionConfig Union操作配置
type UnionConfig struct {
	GroupFields      []string                 // 分组字段列表
	OutputLayerName  string                   // 输出图层名称
	GeomType         C.OGRwkbGeometryType     // 输出几何类型
	PrecisionConfig  *GeometryPrecisionConfig // 几何精度配置
	ProgressCallback ProgressCallback         // 进度回调
}

// FeatureGroup 要素分组
type FeatureGroup struct {
	GroupKey string            // 分组键（由多个字段值组成）
	Features []C.OGRFeatureH   // 该组的所有要素
	Fields   map[string]string // 分组字段的值
}

// 在 UnionProcessor 结构体中添加并发控制字段
type UnionProcessor struct {
	config *UnionConfig
	// 添加并发控制
	maxWorkers int
	semaphore  chan struct{}
}

// 修改 NewUnionProcessor 函数
func NewUnionProcessor(config *UnionConfig) *UnionProcessor {
	maxWorkers := runtime.NumCPU()
	if maxWorkers > 8 {
		maxWorkers = 8 // 限制最大并发数，避免过多的内存使用
	}

	return &UnionProcessor{
		config:     config,
		maxWorkers: maxWorkers,
		semaphore:  make(chan struct{}, maxWorkers),
	}
}

// 添加并发Union结果结构
type unionResult struct {
	groupKey string
	geometry C.OGRGeometryH
	group    *FeatureGroup
	err      error
}

// UnionAnalysis 对数据进行融合分析
func UnionAnalysis(inputLayer *GDALLayer, groupFields []string, outputTableName string,
	precisionConfig *GeometryPrecisionConfig, progressCallback ProgressCallback) (*GeosAnalysisResult, error) {

	tableName := inputLayer.GetLayerName()

	// 确保资源清理
	defer inputLayer.Close()

	// 3. 验证输入参数
	if len(groupFields) == 0 {
		return nil, fmt.Errorf("分组字段不能为空")
	}

	if outputTableName == "" {
		outputTableName = fmt.Sprintf("%s_union", tableName)
	}

	// 4. 验证分组字段是否存在
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

	// 5. 设置默认精度配置
	if precisionConfig == nil {
		precisionConfig = &GeometryPrecisionConfig{
			GridSize:      0.0,   // 使用浮点精度
			PreserveTopo:  true,  // 保持拓扑结构
			KeepCollapsed: false, // 不保留折叠元素
			Enabled:       true,  // 启用精度设置
		}
	}

	// 7. 执行Union分析
	result, err := UnionByFieldsWithPrecision(
		inputLayer,
		groupFields,
		outputTableName,
		precisionConfig,
		progressCallback,
	)

	if err != nil {
		return nil, fmt.Errorf("Union分析失败: %v", err)
	}

	return result, nil
}

// ProcessUnion 执行Union操作
func (up *UnionProcessor) ProcessUnion(inputLayer *GDALLayer) (*GeosAnalysisResult, error) {
	if inputLayer == nil {
		return nil, fmt.Errorf("输入图层不能为空")
	}

	// 验证分组字段
	if err := up.validateGroupFields(inputLayer); err != nil {
		return nil, err
	}

	// 分组要素
	groups, err := up.groupFeatures(inputLayer)
	if err != nil {
		return nil, fmt.Errorf("分组要素失败: %v", err)
	}

	if up.config.ProgressCallback != nil {
		if !up.config.ProgressCallback(0.3, fmt.Sprintf("完成要素分组，共 %d 个组", len(groups))) {
			return nil, fmt.Errorf("操作被用户取消")
		}
	}

	// 创建输出图层
	outputLayer, err := up.createOutputLayer(inputLayer)
	if err != nil {
		return nil, fmt.Errorf("创建输出图层失败: %v", err)
	}

	// 执行Union操作
	processedCount, err := up.performUnion(groups, outputLayer)
	if err != nil {
		outputLayer.Close()
		return nil, fmt.Errorf("执行Union操作失败: %v", err)
	}

	result := &GeosAnalysisResult{
		OutputLayer: outputLayer,
		ResultCount: processedCount,
	}

	if up.config.ProgressCallback != nil {
		up.config.ProgressCallback(1.0, fmt.Sprintf("Union操作完成，处理了 %d 个组，生成 %d 个要素", len(groups), processedCount))
	}

	return result, nil
}

// validateGroupFields 验证分组字段是否存在
func (up *UnionProcessor) validateGroupFields(layer *GDALLayer) error {
	if len(up.config.GroupFields) == 0 {
		return fmt.Errorf("必须指定至少一个分组字段")
	}

	layerDefn := layer.GetLayerDefn()
	fieldCount := int(C.OGR_FD_GetFieldCount(layerDefn))

	// 获取所有字段名
	existingFields := make(map[string]bool)
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(layerDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))
		existingFields[fieldName] = true
	}

	// 检查分组字段是否存在
	for _, groupField := range up.config.GroupFields {
		if !existingFields[groupField] {
			return fmt.Errorf("分组字段 '%s' 在图层中不存在", groupField)
		}
	}

	return nil
}

// groupFeatures 按指定字段分组要素
func (up *UnionProcessor) groupFeatures(layer *GDALLayer) (map[string]*FeatureGroup, error) {
	groups := make(map[string]*FeatureGroup)
	featureCount := layer.GetFeatureCount()
	processedCount := 0

	layer.ResetReading()

	for {
		feature := layer.GetNextFeatureRow()
		if feature == nil {
			break
		}

		// 生成分组键
		groupKey, fieldValues, err := up.generateGroupKey(feature)
		if err != nil {
			C.OGR_F_Destroy(feature)
			return nil, fmt.Errorf("生成分组键失败: %v", err)
		}

		// 获取或创建分组
		group, exists := groups[groupKey]
		if !exists {
			group = &FeatureGroup{
				GroupKey: groupKey,
				Features: make([]C.OGRFeatureH, 0),
				Fields:   fieldValues,
			}
			groups[groupKey] = group
		}

		// 克隆要素并添加到分组
		clonedFeature := C.OGR_F_Clone(feature)
		if clonedFeature != nil {
			group.Features = append(group.Features, clonedFeature)
		}

		C.OGR_F_Destroy(feature)
		processedCount++

		// 更新进度
		if up.config.ProgressCallback != nil && processedCount%1000 == 0 {
			progress := 0.3 * float64(processedCount) / float64(featureCount)
			message := fmt.Sprintf("正在分组要素: %d/%d", processedCount, featureCount)
			if !up.config.ProgressCallback(progress, message) {
				// 清理已分组的要素
				up.cleanupGroups(groups)
				return nil, fmt.Errorf("操作被用户取消")
			}
		}
	}

	return groups, nil
}

// generateGroupKey 生成分组键
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

		// 获取字段值作为字符串
		fieldValue := C.GoString(C.OGR_F_GetFieldAsString(feature, fieldIndex))
		keyParts = append(keyParts, fieldValue)
		fieldValues[fieldName] = fieldValue
	}

	// 使用分隔符连接所有字段值作为分组键
	groupKey := strings.Join(keyParts, "||")
	return groupKey, fieldValues, nil
}

// createOutputLayer 创建输出图层
func (up *UnionProcessor) createOutputLayer(inputLayer *GDALLayer) (*GDALLayer, error) {
	// 获取输入图层信息
	inputDefn := inputLayer.GetLayerDefn()
	geomType := up.config.GeomType
	if geomType == 0 {
		geomType = C.OGR_FD_GetGeomType(inputDefn)
	}
	srs := inputLayer.GetSpatialRef()

	// 创建输出图层名称
	outputName := up.config.OutputLayerName
	if outputName == "" {
		outputName = "union_result"
	}

	// 创建内存图层
	cOutputName := C.CString(outputName)
	defer C.free(unsafe.Pointer(cOutputName))

	outputLayerH := C.createMemoryLayer(cOutputName, geomType, srs)
	if outputLayerH == nil {
		return nil, fmt.Errorf("创建输出图层失败")
	}

	// 复制字段定义
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

	// 包装为GDALLayer
	outputLayer := &GDALLayer{
		layer:   outputLayerH,
		dataset: nil, // 内存图层不需要dataset
		driver:  nil,
	}

	runtime.SetFinalizer(outputLayer, (*GDALLayer).cleanup)
	return outputLayer, nil
}

// 替换原来的 performUnion 函数
func (up *UnionProcessor) performUnion(groups map[string]*FeatureGroup, outputLayer *GDALLayer) (int, error) {
	totalGroups := len(groups)
	if totalGroups == 0 {
		return 0, nil
	}

	// 按分组键排序以确保一致的处理顺序
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	// 创建结果通道和工作通道
	resultChan := make(chan unionResult, totalGroups)
	workChan := make(chan string, totalGroups)

	// 启动worker goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < up.maxWorkers; i++ {
		wg.Add(1)
		go up.unionWorker(ctx, &wg, workChan, resultChan, groups)
	}

	// 发送工作任务
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

	// 等待所有worker完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果并写入输出图层
	processedCount := 0
	completedGroups := 0
	outputDefn := outputLayer.GetLayerDefn()

	for result := range resultChan {
		completedGroups++

		if result.err != nil {
			fmt.Printf("警告: 分组 '%s' Union操作失败: %v\n", result.groupKey, result.err)
			continue
		}

		if result.geometry == nil {
			continue
		}

		// 应用几何精度设置
		finalGeometry := result.geometry
		if up.config.PrecisionConfig != nil && up.config.PrecisionConfig.Enabled {
			processedGeom, err := up.applyPrecisionSettings(result.geometry)
			if err != nil {
				fmt.Printf("警告: 分组 '%s' 精度处理失败: %v\n", result.groupKey, err)
			} else if processedGeom != result.geometry {
				C.OGR_G_DestroyGeometry(result.geometry)
				finalGeometry = processedGeom
			}
		}

		// 创建输出要素（这部分必须串行，因为GDAL写操作不是线程安全的）
		if up.createOutputFeature(outputLayer, outputDefn, finalGeometry, result.group) {
			processedCount++
		}

		// 清理几何体
		C.OGR_G_DestroyGeometry(finalGeometry)

		// 更新进度
		if up.config.ProgressCallback != nil && completedGroups%10 == 0 {
			progress := 0.1 + 0.9*float64(completedGroups)/float64(totalGroups)
			message := fmt.Sprintf("正在合并数据: %d/%d 组", completedGroups, totalGroups)
			if !up.config.ProgressCallback(progress, message) {
				cancel() // 取消所有worker
				break
			}
		}
	}

	// 清理分组数据
	up.cleanupGroups(groups)

	if ctx.Err() != nil {
		return processedCount, fmt.Errorf("操作被用户取消")
	}

	return processedCount, nil
}

// 添加worker函数
func (up *UnionProcessor) unionWorker(ctx context.Context, wg *sync.WaitGroup, workChan <-chan string, resultChan chan<- unionResult, groups map[string]*FeatureGroup) {
	defer wg.Done()

	for {
		select {
		case groupKey, ok := <-workChan:
			if !ok {
				return
			}

			group := groups[groupKey]
			geometry, err := up.unionGroupGeometries(group.Features)

			result := unionResult{
				groupKey: groupKey,
				geometry: geometry,
				group:    group,
				err:      err,
			}

			select {
			case resultChan <- result:
			case <-ctx.Done():
				// 如果操作被取消，清理几何体
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

// 添加创建输出要素的辅助函数（串行执行，确保GDAL写操作的线程安全）
func (up *UnionProcessor) createOutputFeature(outputLayer *GDALLayer, outputDefn C.OGRFeatureDefnH, geometry C.OGRGeometryH, group *FeatureGroup) bool {
	// 创建输出要素
	outputFeature := C.OGR_F_Create(outputDefn)
	if outputFeature == nil {
		return false
	}
	defer C.OGR_F_Destroy(outputFeature)

	// 设置几何体
	setGeomErr := C.OGR_F_SetGeometry(outputFeature, geometry)
	if setGeomErr != C.OGRERR_NONE {
		return false
	}

	// 复制分组字段的值到输出要素
	up.copyGroupFieldsToFeature(outputFeature, group, outputDefn)

	// 添加要素到输出图层
	createErr := C.OGR_L_CreateFeature(outputLayer.layer, outputFeature)
	return createErr == C.OGRERR_NONE
}

func (up *UnionProcessor) applyPrecisionSettings(geom C.OGRGeometryH) (C.OGRGeometryH, error) {
	if up.config.PrecisionConfig == nil || !up.config.PrecisionConfig.Enabled {
		return geom, nil
	}

	// 检查几何体是否有效
	if C.OGR_G_IsValid(geom) == 0 {
		// 尝试修复无效几何体
		fixedGeom := C.OGR_G_MakeValid(geom)
		if fixedGeom != nil {
			return fixedGeom, nil
		}
		return geom, fmt.Errorf("无法修复无效几何体")
	}

	// 如果设置了网格大小，使用正确的精度设置方法
	if up.config.PrecisionConfig.GridSize > 0 {
		// 构建精度设置标志
		flags := up.config.PrecisionConfig.getFlags()

		// 使用C函数设置精度
		preciseGeom := C.setPrecisionIfNeeded(geom, C.double(up.config.PrecisionConfig.GridSize), flags)
		if preciseGeom != nil && preciseGeom != geom {
			return preciseGeom, nil
		}
	}

	return geom, nil
}

// unionGroupGeometries 对一组要素的几何体执行Union操作
func (up *UnionProcessor) unionGroupGeometries(features []C.OGRFeatureH) (C.OGRGeometryH, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("要素列表为空")
	}

	if len(features) == 1 {
		// 只有一个要素，直接克隆其几何体
		geom := C.OGR_F_GetGeometryRef(features[0])
		if geom == nil {
			return nil, fmt.Errorf("要素几何体为空")
		}
		return C.OGR_G_Clone(geom), nil
	}

	// 获取第一个几何体作为起始
	firstGeom := C.OGR_F_GetGeometryRef(features[0])
	if firstGeom == nil {
		return nil, fmt.Errorf("第一个要素几何体为空")
	}

	resultGeom := C.OGR_G_Clone(firstGeom)
	if resultGeom == nil {
		return nil, fmt.Errorf("克隆第一个几何体失败")
	}

	// 逐个与其他几何体进行Union
	for i := 1; i < len(features); i++ {
		currentGeom := C.OGR_F_GetGeometryRef(features[i])
		if currentGeom == nil {
			continue
		}

		// 执行Union操作
		unionResult := C.OGR_G_Union(resultGeom, currentGeom)
		if unionResult == nil {
			fmt.Printf("警告: Union操作失败，跳过要素 %d\n", i)
			continue
		}

		// 规范化几何类型
		expectedType := C.OGR_G_GetGeometryType(resultGeom)
		normalizedGeom := C.normalizeGeometryType(unionResult, expectedType)

		// 更新结果几何体
		C.OGR_G_DestroyGeometry(resultGeom)

		if normalizedGeom != unionResult {
			C.OGR_G_DestroyGeometry(unionResult)
			resultGeom = normalizedGeom
		} else {
			resultGeom = unionResult
		}

		if resultGeom == nil {
			return nil, fmt.Errorf("Union操作后几何体为空")
		}
	}

	return resultGeom, nil
}

// copyGroupFieldsToFeature 复制分组字段值到输出要素
func (up *UnionProcessor) copyGroupFieldsToFeature(outputFeature C.OGRFeatureH, group *FeatureGroup, outputDefn C.OGRFeatureDefnH) {
	if len(group.Features) == 0 {
		return
	}

	// 使用第一个要素作为字段值的来源
	sourceFeature := group.Features[0]

	// 复制所有字段值
	fieldCount := int(C.OGR_FD_GetFieldCount(outputDefn))
	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(outputDefn, C.int(i))
		fieldName := C.GoString(C.OGR_Fld_GetNameRef(fieldDefn))

		// 获取源要素中对应字段的索引
		cFieldName := C.CString(fieldName)
		sourceFieldIndex := C.OGR_F_GetFieldIndex(sourceFeature, cFieldName)
		C.free(unsafe.Pointer(cFieldName))

		// 修复：将C函数返回值转换为bool
		if sourceFieldIndex >= 0 && C.OGR_F_IsFieldSet(sourceFeature, sourceFieldIndex) != 0 {
			// 复制字段值
			C.copyFieldValue(sourceFeature, outputFeature, sourceFieldIndex, C.int(i))
		}
	}
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

// UnionByFieldsWithPrecision 便捷函数：按指定字段执行Union操作（带精度控制）
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

// Close 关闭Union结果，释放资源
func (ur *GeosAnalysisResult) Close() {
	if ur.OutputLayer != nil {
		ur.OutputLayer.Close()
		ur.OutputLayer = nil
	}
}
