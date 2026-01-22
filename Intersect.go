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
// 执行带进度监测的相交分析
static OGRErr performIntersectionWithProgress(OGRLayerH inputLayer,
                                     OGRLayerH methodLayer,
                                     OGRLayerH resultLayer,
                                     char **options,
                                     void *progressData) {
    return OGR_L_Intersection(inputLayer, methodLayer, resultLayer, options,
                             progressCallback, progressData);
}
*/
import "C"

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// 并行空间相交分析
func SpatialIntersectionAnalysis(inputLayer, methodLayer *GDALLayer, config *ParallelGeosConfig, strategy FieldMergeStrategy) (*GeosAnalysisResult, error) {

	defer inputLayer.Close()

	defer methodLayer.Close()
	// 执行并行相交分析

	err := addIdentifierField(inputLayer, "gogeo_analysis_id")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}
	if strategy == UseTable2Fields {
		err = addIdentifierField(methodLayer, "gogeo_analysis_id")
	}
	resultLayer, err := performIntersectionAnalysis(inputLayer, methodLayer, strategy, config)
	if err != nil {
		return nil, fmt.Errorf("执行并行相交分析失败: %v", err)
	}

	// 计算结果数量
	resultCount := resultLayer.GetFeatureCount()

	if config.IsMergeTile {
		// 执行按标识字段的融合操作
		unionResult, err := PerformUnionByFields(resultLayer, config.PrecisionConfig, config.ProgressCallback)
		if err != nil {
			return nil, fmt.Errorf("执行融合操作失败: %v", err)
		}

		// 删除临时的_identityID字段
		err = DeleteFieldFromLayerFuzzy(unionResult.OutputLayer, "gogeo_analysis_id")
		if err != nil {
			fmt.Printf("警告: 删除临时标识字段失败: %v\n", err)
		}

		return unionResult, nil
	} else {

		return &GeosAnalysisResult{
			OutputLayer: resultLayer,
			ResultCount: resultCount,
		}, nil
	}
}

func performIntersectionAnalysis(inputLayer, methodLayer *GDALLayer, strategy FieldMergeStrategy, config *ParallelGeosConfig) (*GDALLayer, error) {
	if config.PrecisionConfig != nil {
		// 创建内存副本
		inputMemLayer, err := createMemoryLayerCopy(inputLayer, "input_mem_layer")
		if err != nil {
			return nil, fmt.Errorf("创建输入图层内存副本失败: %v", err)
		}

		methodMemLayer, err := createMemoryLayerCopy(methodLayer, "erase_mem_layer")
		if err != nil {
			inputMemLayer.Close()
			return nil, fmt.Errorf("创建图层内存副本失败: %v", err)
		}

		// 在内存图层上设置精度
		if config.PrecisionConfig.Enabled {
			flags := config.PrecisionConfig.getFlags()
			gridSize := C.double(config.PrecisionConfig.GridSize)

			C.setLayerGeometryPrecision(inputMemLayer.layer, gridSize, flags)
			C.setLayerGeometryPrecision(methodMemLayer.layer, gridSize, flags)
		}

		// 使用内存图层进行后续处理
		inputLayer = inputMemLayer
		methodLayer = methodMemLayer
	}

	// 创建结果图层
	resultLayer, err := CreateIntersectionResultLayer(inputLayer, methodLayer, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}
	taskid := uuid.New().String()
	//对A B图层进行分块,并创建bin文件
	GenerateTiles(inputLayer, methodLayer, config.TileCount, taskid)
	//读取文件列表，并发执行擦除操作
	GPbins, err := ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("提取分组文件失败: %v", err)
	}
	// 并发执行分析
	err = executeConcurrentIntersectionAnalysis(GPbins, resultLayer, config, strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("并发分析失败: %v", err)
	}
	// 清理临时文件
	defer func() {
		err := cleanupTileFiles(taskid)
		if err != nil {
			log.Printf("清理临时文件失败: %v", err)
		}
	}()
	return resultLayer, nil
}

func executeConcurrentIntersectionAnalysis(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig, strategy FieldMergeStrategy) error {
	maxWorkers := config.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}

	totalTasks := len(tileGroups)
	if totalTasks == 0 {
		return fmt.Errorf("没有分块需要处理")
	}

	// 创建任务队列和结果队列
	taskQueue := make(chan GroupTileFiles, totalTasks)
	results := make(chan taskResult, totalTasks)

	// 启动固定数量的工作协程
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker_intersection(i, taskQueue, results, config, &wg, strategy)
	}

	// 发送所有任务到队列
	go func() {
		for _, tileGroup := range tileGroups {
			taskQueue <- tileGroup
		}
		close(taskQueue) // 关闭任务队列，通知工作协程没有更多任务
	}()

	// 启动结果收集协程
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	var processingError error
	completed := 0

	go func() {
		defer resultWg.Done()

		var totalDuration time.Duration
		var minDuration, maxDuration time.Duration

		for i := 0; i < totalTasks; i++ {
			result := <-results
			completed++

			if result.err != nil {
				processingError = fmt.Errorf("分块 %d 处理失败: %v", result.index, result.err)
				log.Printf("错误: %v", processingError)
				return
			}

			// 统计执行时间
			totalDuration += result.duration
			if i == 0 {
				minDuration = result.duration
				maxDuration = result.duration
			} else {
				if result.duration < minDuration {
					minDuration = result.duration
				}
				if result.duration > maxDuration {
					maxDuration = result.duration
				}
			}

			// 将结果合并到主图层
			if result.layer != nil {
				err := mergeResultsToMainLayer(result.layer, resultLayer)
				if err != nil {
					processingError = fmt.Errorf("合并分块 %d 结果失败: %v", result.index, err)
					log.Printf("错误: %v", processingError)
					return
				}

				// 释放临时图层资源
				result.layer.Close()
			}

			// 进度回调
			if config.ProgressCallback != nil {
				progress := float64(completed) / float64(totalTasks)
				avgDuration := totalDuration / time.Duration(completed)

				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)

				message := fmt.Sprintf("已完成: %d/%d, 平均耗时: %v, 内存: %.2fMB, 协程数: %d",
					completed, totalTasks, avgDuration,
					float64(memStats.Alloc)/1024/1024, runtime.NumGoroutine())

				config.ProgressCallback(progress, message)
			}

			// 每处理50个任务输出一次详细统计
			if completed%50 == 0 || completed == totalTasks {
				avgDuration := totalDuration / time.Duration(completed)
				log.Printf("进度统计 - 已完成: %d/%d, 平均耗时: %v, 最快: %v, 最慢: %v",
					completed, totalTasks, avgDuration, minDuration, maxDuration)
			}
		}

		log.Printf("所有分块处理完成，总计: %d", completed)
	}()

	// 等待所有工作协程完成
	wg.Wait()
	close(results) // 关闭结果队列

	// 等待结果收集完成
	resultWg.Wait()

	if processingError != nil {
		return processingError
	}

	return nil
}

func worker_intersection(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult, config *ParallelGeosConfig, wg *sync.WaitGroup, strategy FieldMergeStrategy) {
	defer wg.Done()

	tasksProcessed := 0

	for tileGroup := range taskQueue {

		start := time.Now()

		// 处理单个分块
		layer, err := processTileGroupforIntersection(tileGroup, config, strategy)

		duration := time.Since(start)

		tasksProcessed++

		// 发送结果
		results <- taskResult{
			layer:    layer,
			err:      err,
			duration: duration,
			index:    tileGroup.Index,
		}
		runtime.GC()

	}

}

func processTileGroupforIntersection(tileGroup GroupTileFiles, config *ParallelGeosConfig, strategy FieldMergeStrategy) (*GDALLayer, error) {

	// 加载layer1的bin文件
	inputTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer1)
	if err != nil {
		return nil, fmt.Errorf("加载输入分块文件失败: %v", err)
	}

	// 加载layer2的bin文件
	methodTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer2)
	if err != nil {
		return nil, fmt.Errorf("加载擦除分块文件失败: %v", err)
	}
	defer func() {
		inputTileLayer.Close()
		methodTileLayer.Close()

	}()

	// 为当前分块创建临时结果图层
	tileName := fmt.Sprintf("tile_result_%d", tileGroup.Index)
	tileResultLayer, err := createIntersectionTileResultLayer(inputTileLayer, methodTileLayer, tileName, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}

	// 执行裁剪分析 - 不使用进度回调
	err = executeIntersectionWithStrategy(inputTileLayer, methodTileLayer, tileResultLayer, strategy, nil)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行擦除分析失败: %v", err)
	}

	return tileResultLayer, nil
}

func createIntersectionTileResultLayer(inputLayer, methodLayer *GDALLayer, layerName string, strategy FieldMergeStrategy) (*GDALLayer, error) {
	layerNameC := C.CString(layerName)
	defer C.free(unsafe.Pointer(layerNameC))

	// 获取空间参考系统
	srs := inputLayer.GetSpatialRef()

	// 创建结果图层
	resultLayerPtr := C.createMemoryLayer(layerNameC, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 根据策略添加字段定义
	err := addFieldsBasedOnStrategy(resultLayer, inputLayer, methodLayer, strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

func executeIntersectionWithStrategy(layer1, layer2, resultLayer *GDALLayer, strategy FieldMergeStrategy, progressCallback ProgressCallback) error {
	var options **C.char
	defer func() {
		if options != nil {
			C.CSLDestroy(options)
		}
	}()

	switch strategy {
	case UseTable1Fields:
		// 只保留输入图层的字段
		skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
		promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
		inputFieldsOpt := C.CString("INPUT_FIELDS_ONLY=YES")
		keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
		defer C.free(unsafe.Pointer(skipFailuresOpt))
		defer C.free(unsafe.Pointer(promoteToMultiOpt))
		defer C.free(unsafe.Pointer(inputFieldsOpt))
		defer C.free(unsafe.Pointer(keepLowerDimOpt))

		options = C.CSLAddString(options, skipFailuresOpt)
		options = C.CSLAddString(options, promoteToMultiOpt)
		options = C.CSLAddString(options, inputFieldsOpt)
		options = C.CSLAddString(options, keepLowerDimOpt)

		return executeGDALIntersection(layer1, layer2, resultLayer, options, progressCallback)

	case UseTable2Fields:
		// 只保留方法图层的字段
		skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
		promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
		methodFieldsOpt := C.CString("METHOD_FIELDS_ONLY=YES")
		keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
		defer C.free(unsafe.Pointer(skipFailuresOpt))
		defer C.free(unsafe.Pointer(promoteToMultiOpt))
		defer C.free(unsafe.Pointer(methodFieldsOpt))
		defer C.free(unsafe.Pointer(keepLowerDimOpt))

		options = C.CSLAddString(options, skipFailuresOpt)
		options = C.CSLAddString(options, promoteToMultiOpt)
		options = C.CSLAddString(options, methodFieldsOpt)
		options = C.CSLAddString(options, keepLowerDimOpt)

		return executeGDALIntersection(layer1, layer2, resultLayer, options, progressCallback)

	case MergePreferTable1:
		// 合并字段，冲突时优先使用输入图层
		skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
		promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
		keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
		defer C.free(unsafe.Pointer(skipFailuresOpt))
		defer C.free(unsafe.Pointer(promoteToMultiOpt))
		defer C.free(unsafe.Pointer(keepLowerDimOpt))

		options = C.CSLAddString(options, skipFailuresOpt)
		options = C.CSLAddString(options, promoteToMultiOpt)
		options = C.CSLAddString(options, keepLowerDimOpt)

		// 默认行为就是输入图层优先
		return executeGDALIntersection(layer1, layer2, resultLayer, options, progressCallback)

	case MergePreferTable2:
		// 合并字段，冲突时优先使用方法图层 - 交换图层顺序
		skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
		promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
		keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
		defer C.free(unsafe.Pointer(skipFailuresOpt))
		defer C.free(unsafe.Pointer(promoteToMultiOpt))
		defer C.free(unsafe.Pointer(keepLowerDimOpt))

		options = C.CSLAddString(options, skipFailuresOpt)
		options = C.CSLAddString(options, promoteToMultiOpt)
		options = C.CSLAddString(options, keepLowerDimOpt)

		// 交换图层顺序，让table2作为输入图层
		return executeGDALIntersection(layer2, layer1, resultLayer, options, progressCallback)

	case MergeWithPrefix:
		// 使用前缀区分字段来源
		skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
		promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
		inputPrefixOpt := C.CString(fmt.Sprintf("INPUT_PREFIX="))
		methodPrefixOpt := C.CString(fmt.Sprintf("METHOD_PREFIX=l2_"))
		keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
		defer C.free(unsafe.Pointer(skipFailuresOpt))
		defer C.free(unsafe.Pointer(promoteToMultiOpt))
		defer C.free(unsafe.Pointer(inputPrefixOpt))
		defer C.free(unsafe.Pointer(methodPrefixOpt))
		defer C.free(unsafe.Pointer(keepLowerDimOpt))

		options = C.CSLAddString(options, skipFailuresOpt)
		options = C.CSLAddString(options, promoteToMultiOpt)
		options = C.CSLAddString(options, inputPrefixOpt)
		options = C.CSLAddString(options, methodPrefixOpt)
		options = C.CSLAddString(options, keepLowerDimOpt)

		return executeGDALIntersection(layer1, layer2, resultLayer, options, progressCallback)

	default:
		return fmt.Errorf("不支持的字段合并策略: %v", strategy)
	}
}

func CreateIntersectionResultLayer(layer1, layer2 *GDALLayer, strategy FieldMergeStrategy) (*GDALLayer, error) {
	layerName := C.CString("intersection_result")
	defer C.free(unsafe.Pointer(layerName))

	// 获取空间参考系统
	srs := layer1.GetSpatialRef()
	if srs == nil {
		srs = layer2.GetSpatialRef()
	}

	// 创建结果图层
	resultLayerPtr := C.createMemoryLayer(layerName, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 根据策略添加字段定义
	err := addFieldsBasedOnStrategy(resultLayer, layer1, layer2, strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

// 新增函数：根据策略添加字段
func addFieldsBasedOnStrategy(resultLayer, layer1, layer2 *GDALLayer, strategy FieldMergeStrategy) error {

	switch strategy {
	case UseTable1Fields:
		// 只添加table1的字段
		return addLayerFields(resultLayer, layer1, "")

	case UseTable2Fields:
		// 只添加table2的字段
		return addLayerFields(resultLayer, layer2, "")

	case MergePreferTable1:
		// 先添加table1字段，再添加table2中不冲突的字段
		if err := addLayerFields(resultLayer, layer1, ""); err != nil {
			return err
		}
		return addNonConflictingFields(resultLayer, layer2, layer1, "")

	case MergePreferTable2:
		// 先添加table2字段，再添加table1中不冲突的字段
		if err := addLayerFields(resultLayer, layer2, ""); err != nil {
			return err
		}
		return addNonConflictingFields(resultLayer, layer1, layer2, "")

	case MergeWithPrefix:
		// 添加带前缀的字段
		if err := addLayerFields(resultLayer, layer1, ""); err != nil {
			return err
		}
		return addLayerFields(resultLayer, layer2, "l2_")

	default:
		return fmt.Errorf("不支持的字段策略: %v", strategy)
	}
}

// 添加不冲突的字段
func addNonConflictingFields(resultLayer, sourceLayer, existingLayer *GDALLayer, prefix string) error {
	sourceDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	resultDefn := C.OGR_L_GetLayerDefn(resultLayer.layer)

	fieldCount := int(C.OGR_FD_GetFieldCount(sourceDefn))

	for i := 0; i < fieldCount; i++ {
		fieldDefn := C.OGR_FD_GetFieldDefn(sourceDefn, C.int(i))
		fieldName := C.OGR_Fld_GetNameRef(fieldDefn)
		fieldNameStr := C.GoString(fieldName)

		// 检查字段是否已存在
		fieldIndex := C.OGR_FD_GetFieldIndex(resultDefn, fieldName)
		if fieldIndex >= 0 {
			// 字段已存在，跳过
			continue
		}

		// 添加字段
		var newFieldName string
		if prefix != "" {
			newFieldName = prefix + fieldNameStr
		} else {
			newFieldName = fieldNameStr
		}

		newFieldNameC := C.CString(newFieldName)
		newFieldDefn := C.OGR_Fld_Create(newFieldNameC, C.OGR_Fld_GetType(fieldDefn))

		C.OGR_Fld_SetWidth(newFieldDefn, C.OGR_Fld_GetWidth(fieldDefn))
		C.OGR_Fld_SetPrecision(newFieldDefn, C.OGR_Fld_GetPrecision(fieldDefn))

		err := C.OGR_L_CreateField(resultLayer.layer, newFieldDefn, 1)

		C.OGR_Fld_Destroy(newFieldDefn)
		C.free(unsafe.Pointer(newFieldNameC))

		if err != C.OGRERR_NONE {
			return fmt.Errorf("创建字段 %s 失败", newFieldName)
		}
	}

	return nil
}

// GDAL相交分析
func executeGDALIntersection(inputLayer, methodLayer, resultLayer *GDALLayer, options **C.char, progressCallback ProgressCallback) error {
	var progressData *ProgressData
	var progressArg unsafe.Pointer
	// 启用多线程处理
	C.CPLSetConfigOption(C.CString("GDAL_NUM_THREADS"), C.CString("ALL_CPUS"))
	defer C.CPLSetConfigOption(C.CString("GDAL_NUM_THREADS"), nil)
	// 如果有进度回调，设置进度数据
	if progressCallback != nil {
		progressData = &ProgressData{
			callback:  progressCallback,
			cancelled: false,
		}

		// 将进度数据存储到全局映射中
		progressArg = unsafe.Pointer(progressData)
		progressDataMutex.Lock()
		progressDataMap[uintptr(progressArg)] = progressData
		progressDataMutex.Unlock()

		// 确保在函数结束时清理进度数据
		defer func() {
			progressDataMutex.Lock()
			delete(progressDataMap, uintptr(progressArg))
			progressDataMutex.Unlock()
		}()
	}

	// 调用C函数执行相交分析
	err := C.performIntersectionWithProgress(
		inputLayer.layer,
		methodLayer.layer,
		resultLayer.layer,
		options,
		progressArg,
	)

	if err != C.OGRERR_NONE {

		return fmt.Errorf("GDAL相交分析失败，错误代码: %d", int(err))
	}

	return nil
}

//PG版本

func processTileGroupforIntersectionOptimized(tileGroup GroupTileFiles, config *ParallelGeosConfig, strategy FieldMergeStrategy) (*GDALLayer, error) {
	// 加载layer1的bin文件
	inputTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer1)
	if err != nil {
		return nil, fmt.Errorf("加载输入分块文件失败: %v", err)
	}
	// 加载layer2的bin文件
	methodTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer2)
	if err != nil {
		inputTileLayer.Close()
		return nil, fmt.Errorf("加载擦除分块文件失败: %v", err)
	}

	defer func() {
		inputTileLayer.Close()
		methodTileLayer.Close()
	}()
	// **关键修改：在这里应用精度设置**
	if config.PrecisionConfig != nil && config.PrecisionConfig.Enabled {
		err = applyPrecisionToLayer(inputTileLayer, config.PrecisionConfig)
		if err != nil {
			return nil, fmt.Errorf("应用精度到输入图层失败: %v", err)
		}
		err = applyPrecisionToLayer(methodTileLayer, config.PrecisionConfig)
		if err != nil {
			return nil, fmt.Errorf("应用精度到方法图层失败: %v", err)
		}
	}
	// 为当前分块创建临时结果图层
	tileName := fmt.Sprintf("tile_result_%d", tileGroup.Index)
	tileResultLayer, err := createIntersectionTileResultLayer(inputTileLayer, methodTileLayer, tileName, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}
	// 执行相交分析
	err = executeIntersectionWithStrategy(inputTileLayer, methodTileLayer, tileResultLayer, strategy, nil)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行相交分析失败: %v", err)
	}
	return tileResultLayer, nil
}

// applyPrecisionToLayer 对图层应用精度设置
func applyPrecisionToLayer(layer *GDALLayer, precisionConfig *GeometryPrecisionConfig) error {
	if layer == nil || layer.layer == nil {
		return fmt.Errorf("无效的图层")
	}
	flags := precisionConfig.getFlags()
	gridSize := C.double(precisionConfig.GridSize)
	// 调用C函数设置精度
	C.setLayerGeometryPrecision(layer.layer, gridSize, flags)
	return nil
}

// 修改worker函数以使用优化版本
func worker_intersection_optimized(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult, config *ParallelGeosConfig, wg *sync.WaitGroup, strategy FieldMergeStrategy) {
	defer wg.Done()
	tasksProcessed := 0
	for tileGroup := range taskQueue {
		start := time.Now()
		// 使用优化版本处理单个分块（包含精度应用）
		layer, err := processTileGroupforIntersectionOptimized(tileGroup, config, strategy)
		duration := time.Since(start)
		tasksProcessed++
		// 发送结果
		results <- taskResult{
			layer:    layer,
			err:      err,
			duration: duration,
			index:    tileGroup.Index,
		}

		runtime.GC()
	}
}

// 修改ExecuteConcurrentIntersectionAnalysis以支持优化版本
func ExecuteConcurrentIntersectionAnalysisOptimized(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig, strategy FieldMergeStrategy) error {
	maxWorkers := config.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}
	totalTasks := len(tileGroups)
	if totalTasks == 0 {
		return fmt.Errorf("没有分块需要处理")
	}
	// 创建任务队列和结果队列
	taskQueue := make(chan GroupTileFiles, totalTasks)
	results := make(chan taskResult, totalTasks)
	// 启动固定数量的工作协程（使用优化版本的worker）
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker_intersection_optimized(i, taskQueue, results, config, &wg, strategy)
	}
	// 发送所有任务到队列
	go func() {
		for _, tileGroup := range tileGroups {
			taskQueue <- tileGroup
		}
		close(taskQueue)
	}()
	// 启动结果收集协程
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	var processingError error
	completed := 0
	go func() {
		defer resultWg.Done()
		var totalDuration time.Duration
		var minDuration, maxDuration time.Duration
		for i := 0; i < totalTasks; i++ {
			result := <-results
			completed++
			if result.err != nil {
				processingError = fmt.Errorf("分块 %d 处理失败: %v", result.index, result.err)
				log.Printf("错误: %v", processingError)
				return
			}
			// 统计执行时间
			totalDuration += result.duration
			if i == 0 {
				minDuration = result.duration
				maxDuration = result.duration
			} else {
				if result.duration < minDuration {
					minDuration = result.duration
				}
				if result.duration > maxDuration {
					maxDuration = result.duration
				}
			}
			// 将结果合并到主图层
			if result.layer != nil {
				err := mergeResultsToMainLayer(result.layer, resultLayer)
				if err != nil {
					processingError = fmt.Errorf("合并分块 %d 结果失败: %v", result.index, err)
					log.Printf("错误: %v", processingError)
					return
				}
				// 释放临时图层资源
				result.layer.Close()
			}
			// 进度回调
			if config.ProgressCallback != nil {
				progress := float64(completed) / float64(totalTasks)
				avgDuration := totalDuration / time.Duration(completed)
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				message := fmt.Sprintf("已完成: %d/%d, 平均耗时: %v, 内存: %.2fMB, 协程数: %d",
					completed, totalTasks, avgDuration,
					float64(memStats.Alloc)/1024/1024, runtime.NumGoroutine())
				config.ProgressCallback(progress, message)
			}
			// 每处理50个任务输出一次详细统计
			if completed%50 == 0 || completed == totalTasks {
				avgDuration := totalDuration / time.Duration(completed)
				log.Printf("进度统计 - 已完成: %d/%d, 平均耗时: %v, 最快: %v, 最慢: %v",
					completed, totalTasks, avgDuration, minDuration, maxDuration)
			}
		}
		log.Printf("所有分块处理完成，总计: %d", completed)
	}()
	// 等待所有工作协程完成
	wg.Wait()
	close(results)
	// 等待结果收集完成
	resultWg.Wait()
	if processingError != nil {
		return processingError
	}
	return nil
}

func performIntersectionAnalysisOptimized(inputLayer, methodLayer *GDALLayer, strategy FieldMergeStrategy, config *ParallelGeosConfig, useOptimized bool) (*GDALLayer, error) {
	// **移除精度预处理部分**
	// 不再在这里创建内存副本和应用精度
	// 精度将在反序列化bin文件后应用
	// 创建结果图层
	resultLayer, err := CreateIntersectionResultLayer(inputLayer, methodLayer, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}

	taskid := uuid.New().String()

	if useOptimized {
		// 使用优化版本：直接从PG生成bin文件
		// 注意：这里需要传入数据库连接和表名
		// 这个函数签名需要调整，或者在更上层调用
		log.Printf("使用优化版本生成瓦片")
	} else {
		// 使用原版本：对GDALLayer进行分块
		GenerateTiles(inputLayer, methodLayer, config.TileCount, taskid)
	}

	// 读取文件列表，并发执行相交操作
	GPbins, err := ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("提取分组文件失败: %v", err)
	}

	// 并发执行分析（使用优化版本，会在反序列化后应用精度）
	err = ExecuteConcurrentIntersectionAnalysisOptimized(GPbins, resultLayer, config, strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("并发分析失败: %v", err)
	}

	// 清理临时文件
	defer func() {
		err := cleanupTileFiles(taskid)
		if err != nil {
			log.Printf("清理临时文件失败: %v", err)
		}
	}()

	return resultLayer, nil
}
