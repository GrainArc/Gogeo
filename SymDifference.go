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
static OGRErr performSymDifferenceWithProgress(OGRLayerH inputLayer,
                                     OGRLayerH methodLayer,
                                     OGRLayerH resultLayer,
                                     char **options,
                                     void *progressData) {
    return OGR_L_SymDifference(inputLayer, methodLayer, resultLayer, options,
                             progressCallback, progressData);
}



*/
import "C"
import (
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"log"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

func SpatialSymDifferenceAnalysis(inputLayer, methodLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error) {

	defer inputLayer.Close()

	defer methodLayer.Close()

	// 为两个图层添加唯一标识字段
	err := addIdentifierField(inputLayer, "gogeo_analysis_id")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}
	err = addIdentifierField(methodLayer, "gogeo_analysis_id2")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}

	// 执行基于瓦片裁剪的并行对称差异分析
	resultLayer, err := performSymDifferenceAnalysis(inputLayer, methodLayer, config)
	if err != nil {
		return nil, fmt.Errorf("执行瓦片裁剪对称差异分析失败: %v", err)
	}

	// 计算结果数量
	resultCount := resultLayer.GetFeatureCount()

	fmt.Printf("对称差异分析完成，共生成 %d 个要素\n", resultCount)

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

func performSymDifferenceAnalysis(inputLayer, methodLayer *GDALLayer, config *ParallelGeosConfig) (*GDALLayer, error) {
	if config.PrecisionConfig != nil {
		// 创建内存副本
		inputMemLayer, err := createMemoryLayerCopy(inputLayer, "input_mem_layer")
		if err != nil {
			return nil, fmt.Errorf("创建输入图层内存副本失败: %v", err)
		}

		methodMemLayer, err := createMemoryLayerCopy(methodLayer, "erase_mem_layer")
		if err != nil {
			inputMemLayer.Close()
			return nil, fmt.Errorf("创建擦除图层内存副本失败: %v", err)
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
	resultLayer, err := createSymDifferenceResultLayer(inputLayer, methodLayer)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}
	taskid := uuid.New().String()
	//对A B图层进行分块,并创建bin文件
	GenerateTiles(inputLayer, methodLayer, config.TileCount, taskid)
	//读取文件列表，并发执行操作
	GPbins, err := ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("提取分组文件失败: %v", err)
	}
	// 并发执行分析
	err = executeConcurrentSymDifferenceAnalysis(GPbins, resultLayer, config)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("并发擦除分析失败: %v", err)
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
func executeConcurrentSymDifferenceAnalysis(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig) error {
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
		go worker_symDifference(i, taskQueue, results, config, &wg)
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

func worker_symDifference(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult,
	config *ParallelGeosConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	tasksProcessed := 0

	for tileGroup := range taskQueue {

		start := time.Now()

		// 处理单个分块
		layer, err := processTileGroupforSymDifference(tileGroup, config)

		duration := time.Since(start)

		tasksProcessed++

		// 发送结果
		results <- taskResult{
			layer:    layer,
			err:      err,
			duration: duration,
			index:    tileGroup.Index,
		}

		// 定期强制垃圾回收

		runtime.GC()

	}

}
func processTileGroupforSymDifference(tileGroup GroupTileFiles, config *ParallelGeosConfig) (*GDALLayer, error) {

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
	tileResultLayer, err := createSymDifferenceTileResultLayer(inputTileLayer, methodTileLayer, tileName)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}

	// 执行裁剪分析 - 不使用进度回调
	err = executeSymDifferenceAnalysis(inputTileLayer, methodTileLayer, tileResultLayer, nil)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行擦除分析失败: %v", err)
	}
	return tileResultLayer, nil
}

func createSymDifferenceTileResultLayer(inputLayer, methodLayer *GDALLayer, layerName string) (*GDALLayer, error) {
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

	// 添加字段定义 - 使用默认策略（合并字段，带前缀区分来源）
	err := addSymDifferenceFields(resultLayer, inputLayer, methodLayer)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

// createGeosAnalysisResultLayer 创建对称差异结果图层
func createSymDifferenceResultLayer(layer1, layer2 *GDALLayer) (*GDALLayer, error) {
	layerName := C.CString("symdifference_result")
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

	// 添加字段定义 - 使用默认策略（合并字段，带前缀区分来源）
	err := addSymDifferenceFields(resultLayer, layer1, layer2)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

// addSymDifferenceFields 添加对称差异分析的字段（修改版本）
func addSymDifferenceFields(resultLayer, layer1, layer2 *GDALLayer) error {

	// 合并两个图层的字段（不使用前缀）
	err1 := addLayerFields(resultLayer, layer1, "")
	if err1 != nil {
		return fmt.Errorf("添加图层1字段失败: %v", err1)
	}

	err2 := addLayerFields(resultLayer, layer2, "")
	if err2 != nil {
		return fmt.Errorf("添加图层2字段失败: %v", err2)
	}

	return nil
}

// executeSymDifferenceAnalysis 执行对称差异分析
func executeSymDifferenceAnalysis(layer1, layer2, resultLayer *GDALLayer, progressCallback ProgressCallback) error {
	// 设置GDAL选项
	var options **C.char
	defer func() {
		if options != nil {
			C.CSLDestroy(options)
		}
	}()

	skipFailuresOpt := C.CString("SKIP_FAILURES=YES")
	promoteToMultiOpt := C.CString("PROMOTE_TO_MULTI=YES")
	keepLowerDimOpt := C.CString("KEEP_LOWER_DIMENSION_GEOMETRIES=NO")
	defer C.free(unsafe.Pointer(skipFailuresOpt))
	defer C.free(unsafe.Pointer(promoteToMultiOpt))
	defer C.free(unsafe.Pointer(keepLowerDimOpt))

	options = C.CSLAddString(options, skipFailuresOpt)
	options = C.CSLAddString(options, promoteToMultiOpt)
	options = C.CSLAddString(options, keepLowerDimOpt)

	// 执行对称差异操作
	return executeGDALSymDifference(layer1, layer2, resultLayer, options, progressCallback)
}

// executeGDALSymDifference 执行带进度的GDAL对称差异操作
func executeGDALSymDifference(layer1, layer2, resultLayer *GDALLayer, options **C.char, progressCallback ProgressCallback) error {
	var progressData *ProgressData
	var progressArg unsafe.Pointer

	// 设置进度回调
	if progressCallback != nil {
		progressData = &ProgressData{
			callback:  progressCallback,
			cancelled: false,
		}
		progressArg = unsafe.Pointer(uintptr(unsafe.Pointer(progressData)))

		progressDataMutex.Lock()
		progressDataMap[uintptr(progressArg)] = progressData
		progressDataMutex.Unlock()

		defer func() {
			progressDataMutex.Lock()
			delete(progressDataMap, uintptr(progressArg))
			progressDataMutex.Unlock()
		}()
	}

	// 调用GDAL的对称差异函数
	var err C.OGRErr
	if progressCallback != nil {
		err = C.performSymDifferenceWithProgress(layer1.layer, layer2.layer, resultLayer.layer, options, progressArg)
	} else {
		err = C.OGR_L_SymDifference(layer1.layer, layer2.layer, resultLayer.layer, options, nil, nil)
	}

	if err != C.OGRERR_NONE {
		return fmt.Errorf("GDAL对称差异操作失败，错误代码: %d", int(err))
	}

	return nil
}

// PG优化版本的对称差异分析
func SpatialSymDifferenceAnalysisParallelPG(
	db *gorm.DB,
	table1, table2 string,
	config *ParallelGeosConfig,
) (*GeosAnalysisResult, error) {

	taskid := uuid.New().String()
	// 1. 直接从PG生成瓦片bin文件（优化版本）
	log.Printf("开始从PostgreSQL生成瓦片...")
	err := GenerateTilesFromPG(db, table1, table2, config.TileCount, taskid)
	if err != nil {
		return nil, fmt.Errorf("生成瓦片失败: %v", err)
	}
	// 2. 读取bin文件分组
	log.Printf("读取瓦片分组...")
	GPbins, err := ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("读取分组文件失败: %v", err)
	}

	resultLayer, err := createSymDifferenceResultLayerFromBin(GPbins, table1, table2)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}

	log.Printf("开始并发执行对称差异分析...")
	err = ExecuteConcurrentSymDifferenceAnalysisPG(GPbins, resultLayer, config)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("并发分析失败: %v", err)
	}
	// 5. 清理临时文件
	defer func() {
		err := CleanupTileFiles(taskid)
		if err != nil {
			log.Printf("清理临时文件失败: %v", err)
		}
	}()
	// 6. 计算结果数量
	resultCount := resultLayer.GetFeatureCount()
	log.Printf("对称差异分析完成，共生成 %d 个要素", resultCount)
	resultLayer.PrintLayerInfo()
	// 7. 如果需要合并瓦片
	if config.IsMergeTile {
		log.Printf("开始合并瓦片...")
		unionResult, err := PerformUnionByFieldsPG(resultLayer, config.PrecisionConfig, config.ProgressCallback)
		if err != nil {
			return nil, fmt.Errorf("执行融合操作失败: %v", err)
		}
		// 删除临时标识字段
		err = DeleteFieldFromLayer(unionResult.OutputLayer, "id")
		if err != nil {
			log.Printf("警告: 删除临时标识字段失败: %v", err)
		}
		log.Printf("合并完成，最终结果: %d 个要素", unionResult.ResultCount)
		return unionResult, nil
	}
	return &GeosAnalysisResult{
		OutputLayer: resultLayer,
		ResultCount: resultCount,
	}, nil
}

// createSymDifferenceResultLayerFromBin 从bin文件创建对称差异结果图层
func createSymDifferenceResultLayerFromBin(GPbins []GroupTileFiles, table1, table2 string) (*GDALLayer, error) {
	// 找到第一个非空的bin文件
	var layer1Path, layer2Path string

	for _, group := range GPbins {
		// 检查文件是否存在且非空
		if IsValidBinFile(group.GPBin.Layer1) {
			layer1Path = group.GPBin.Layer1
		}
		if IsValidBinFile(group.GPBin.Layer2) {
			layer2Path = group.GPBin.Layer2
		}

		if layer1Path != "" && layer2Path != "" {
			break
		}
	}
	if layer1Path == "" || layer2Path == "" {
		return nil, fmt.Errorf("未找到有效的bin文件")
	}
	// 反序列化第一个bin文件以获取schema信息
	tempLayer1, err := DeserializeLayerFromFile(layer1Path)
	if err != nil {
		return nil, fmt.Errorf("反序列化layer1失败: %v", err)
	}
	defer tempLayer1.Close()
	tempLayer2, err := DeserializeLayerFromFile(layer2Path)
	if err != nil {
		return nil, fmt.Errorf("反序列化layer2失败: %v", err)
	}
	defer tempLayer2.Close()
	// 创建结果图层（复用现有函数）
	return createSymDifferenceResultLayer(tempLayer1, tempLayer2)
}

// processTileGroupforSymDifferencePG PG优化版本的对称差异分块处理
func processTileGroupforSymDifferencePG(tileGroup GroupTileFiles, config *ParallelGeosConfig) (*GDALLayer, error) {
	// 加载layer1的bin文件
	inputTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer1)
	if err != nil {
		return nil, fmt.Errorf("加载输入分块文件失败: %v", err)
	}
	// 加载layer2的bin文件
	methodTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer2)
	if err != nil {
		inputTileLayer.Close()
		return nil, fmt.Errorf("加载方法分块文件失败: %v", err)
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
	tileResultLayer, err := createSymDifferenceTileResultLayer(inputTileLayer, methodTileLayer, tileName)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}
	// 执行对称差异分析
	err = executeSymDifferenceAnalysis(inputTileLayer, methodTileLayer, tileResultLayer, nil)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行对称差异分析失败: %v", err)
	}
	return tileResultLayer, nil
}

// worker_symDifference_pg PG优化版本的对称差异工作协程
func worker_symDifference_pg(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult, config *ParallelGeosConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	tasksProcessed := 0
	for tileGroup := range taskQueue {
		start := time.Now()
		// 使用优化版本处理单个分块（包含精度应用）
		layer, err := processTileGroupforSymDifferencePG(tileGroup, config)
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

// ExecuteConcurrentSymDifferenceAnalysisPG PG优化版本的并发对称差异分析
func ExecuteConcurrentSymDifferenceAnalysisPG(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig) error {
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
		go worker_symDifference_pg(i, taskQueue, results, config, &wg)
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
