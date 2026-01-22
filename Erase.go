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
static OGRErr performEraseWithProgress(OGRLayerH inputLayer,
                                     OGRLayerH methodLayer,
                                     OGRLayerH resultLayer,
                                     char **options,
                                     void *progressData) {
    return OGR_L_Erase(inputLayer, methodLayer, resultLayer, options,
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
	"sync/atomic"
	"time"
	"unsafe"
)

// SpatialEraseAnalysis执行并行空间擦除分析
func SpatialEraseAnalysis(inputLayer, methodlayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error) {
	// 读取输入图层

	defer inputLayer.Close()

	// 读取擦除图层

	defer methodlayer.Close()

	// 为输入图层添加唯一标识字段（用于后续融合）
	err := addIdentifierField(inputLayer, "gogeo_analysis_id")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}
	inputTable := inputLayer.GetLayerName()
	eraseTable := methodlayer.GetLayerName()

	// 执行基于瓦片裁剪的并行擦除分析
	resultLayer, err := performTileClipEraseAnalysis(inputLayer, methodlayer, inputTable, eraseTable, config)
	if err != nil {
		return nil, fmt.Errorf("执行瓦片裁剪擦除分析失败: %v", err)
	}

	// 计算结果数量
	resultCount := resultLayer.GetFeatureCount()

	if config.IsMergeTile == true {
		fmt.Println("配置要求执行融合操作，开始融合...")
		unionResult, err := PerformUnionByFields(resultLayer, config.PrecisionConfig, config.ProgressCallback)
		if err != nil {
			return nil, fmt.Errorf("执行融合操作失败: %v", err)
		}

		fmt.Printf("融合操作完成，最终生成 %d 个要素\n", unionResult.ResultCount)
		// 删除临时的字段
		err = deleteFieldFromLayer(unionResult.OutputLayer, "gogeo_analysis_id")
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

// performTileClipEraseAnalysis 执行基于瓦片裁剪的并行擦除分析
func performTileClipEraseAnalysis(inputLayer, eraseLayer *GDALLayer, inputTableName, eraseTableName string, config *ParallelGeosConfig) (*GDALLayer, error) {
	// 如果启用了精度设置，在分块裁剪前对原始图层进行精度处理

	if config.PrecisionConfig != nil {
		// 创建内存副本
		inputMemLayer, err := createMemoryLayerCopy(inputLayer, "input_mem_layer")
		if err != nil {
			return nil, fmt.Errorf("创建输入图层内存副本失败: %v", err)
		}

		eraseMemLayer, err := createMemoryLayerCopy(eraseLayer, "erase_mem_layer")
		if err != nil {
			inputMemLayer.Close()
			return nil, fmt.Errorf("创建擦除图层内存副本失败: %v", err)
		}

		// 在内存图层上设置精度
		if config.PrecisionConfig.Enabled {
			flags := config.PrecisionConfig.getFlags()
			gridSize := C.double(config.PrecisionConfig.GridSize)

			C.setLayerGeometryPrecision(inputMemLayer.layer, gridSize, flags)
			C.setLayerGeometryPrecision(eraseMemLayer.layer, gridSize, flags)
		}
		// 使用内存图层进行后续处理
		inputLayer = inputMemLayer
		eraseLayer = eraseMemLayer
	}
	// 创建结果图层
	resultLayer, err := createEraseAnalysisResultLayer(inputLayer)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}
	taskid := uuid.New().String()

	//对A B图层进行分块,并创建bin文件
	GenerateTiles(inputLayer, eraseLayer, config.TileCount, taskid)
	//读取文件列表，并发执行擦除操作
	GPbins, err := ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("提取分组文件失败: %v", err)
	}

	// 并发执行擦除分析
	err = executeConcurrentEraseAnalysis(GPbins, resultLayer, config)
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

func executeConcurrentEraseAnalysis(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig) error {
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
		go worker(i, taskQueue, results, config, &wg)
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

func worker(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult, config *ParallelGeosConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	tasksProcessed := 0

	for tileGroup := range taskQueue {

		start := time.Now()

		// 处理单个分块
		layer, err := processTileGroup(tileGroup, config)

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

func processTileGroup(tileGroup GroupTileFiles, config *ParallelGeosConfig) (*GDALLayer, error) {

	// 加载layer1的bin文件
	inputTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer1)
	if err != nil {
		return nil, fmt.Errorf("加载输入分块文件失败: %v", err)
	}

	// 加载layer2的bin文件
	eraseTileLayer, err := DeserializeLayerFromFile(tileGroup.GPBin.Layer2)
	if err != nil {
		return nil, fmt.Errorf("加载擦除分块文件失败: %v", err)
	}
	defer func() {
		inputTileLayer.Close()
		eraseTileLayer.Close()

	}()

	// 为当前分块创建临时结果图层
	tileName := fmt.Sprintf("tile_result_%d", tileGroup.Index)
	tileResultLayer, err := createTileResultLayer(inputTileLayer, tileName)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}

	// 执行擦除分析 - 不使用进度回调
	err = executeEraseAnalysis(inputTileLayer, eraseTileLayer, tileResultLayer, nil)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行擦除分析失败: %v", err)
	}
	return tileResultLayer, nil
}

// createEraseAnalysisResultLayer 创建擦除分析结果图层
func createEraseAnalysisResultLayer(inputLayer *GDALLayer) (*GDALLayer, error) {
	layerName := C.CString("erase_result")
	defer C.free(unsafe.Pointer(layerName))

	// 获取空间参考系统
	srs := inputLayer.GetSpatialRef()

	// 创建结果图层
	resultLayerPtr := C.createMemoryLayer(layerName, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 添加字段定义 - 只需要输入图层的字段
	err := addLayerFields(resultLayer, inputLayer, "")

	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}

// executeEraseAnalysis 执行擦除分析
func executeEraseAnalysis(inputLayer, eraseLayer, resultLayer *GDALLayer, progressCallback ProgressCallback) error {
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
	usePreparatedGeomOpt := C.CString("USE_PREPARED_GEOMETRIES=YES")
	methodOpt := C.CString("METHOD=FAST") // 或者 "ACCURATE"
	defer C.free(unsafe.Pointer(skipFailuresOpt))
	defer C.free(unsafe.Pointer(promoteToMultiOpt))
	defer C.free(unsafe.Pointer(keepLowerDimOpt))
	defer C.free(unsafe.Pointer(usePreparatedGeomOpt))
	defer C.free(unsafe.Pointer(methodOpt))
	options = C.CSLAddString(options, skipFailuresOpt)
	options = C.CSLAddString(options, promoteToMultiOpt)
	options = C.CSLAddString(options, keepLowerDimOpt)
	options = C.CSLAddString(options, usePreparatedGeomOpt)
	options = C.CSLAddString(options, methodOpt)

	// 执行擦除操作
	return executeGDALEraseWithProgress(inputLayer, eraseLayer, resultLayer, options, progressCallback)
}

// 修改执行函数
func executeGDALEraseWithProgress(inputLayer, eraseLayer, resultLayer *GDALLayer, options **C.char, progressCallback ProgressCallback) error {
	// 首先修复几何体拓扑

	fixGeometryTopology(inputLayer)
	fixGeometryTopology(eraseLayer)

	var err C.OGRErr

	if progressCallback != nil {
		// 创建进度数据结构
		progressData := &ProgressData{
			callback:  progressCallback,
			cancelled: false,
		}

		// 生成唯一ID
		progressID := atomic.AddInt64(&progressIDCounter, 1)
		progressKey := uintptr(progressID)

		progressDataMutex.Lock()
		progressDataMap[progressKey] = progressData
		progressDataMutex.Unlock()

		// 清理函数
		defer func() {
			progressDataMutex.Lock()
			delete(progressDataMap, progressKey)
			progressDataMutex.Unlock()
		}()

		// 传递ID值
		err = C.performEraseWithProgress(inputLayer.layer, eraseLayer.layer, resultLayer.layer, options, unsafe.Pointer(progressKey))
	} else {
		err = C.OGR_L_Erase(inputLayer.layer, eraseLayer.layer, resultLayer.layer, options, nil, nil)
	}

	// 执行后立即清理GDAL内部缓存
	defer func() {
		// 强制清理图层缓存
		C.OGR_L_ResetReading(inputLayer.layer)
		C.OGR_L_ResetReading(eraseLayer.layer)

	}()

	if err != C.OGRERR_NONE {
		return fmt.Errorf("GDAL擦除操作失败，错误代码: %d", int(err))
	}

	return nil
}
