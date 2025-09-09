/*
Copyright (C) 2025 [fmecool]

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
static OGRErr performIdentityWithProgress(OGRLayerH inputLayer,
                                     OGRLayerH methodLayer,
                                     OGRLayerH resultLayer,
                                     char **options,
                                     void *progressData) {
    return OGR_L_Identity(inputLayer, methodLayer, resultLayer, options,
                         progressCallback, progressData);
}
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"log"
	"time"
	"github.com/google/uuid"
	"unsafe"
)


func SpatialIdentityAnalysis(inputLayer, methodLayer *GDALLayer, config *ParallelGeosConfig) (*GeosAnalysisResult, error) {

	defer inputLayer.Close()

	defer methodLayer.Close()

	// 为两个图层添加唯一标识字段
	err := addIdentifierField(inputLayer,"gogeo_analysis_id")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}
	err = addIdentifierField(methodLayer,"gogeo_analysis_id2")
	if err != nil {
		return nil, fmt.Errorf("添加唯一标识字段失败: %v", err)
	}

	resultLayer, err := performTileIdentityAnalysis(inputLayer, methodLayer, config,1)
	if err != nil {
		return nil, fmt.Errorf("执行瓦片裁剪Identity分析失败: %v", err)
	}

	// 计算结果数量
	resultCount := resultLayer.GetFeatureCount()


	if config.IsMergeTile {
		// 执行按标识字段的融合操作
		unionResult, err := performUnionByFields(resultLayer, config.PrecisionConfig, config.ProgressCallback)
		if err != nil {
			return nil, fmt.Errorf("执行融合操作失败: %v", err)
		}

		// 删除临时的_identityID字段
		err = deleteFieldFromLayerFuzzy(unionResult.OutputLayer, "gogeo_analysis_id")
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



// performTileClipIdentityAnalysis 执行基于瓦片裁剪的并行Identity分析
func performTileIdentityAnalysis(inputLayer, methodLayer *GDALLayer,  config *ParallelGeosConfig,strategy FieldMergeStrategy) (*GDALLayer, error) {
	if config.PrecisionConfig != nil  {
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
		if  config.PrecisionConfig.Enabled {
			// 在内存图层上设置精度
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
	resultLayer, err := createIdentityAnalysisResultLayer(inputLayer, methodLayer, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}
	taskid := uuid.New().String()
	//对A B图层进行分块,并创建bin文件
	GenerateTiles(inputLayer,methodLayer,config.TileCount,taskid)
	//读取文件列表，并发执行擦除操作
	GPbins ,err:= ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("提取分组文件失败: %v", err)
	}
	// 并发执行分析
	err = executeConcurrentIdentityAnalysis(GPbins, resultLayer, config, strategy)
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
func executeConcurrentIdentityAnalysis(tileGroups []GroupTileFiles, resultLayer *GDALLayer, config *ParallelGeosConfig, strategy FieldMergeStrategy) error {
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
		go worker_identity(i, taskQueue, results, config, &wg, strategy)
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

func worker_identity(workerID int, taskQueue <-chan GroupTileFiles, results chan<- taskResult, config *ParallelGeosConfig, wg *sync.WaitGroup , strategy FieldMergeStrategy) {
	defer wg.Done()

	tasksProcessed := 0


	for tileGroup := range taskQueue {

		start := time.Now()

		// 处理单个分块
		layer, err := processTileGroupforIdentity(tileGroup, config ,strategy)

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
func processTileGroupforIdentity(tileGroup GroupTileFiles, config *ParallelGeosConfig ,  strategy FieldMergeStrategy) (*GDALLayer, error) {

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
	tileResultLayer, err := createIdentityTileResultLayer(inputTileLayer, methodTileLayer,tileName,strategy)
	if err != nil {
		return nil, fmt.Errorf("创建分块结果图层失败: %v", err)
	}

	// 执行裁剪分析 - 不使用进度回调
	err = executeIdentidyAnalysis(inputTileLayer, methodTileLayer, tileResultLayer, nil ,strategy)
	if err != nil {
		tileResultLayer.Close()
		return nil, fmt.Errorf("执行擦除分析失败: %v", err)
	}
	return tileResultLayer, nil
}

// executeIdentityAnalysis 执行Identity分析
func executeIdentidyAnalysis(inputLayer, methodLayer, resultLayer *GDALLayer, progressCallback ProgressCallback ,strategy FieldMergeStrategy) error {
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
	switch strategy {
	case MergeWithPrefix:
		inputPrefixOpt := C.CString(fmt.Sprintf("INPUT_PREFIX="))
		methodPrefixOpt := C.CString(fmt.Sprintf("METHOD_PREFIX=l2_"))
		defer C.free(unsafe.Pointer(methodPrefixOpt))

		defer C.free(unsafe.Pointer(inputPrefixOpt))
		options = C.CSLAddString(options, methodPrefixOpt)
		options = C.CSLAddString(options, methodPrefixOpt)
		}

	// 执行Identity操作
	return executeGDALIdentityWithProgress(inputLayer, methodLayer, resultLayer, options, progressCallback)
}
// executeGDALIdentityWithProgress 执行带进度的GDAL Identity操作
func executeGDALIdentityWithProgress(inputLayer, methodLayer, resultLayer *GDALLayer, options **C.char, progressCallback ProgressCallback) error {
	var progressData *ProgressData
	var progressArg unsafe.Pointer
	// 启用多线程处理
	C.CPLSetConfigOption(C.CString("GDAL_NUM_THREADS"), C.CString("ALL_CPUS"))
	defer C.CPLSetConfigOption(C.CString("GDAL_NUM_THREADS"), nil)
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

	// 调用GDAL的Identity函数
	var err C.OGRErr
	if progressCallback != nil {
		err = C.performIdentityWithProgress(inputLayer.layer, methodLayer.layer, resultLayer.layer, options, progressArg)
	} else {
		err = C.OGR_L_Identity(inputLayer.layer, methodLayer.layer, resultLayer.layer, options, nil, nil)
	}

	if err != C.OGRERR_NONE {

		return fmt.Errorf("GDAL相交分析失败，错误代码: %d", int(err))
	}

	return nil
}

// createIdentityAnalysisResultLayer 创建Identity结果图层
func createIdentityAnalysisResultLayer(inputLayer, methodLayer *GDALLayer, strategy FieldMergeStrategy) (*GDALLayer, error) {
	layerName := C.CString("identity_result")
	defer C.free(unsafe.Pointer(layerName))

	// 获取空间参考系统
	srs := inputLayer.GetSpatialRef()
	if srs == nil {
		srs = methodLayer.GetSpatialRef()
	}

	// 创建结果图层
	resultLayerPtr := C.createMemoryLayer(layerName, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 添加字段定义
	err := addIdentityFields(resultLayer, inputLayer, methodLayer,strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}



// addIdentityFields 添加Identity分析的字段
func addIdentityFields(resultLayer, inputLayer, methodLayer *GDALLayer, strategy FieldMergeStrategy) error {
	switch strategy {
	case MergePreferTable2:
		// 添加输入图层的字段（不使用前缀）
		err1 := addLayerFields(resultLayer, inputLayer, "")
		if err1 != nil {
			return fmt.Errorf("添加输入图层字段失败: %v", err1)
		}

		// 添加方法图层的字段（不使用前缀，处理重名字段）
		err2 := addLayerFields(resultLayer, methodLayer, "")
		if err2 != nil {
			return fmt.Errorf("添加方法图层字段失败: %v", err2)
		}
	case MergeWithPrefix:
		err1 := addLayerFields(resultLayer, inputLayer, "")
		if err1 != nil {
			return fmt.Errorf("添加输入图层字段失败: %v", err1)
		}

		// 添加方法图层的字段（不使用前缀，处理重名字段）
		err2 := addLayerFields(resultLayer, methodLayer, "l2_")
		if err2 != nil {
			return fmt.Errorf("添加方法图层字段失败: %v", err2)
		}
	default:
		err1 := addLayerFields(resultLayer, inputLayer, "")
		if err1 != nil {
			return fmt.Errorf("添加输入图层字段失败: %v", err1)
		}

		// 添加方法图层的字段（不使用前缀，处理重名字段）
		err2 := addLayerFields(resultLayer, methodLayer, "")
		if err2 != nil {
			return fmt.Errorf("添加方法图层字段失败: %v", err2)
		}
		}

	return nil
}



func createIdentityTileResultLayer(inputLayer,methodLayer *GDALLayer, layerName string  ,strategy FieldMergeStrategy) (*GDALLayer, error) {
	layerNameC := C.CString(layerName)
	defer C.free(unsafe.Pointer(layerNameC))

	// 获取空间参考系统
	srs := inputLayer.GetSpatialRef()

	// 创建内存图层
	resultLayerPtr := C.createMemoryLayer(layerNameC, C.wkbMultiPolygon, srs)
	if resultLayerPtr == nil {
		return nil, fmt.Errorf("创建分块结果图层失败")
	}

	resultLayer := &GDALLayer{layer: resultLayerPtr}
	runtime.SetFinalizer(resultLayer, (*GDALLayer).cleanup)

	// 添加字段定义
	err := addIdentityFields(resultLayer, inputLayer, methodLayer ,strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("添加字段失败: %v", err)
	}

	return resultLayer, nil
}
