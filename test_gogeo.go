/*
Copyright (C) 2024 [Your Name]

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

*/
import "C"
import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"
)

func PerformSpatialIntersectionTest(shpFile1, shpFile2, outputFile string) error {
	startTime := time.Now()

	// 1. 读取第一个shapefile
	fmt.Println("正在读取第一个shapefile...")
	reader1, err := NewFileGeoReader(shpFile1)
	if err != nil {
		return fmt.Errorf("创建第一个文件读取器失败: %v", err)
	}

	layer1, err := reader1.ReadShapeFile()
	if err != nil {
		return fmt.Errorf("读取第一个shapefile失败: %v", err)
	}

	// 打印第一个图层信息
	fmt.Println("第一个图层信息:")
	layer1.PrintLayerInfo()

	// 2. 读取第二个shapefile
	fmt.Println("\n正在读取第二个shapefile...")
	reader2, err := NewFileGeoReader(shpFile2)
	if err != nil {
		layer1.Close()
		return fmt.Errorf("创建第二个文件读取器失败: %v", err)
	}

	layer2, err := reader2.ReadShapeFile()
	if err != nil {
		layer1.Close()
		return fmt.Errorf("读取第二个shapefile失败: %v", err)
	}

	// 打印第二个图层信息
	fmt.Println("第二个图层信息:")
	layer2.PrintLayerInfo()

	// 3. 配置并行相交分析参数
	config := &ParallelGeosConfig{
		TileCount:  10, // 4x4分块
		MaxWorkers: 32, // 使用所有CPU核心
		// 分块缓冲距离
		IsMergeTile:      true,             // 合并分块结果
		ProgressCallback: progressCallback, // 进度回调函数
		PrecisionConfig: &GeometryPrecisionConfig{
			Enabled:       true,
			GridSize:      0.000000001, // 几何精度网格大小
			PreserveTopo:  true,        // 保持拓扑
			KeepCollapsed: false,       // 不保留退化几何
		},
	}

	// 5. 执行空间相交分析
	result, err := SpatialIdentityAnalysis(layer1, layer2, config)
	if err != nil {
		return fmt.Errorf("空间分析执行失败: %v", err)
	}

	analysisTime := time.Since(startTime)
	fmt.Printf("\n分析完成! 耗时: %v\n", analysisTime)
	fmt.Printf("结果要素数量: %d\n", result.ResultCount)

	// 6. 将结果写出为shapefile
	fmt.Println("正在写出结果到shapefile...")
	writeStartTime := time.Now()

	// 获取输出文件的图层名称（不含扩展名）
	layerName := getFileNameWithoutExt(outputFile)

	err = WriteShapeFileLayer(result.OutputLayer, outputFile, layerName, true)
	if err != nil {
		result.OutputLayer.Close()
		return fmt.Errorf("写出shapefile失败: %v", err)
	}

	writeTime := time.Since(writeStartTime)
	totalTime := time.Since(startTime)

	fmt.Printf("结果写出完成! 耗时: %v\n", writeTime)
	fmt.Printf("总耗时: %v\n", totalTime)
	fmt.Printf("输出文件: %s\n", outputFile)

	// 7. 验证输出文件
	err = verifyOutputFile(outputFile)
	if err != nil {
		fmt.Printf("警告: 输出文件验证失败: %v\n", err)
	} else {
		fmt.Println("输出文件验证成功!")
	}

	// 清理资源
	result.OutputLayer.Close()

	return nil
}

// progressCallback 进度回调函数
func progressCallback(complete float64, message string) bool {
	// 显示进度信息
	fmt.Printf("\r进度: %.1f%% - %s", complete*100, message)

	// 如果进度完成，换行
	if complete >= 1.0 {
		fmt.Println()
	}

	// 返回true继续执行，返回false取消执行
	return true
}

// getFileNameWithoutExt 获取不含扩展名的文件名
func getFileNameWithoutExt(filePath string) string {
	fileName := filepath.Base(filePath)
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}

// verifyOutputFile 验证输出文件
func verifyOutputFile(filePath string) error {
	// 读取输出文件验证
	reader, err := NewFileGeoReader(filePath)
	if err != nil {
		return fmt.Errorf("无法读取输出文件: %v", err)
	}

	layer, err := reader.ReadShapeFile()
	if err != nil {
		return fmt.Errorf("无法读取输出图层: %v", err)
	}
	defer layer.Close()

	// 打印输出图层信息
	fmt.Println("\n输出图层信息:")
	layer.PrintLayerInfo()

	// 检查要素数量
	featureCount := layer.GetFeatureCount()
	if featureCount == 0 {
		return fmt.Errorf("输出文件中没有要素")
	}

	fmt.Printf("验证通过: 输出文件包含 %d 个要素\n", featureCount)
	return nil
}

// ReadBinFilesAndConvertToGDB 读取文件夹内所有bin文件并转换为GDB
func ReadBinFilesAndConvertToGDB(folderPath string, outputGDBPath string) error {
	// 检查文件夹是否存在
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		return fmt.Errorf("文件夹不存在: %s", folderPath)
	}

	// 读取文件夹内容
	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("无法读取文件夹 %s: %v", folderPath, err)
	}

	// 过滤出所有.bin文件
	var binFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.ToLower(filepath.Ext(file.Name())) == ".bin" {
			binFiles = append(binFiles, filepath.Join(folderPath, file.Name()))
		}
	}

	if len(binFiles) == 0 {
		return fmt.Errorf("文件夹 %s 中没有找到.bin文件", folderPath)
	}

	fmt.Printf("找到 %d 个.bin文件\n", len(binFiles))

	// 创建GDB写入器
	NewFileGeoWriter(outputGDBPath, true) // true表示覆盖已存在的文件
	if err != nil {
		return fmt.Errorf("无法创建GDB写入器: %v", err)
	}

	// 初始化GDAL
	InitializeGDAL()

	// 获取FileGDB驱动
	driver := C.OGRGetDriverByName(C.CString("FileGDB"))
	if driver == nil {
		// 如果FileGDB驱动不可用，尝试OpenFileGDB驱动
		driver = C.OGRGetDriverByName(C.CString("OpenFileGDB"))
		if driver == nil {
			return fmt.Errorf("无法获取GDB驱动（需要FileGDB或OpenFileGDB驱动）")
		}
	}

	// 如果GDB已存在且需要覆盖，先删除
	if _, err := os.Stat(outputGDBPath); err == nil {
		os.RemoveAll(outputGDBPath)
	}

	// 创建GDB数据源
	cGDBPath := C.CString(outputGDBPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cGDBPath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建GDB文件: %s", outputGDBPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 处理每个bin文件
	successCount := 0
	errorCount := 0

	for i, binFile := range binFiles {
		fmt.Printf("处理文件 %d/%d: %s\n", i+1, len(binFiles), filepath.Base(binFile))

		// 反序列化图层
		layer, err := DeserializeLayerFromFile(binFile)
		if err != nil {
			fmt.Printf("  错误: 无法反序列化文件 %s: %v\n", binFile, err)
			errorCount++
			continue
		}

		// 生成图层名称（使用文件名，去掉扩展名）
		layerName := strings.TrimSuffix(filepath.Base(binFile), ".bin")
		layerName = sanitizeLayerName(layerName)

		// 将图层写入GDB
		err = writeLayerToDataset(layer, dataset, layerName)
		if err != nil {
			fmt.Printf("  错误: 无法写入图层 %s: %v\n", layerName, err)
			errorCount++
		} else {
			fmt.Printf("  成功: 图层 %s 已写入GDB\n", layerName)
			successCount++
		}

		// 关闭图层以释放资源
		layer.Close()
	}

	fmt.Printf("\n转换完成:\n")
	fmt.Printf("  成功: %d 个图层\n", successCount)
	fmt.Printf("  失败: %d 个图层\n", errorCount)
	fmt.Printf("  输出GDB: %s\n", outputGDBPath)

	if successCount == 0 {
		return fmt.Errorf("没有成功转换任何图层")
	}

	return nil
}

// writeLayerToDataset 将GDALLayer写入到指定的数据集
func writeLayerToDataset(sourceLayer *GDALLayer, dataset C.OGRDataSourceH, layerName string) error {
	// 获取源图层信息
	sourceDefn := sourceLayer.GetLayerDefn()
	geomType := C.OGR_FD_GetGeomType(sourceDefn)
	srs := sourceLayer.GetSpatialRef()

	// 创建图层
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	newLayer := C.OGR_DS_CreateLayer(dataset, cLayerName, srs, geomType, nil)
	if newLayer == nil {
		return fmt.Errorf("无法创建图层: %s", layerName)
	}

	// 创建临时写入器用于复制字段和要素
	tempWriter := &FileGeoWriter{
		FileType: "gdb",
	}

	// 复制字段定义
	err := tempWriter.copyFieldDefinitions(sourceDefn, newLayer)
	if err != nil {
		return fmt.Errorf("复制字段定义失败: %v", err)
	}

	// 复制要素
	err = tempWriter.copyFeatures(sourceLayer, newLayer)
	if err != nil {
		return fmt.Errorf("复制要素失败: %v", err)
	}

	return nil
}

// sanitizeLayerName 清理图层名称以符合GDB要求
func sanitizeLayerName(name string) string {
	// 移除特殊字符，替换为下划线
	sanitized := strings.ReplaceAll(name, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, "(", "_")
	sanitized = strings.ReplaceAll(sanitized, ")", "_")

	// 确保图层名不以数字开头
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "layer_" + sanitized
	}

	// 确保图层名不为空
	if len(sanitized) == 0 {
		sanitized = "unknown_layer"
	}

	return sanitized
}

// ProcessBinFolderToGDB 处理bin文件夹并转换为GDB的便捷函数
func ProcessBinFolderToGDB(binFolderPath string, outputGDBPath string) error {
	return ReadBinFilesAndConvertToGDB(binFolderPath, outputGDBPath)
}

// BatchConvertBinToGDB 批量转换bin文件到GDB（支持子文件夹）
func BatchConvertBinToGDB(rootFolderPath string, outputGDBPath string, includeSubfolders bool) error {
	var allBinFiles []string

	// 遍历文件夹
	err := filepath.Walk(rootFolderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果不包含子文件夹，跳过子目录
		if !includeSubfolders && info.IsDir() && path != rootFolderPath {
			return filepath.SkipDir
		}

		// 检查是否为bin文件
		if !info.IsDir() && strings.ToLower(filepath.Ext(info.Name())) == ".bin" {
			allBinFiles = append(allBinFiles, path)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("遍历文件夹失败: %v", err)
	}

	if len(allBinFiles) == 0 {
		return fmt.Errorf("没有找到.bin文件")
	}

	fmt.Printf("找到 %d 个.bin文件\n", len(allBinFiles))

	// 创建GDB写入器
	NewFileGeoWriter(outputGDBPath, true)
	if err != nil {
		return fmt.Errorf("无法创建GDB写入器: %v", err)
	}

	// 初始化GDAL
	InitializeGDAL()

	// 获取FileGDB驱动
	driver := C.OGRGetDriverByName(C.CString("FileGDB"))
	if driver == nil {
		driver = C.OGRGetDriverByName(C.CString("OpenFileGDB"))
		if driver == nil {
			return fmt.Errorf("无法获取GDB驱动")
		}
	}

	// 删除已存在的GDB
	if _, err := os.Stat(outputGDBPath); err == nil {
		os.RemoveAll(outputGDBPath)
	}

	// 创建GDB数据源
	cGDBPath := C.CString(outputGDBPath)
	defer C.free(unsafe.Pointer(cGDBPath))

	dataset := C.OGR_Dr_CreateDataSource(driver, cGDBPath, nil)
	if dataset == nil {
		return fmt.Errorf("无法创建GDB文件: %s", outputGDBPath)
	}
	defer C.OGR_DS_Destroy(dataset)

	// 处理每个bin文件
	successCount := 0
	errorCount := 0

	for i, binFile := range allBinFiles {
		fmt.Printf("处理文件 %d/%d: %s\n", i+1, len(allBinFiles), binFile)

		// 反序列化图层
		layer, err := DeserializeLayerFromFile(binFile)
		if err != nil {
			fmt.Printf("  错误: %v\n", err)
			errorCount++
			continue
		}

		// 生成唯一的图层名称
		relPath, _ := filepath.Rel(rootFolderPath, binFile)
		layerName := generateUniqueLayerName(relPath)

		// 写入图层
		err = writeLayerToDataset(layer, dataset, layerName)
		if err != nil {
			fmt.Printf("  错误: %v\n", err)
			errorCount++
		} else {
			fmt.Printf("  成功: %s\n", layerName)
			successCount++
		}

		layer.Close()
	}

	fmt.Printf("\n批量转换完成:\n")
	fmt.Printf("  成功: %d 个图层\n", successCount)
	fmt.Printf("  失败: %d 个图层\n", errorCount)

	return nil
}

// generateUniqueLayerName 生成唯一的图层名称
func generateUniqueLayerName(filePath string) string {
	// 将路径分隔符替换为下划线
	name := strings.ReplaceAll(filePath, string(filepath.Separator), "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")

	// 移除.bin扩展名
	name = strings.TrimSuffix(name, ".bin")

	// 清理名称
	return sanitizeLayerName(name)
}
