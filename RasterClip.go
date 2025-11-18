package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

// ClipOptions 裁剪选项
type ClipOptions struct {
	OutputDir         string  // 输出目录
	NameField         string  // 名称字段（默认 "NAME"）
	JPEGQuality       int     // JPEG质量 (1-100，默认85)
	TileSize          int     // 输出瓦片大小（像素，0表示原始分辨率）
	BufferDist        float64 // 缓冲距离（单位：米，0表示不缓冲）
	OverwriteExisting bool    // 是否覆盖已存在的文件
	ImageFormat       string
}

// ClipResult 裁剪结果
type ClipResult struct {
	Name       string
	OutputPath string
	Bounds     [4]float64 // minX, minY, maxX, maxY
	Width      int
	Height     int
	Error      error
}

func (rd *RasterDataset) GetActiveDataset() C.GDALDatasetH {
	if rd.warpedDS != nil {
		return rd.warpedDS
	}
	return rd.dataset
}

// ClipRasterByLayer 使用矢量图层裁剪栅格数据
func (rd *RasterDataset) ClipRasterByLayer(layer *GDALLayer, options *ClipOptions) ([]ClipResult, error) {
	if layer == nil || layer.layer == nil {
		return nil, fmt.Errorf("invalid layer")
	}

	// 设置默认选项
	if options == nil {
		options = &ClipOptions{}
	}
	if options.NameField == "" {
		options.NameField = "NAME"
	}
	if options.JPEGQuality <= 0 || options.JPEGQuality > 100 {
		options.JPEGQuality = 85
	}
	if options.OutputDir == "" {
		options.OutputDir = "./output"
	}

	// 创建输出目录
	if err := os.MkdirAll(options.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// ✅ 获取正确的数据集
	activeDS := rd.GetActiveDataset()

	// 重置图层读取
	C.OGR_L_ResetReading(layer.layer)

	// 获取要素数量
	featureCount := int(C.OGR_L_GetFeatureCount(layer.layer, 1))
	results := make([]ClipResult, 0, featureCount)

	// 获取名称字段索引
	cFieldName := C.CString(options.NameField)
	defer C.free(unsafe.Pointer(cFieldName))

	// 遍历所有要素
	for {
		feature := C.OGR_L_GetNextFeature(layer.layer)
		if feature == nil {
			break
		}

		// ✅ 传入正确的数据集
		result := rd.clipByFeature(feature, activeDS, options, cFieldName)
		results = append(results, result)

		C.OGR_F_Destroy(feature)
	}

	return results, nil
}

// clipByFeature 裁剪单个要素
func (rd *RasterDataset) clipByFeature(
	feature C.OGRFeatureH,
	srcDS C.GDALDatasetH, // ✅ 新增参数
	options *ClipOptions,
	cFieldName *C.char,
) ClipResult {
	result := ClipResult{}

	// 获取名称字段
	fieldIndex := C.OGR_F_GetFieldIndex(feature, cFieldName)
	if fieldIndex < 0 {
		result.Error = fmt.Errorf("field '%s' not found", options.NameField)
		return result
	}

	namePtr := C.OGR_F_GetFieldAsString(feature, fieldIndex)
	if namePtr == nil {
		result.Error = fmt.Errorf("failed to get field value")
		return result
	}
	result.Name = C.GoString(namePtr)

	// 清理文件名
	result.Name = sanitizeFilename(result.Name)
	if result.Name == "" {
		result.Name = fmt.Sprintf("feature_%d", C.OGR_F_GetFID(feature))
	}

	// 构建输出路径
	result.OutputPath = filepath.Join(options.OutputDir, result.Name+"."+options.ImageFormat)

	// 检查文件是否已存在
	if !options.OverwriteExisting {
		if _, err := os.Stat(result.OutputPath); err == nil {
			result.Error = fmt.Errorf("file already exists: %s", result.OutputPath)
			return result
		}
	}

	// 获取几何体
	geom := C.OGR_F_GetGeometryRef(feature)
	if geom == nil {
		result.Error = fmt.Errorf("feature has no geometry")
		return result
	}

	// 应用缓冲区（如果需要）
	if options.BufferDist > 0 {
		geom = C.OGR_G_Buffer(geom, C.double(options.BufferDist), 30)
		defer C.OGR_G_DestroyGeometry(geom)
	}

	// ✅ 使用传入的数据集进行裁剪
	var bounds [4]C.double
	clippedDS := C.clipRasterByGeometry(srcDS, geom, &bounds[0])
	if clippedDS == nil {
		result.Error = fmt.Errorf("failed to clip raster")
		return result
	}
	defer C.GDALClose(clippedDS)

	// 保存边界信息
	result.Bounds = [4]float64{
		float64(bounds[0]),
		float64(bounds[1]),
		float64(bounds[2]),
		float64(bounds[3]),
	}

	// 获取裁剪后的尺寸
	result.Width = int(C.GDALGetRasterXSize(clippedDS))
	result.Height = int(C.GDALGetRasterYSize(clippedDS))

	// 如果需要调整大小
	var outputDS C.GDALDatasetH = clippedDS
	if options.TileSize > 0 {
		outputDS = rd.resizeDataset(clippedDS, options.TileSize)
		if outputDS == nil {
			result.Error = fmt.Errorf("failed to resize dataset")
			return result
		}
		defer C.GDALClose(outputDS)
		result.Width = options.TileSize
		result.Height = options.TileSize
	}

	// 写入JPEG
	cOutputPath := C.CString(result.OutputPath)
	defer C.free(unsafe.Pointer(cOutputPath))

	success := C.writeJPEG(outputDS, cOutputPath, C.int(options.JPEGQuality))
	if success == 0 {
		result.Error = fmt.Errorf("failed to write JPEG: %s", result.OutputPath)
		return result
	}

	return result
}

// resizeDataset 调整数据集大小
func (rd *RasterDataset) resizeDataset(srcDS C.GDALDatasetH, size int) C.GDALDatasetH {
	driver := C.GDALGetDriverByName(C.CString("MEM"))
	if driver == nil {
		return nil
	}

	bandCount := int(C.GDALGetRasterCount(srcDS))

	// 创建内存数据集
	dstDS := C.GDALCreate(driver, C.CString(""), C.int(size), C.int(size),
		C.int(bandCount), C.GDT_Byte, nil)
	if dstDS == nil {
		return nil
	}

	// 复制地理变换和投影
	var geoTransform [6]C.double
	if C.GDALGetGeoTransform(srcDS, &geoTransform[0]) == C.CE_None {
		C.GDALSetGeoTransform(dstDS, &geoTransform[0])
	}

	projection := C.GDALGetProjectionRef(srcDS)
	if projection != nil {
		C.GDALSetProjection(dstDS, projection)
	}

	// 重采样
	C.GDALReprojectImage(srcDS, nil, dstDS, nil, C.GRA_Bilinear, 0, 0, nil, nil, nil)

	return dstDS
}

// sanitizeFilename 清理文件名
func sanitizeFilename(name string) string {
	// 移除或替换非法字符
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
		" ", "_",
	)
	name = replacer.Replace(name)

	// 移除开头和结尾的点和空格
	name = strings.Trim(name, ". ")

	return name
}

// ClipResultByte 裁剪结果（二进制数据版本）
type ClipResultByte struct {
	Name      string
	ImageData []byte     // 图片二进制数据
	Bounds    [4]float64 // minX, minY, maxX, maxY
	Width     int
	Height    int
	Error     error
}

// ClipRasterByLayerByte 使用矢量图层裁剪栅格数据并返回二进制数据
func (rd *RasterDataset) ClipRasterByLayerByte(layer *GDALLayer, options *ClipOptions) ([]ClipResultByte, error) {
	if layer == nil || layer.layer == nil {
		return nil, fmt.Errorf("invalid layer")
	}

	// 设置默认选项
	if options == nil {
		options = &ClipOptions{}
	}
	if options.NameField == "" {
		options.NameField = "NAME"
	}
	if options.JPEGQuality <= 0 || options.JPEGQuality > 100 {
		options.JPEGQuality = 85
	}
	if options.ImageFormat == "" {
		options.ImageFormat = "JPEG"
	}

	// 获取正确的数据集
	activeDS := rd.GetActiveDataset()

	// 重置图层读取
	C.OGR_L_ResetReading(layer.layer)

	// 获取要素数量
	featureCount := int(C.OGR_L_GetFeatureCount(layer.layer, 1))
	results := make([]ClipResultByte, 0, featureCount)

	// 获取名称字段索引
	cFieldName := C.CString(options.NameField)
	defer C.free(unsafe.Pointer(cFieldName))

	// 遍历所有要素
	for {
		feature := C.OGR_L_GetNextFeature(layer.layer)
		if feature == nil {
			break
		}

		result := rd.clipByFeatureToByte(feature, activeDS, options, cFieldName)
		results = append(results, result)

		C.OGR_F_Destroy(feature)
	}

	return results, nil
}

// clipByFeatureToByte 裁剪单个要素并返回二进制数据
func (rd *RasterDataset) clipByFeatureToByte(
	feature C.OGRFeatureH,
	srcDS C.GDALDatasetH,
	options *ClipOptions,
	cFieldName *C.char,
) ClipResultByte {
	result := ClipResultByte{}

	// 获取名称字段
	fieldIndex := C.OGR_F_GetFieldIndex(feature, cFieldName)
	if fieldIndex < 0 {
		result.Error = fmt.Errorf("field '%s' not found", options.NameField)
		return result
	}

	namePtr := C.OGR_F_GetFieldAsString(feature, fieldIndex)
	if namePtr == nil {
		result.Error = fmt.Errorf("failed to get field value")
		return result
	}
	result.Name = C.GoString(namePtr)

	// 清理文件名
	result.Name = sanitizeFilename(result.Name)
	if result.Name == "" {
		result.Name = fmt.Sprintf("feature_%d", C.OGR_F_GetFID(feature))
	}

	// 获取几何体
	geom := C.OGR_F_GetGeometryRef(feature)
	if geom == nil {
		result.Error = fmt.Errorf("feature has no geometry")
		return result
	}

	// 应用缓冲区（如果需要）
	if options.BufferDist > 0 {
		geom = C.OGR_G_Buffer(geom, C.double(options.BufferDist), 30)
		defer C.OGR_G_DestroyGeometry(geom)
	}

	// 使用传入的数据集进行裁剪
	var bounds [4]C.double
	clippedDS := C.clipRasterByGeometry(srcDS, geom, &bounds[0])
	if clippedDS == nil {
		result.Error = fmt.Errorf("failed to clip raster")
		return result
	}
	defer C.GDALClose(clippedDS)

	// 保存边界信息
	result.Bounds = [4]float64{
		float64(bounds[0]),
		float64(bounds[1]),
		float64(bounds[2]),
		float64(bounds[3]),
	}

	// 获取裁剪后的尺寸
	result.Width = int(C.GDALGetRasterXSize(clippedDS))
	result.Height = int(C.GDALGetRasterYSize(clippedDS))

	// 如果需要调整大小
	var outputDS C.GDALDatasetH = clippedDS
	if options.TileSize > 0 {
		outputDS = rd.resizeDataset(clippedDS, options.TileSize)
		if outputDS == nil {
			result.Error = fmt.Errorf("failed to resize dataset")
			return result
		}
		defer C.GDALClose(outputDS)
		result.Width = options.TileSize
		result.Height = options.TileSize
	}

	// 写入到内存缓冲区
	cFormat := C.CString(options.ImageFormat)
	defer C.free(unsafe.Pointer(cFormat))

	imageBuffer := C.writeImageToMemory(outputDS, cFormat, C.int(options.JPEGQuality))
	if imageBuffer == nil {
		result.Error = fmt.Errorf("failed to write image to memory")
		return result
	}
	defer C.freeImageBuffer(imageBuffer)

	// 将 C 数据转换为 Go []byte
	if imageBuffer.size > 0 && imageBuffer.data != nil {
		result.ImageData = C.GoBytes(unsafe.Pointer(imageBuffer.data), C.int(imageBuffer.size))
	} else {
		result.Error = fmt.Errorf("empty image data")
		return result
	}

	return result
}
