package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// 用于生成唯一ID
var tileCounter uint64

// ImageProcessor GDAL图像处理器
type ImageProcessor struct {
	canvasDS    C.GDALDatasetH
	width       int
	height      int
	bands       int
	tileImages  []C.GDALDatasetH
	vsimemPaths []string
	tileBuffers []*C.char // 保存C字符串指针，防止被GC
}

// NewImageProcessor 创建图像处理器
func NewImageProcessor(width, height, bands int) (*ImageProcessor, error) {
	if width <= 0 || height <= 0 || bands <= 0 {
		return nil, errors.New("invalid dimensions: width, height and bands must be positive")
	}

	InitializeGDAL()

	canvasDS := C.createBlankMemDataset(C.int(width), C.int(height), C.int(bands))
	if canvasDS == nil {
		return nil, errors.New("failed to create canvas dataset")
	}

	return &ImageProcessor{
		canvasDS:    canvasDS,
		width:       width,
		height:      height,
		bands:       bands,
		tileImages:  make([]C.GDALDatasetH, 0),
		vsimemPaths: make([]string, 0),
		tileBuffers: make([]*C.char, 0),
	}, nil
}

// NewImageProcessorRGBA 创建RGBA图像处理器（4通道）
func NewImageProcessorRGBA(width, height int) (*ImageProcessor, error) {
	return NewImageProcessor(width, height, 4)
}

// NewImageProcessorRGB 创建RGB图像处理器（3通道）
func NewImageProcessorRGB(width, height int) (*ImageProcessor, error) {
	return NewImageProcessor(width, height, 3)
}

// AddTileFromBuffer 从内存缓冲区添加瓦片
func (p *ImageProcessor) AddTileFromBuffer(data []byte, format string, dstX, dstY int) error {
	if len(data) == 0 {
		return errors.New("empty tile data")
	}

	if p.canvasDS == nil {
		return errors.New("canvas dataset is nil")
	}

	// 生成唯一ID
	tileID := atomic.AddUint64(&tileCounter, 1)

	// 创建vsimem路径
	vsimemPath := fmt.Sprintf("/vsimem/tile_%d_%p.%s", tileID, unsafe.Pointer(&data[0]), format)
	cVsimemPath := C.CString(vsimemPath)

	// 将数据写入vsimem
	cData := C.CBytes(data)
	defer C.free(cData)

	// 使用VSIFileFromMemBuffer创建内存文件
	fp := C.VSIFileFromMemBuffer(cVsimemPath, (*C.GByte)(cData), C.vsi_l_offset(len(data)), C.FALSE)
	if fp == nil {
		C.free(unsafe.Pointer(cVsimemPath))
		return errors.New("failed to create vsimem file")
	}
	C.VSIFCloseL(fp)

	// 保存路径用于后续清理
	p.vsimemPaths = append(p.vsimemPaths, vsimemPath)

	// 打开数据集
	hDS := C.GDALOpen(cVsimemPath, C.GA_ReadOnly)
	C.free(unsafe.Pointer(cVsimemPath))

	if hDS == nil {
		return fmt.Errorf("failed to open tile from buffer, format: %s", format)
	}

	p.tileImages = append(p.tileImages, hDS)

	// 获取瓦片尺寸
	tileWidth := C.GDALGetRasterXSize(hDS)
	tileHeight := C.GDALGetRasterYSize(hDS)

	// 复制到画布
	result := C.copyTileToCanvas(hDS, p.canvasDS,
		0, 0, tileWidth, tileHeight,
		C.int(dstX), C.int(dstY))

	if result != 0 {
		return fmt.Errorf("failed to copy tile to canvas: error code %d", result)
	}

	return nil
}

// AddTileFromBufferWithSize 从内存缓冲区添加瓦片，支持指定源区域
func (p *ImageProcessor) AddTileFromBufferWithSize(data []byte, format string,
	srcX, srcY, srcWidth, srcHeight int,
	dstX, dstY int) error {

	if len(data) == 0 {
		return errors.New("empty tile data")
	}

	if p.canvasDS == nil {
		return errors.New("canvas dataset is nil")
	}

	// 生成唯一ID
	tileID := atomic.AddUint64(&tileCounter, 1)

	// 创建vsimem路径
	vsimemPath := fmt.Sprintf("/vsimem/tile_%d.%s", tileID, format)
	cVsimemPath := C.CString(vsimemPath)

	// 将数据写入vsimem
	cData := C.CBytes(data)
	defer C.free(cData)

	fp := C.VSIFileFromMemBuffer(cVsimemPath, (*C.GByte)(cData), C.vsi_l_offset(len(data)), C.FALSE)
	if fp == nil {
		C.free(unsafe.Pointer(cVsimemPath))
		return errors.New("failed to create vsimem file")
	}
	C.VSIFCloseL(fp)

	p.vsimemPaths = append(p.vsimemPaths, vsimemPath)

	// 打开数据集
	hDS := C.GDALOpen(cVsimemPath, C.GA_ReadOnly)
	C.free(unsafe.Pointer(cVsimemPath))

	if hDS == nil {
		return fmt.Errorf("failed to open tile from buffer, format: %s", format)
	}

	p.tileImages = append(p.tileImages, hDS)

	// 复制到画布
	result := C.copyTileToCanvas(hDS, p.canvasDS,
		C.int(srcX), C.int(srcY), C.int(srcWidth), C.int(srcHeight),
		C.int(dstX), C.int(dstY))

	if result != 0 {
		return fmt.Errorf("failed to copy tile to canvas: error code %d", result)
	}

	return nil
}

// CropAndExport 裁剪并导出
func (p *ImageProcessor) CropAndExport(cropX, cropY, cropWidth, cropHeight int, format string) ([]byte, error) {
	if p.canvasDS == nil {
		return nil, errors.New("canvas dataset is nil")
	}

	// 参数验证
	if cropWidth <= 0 || cropHeight <= 0 {
		return nil, errors.New("invalid crop dimensions")
	}

	var outData *C.uchar
	var outLen C.int

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	result := C.cropAndExport(p.canvasDS,
		C.int(cropX), C.int(cropY), C.int(cropWidth), C.int(cropHeight),
		cFormat, &outData, &outLen)

	if result != 0 {
		return nil, fmt.Errorf("failed to crop and export: error code %d", result)
	}

	if outData == nil || outLen <= 0 {
		return nil, errors.New("export returned empty data")
	}

	// 复制数据到Go切片
	data := C.GoBytes(unsafe.Pointer(outData), outLen)
	C.free(unsafe.Pointer(outData))

	return data, nil
}

// Export 导出整个画布
func (p *ImageProcessor) Export(format string) ([]byte, error) {
	return p.CropAndExport(0, 0, p.width, p.height, format)
}

// ExportToFile 导出到文件
func (p *ImageProcessor) ExportToFile(filename string, quality int) error {
	if p.canvasDS == nil {
		return errors.New("canvas dataset is nil")
	}

	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	result := C.writeJPEG(p.canvasDS, cFilename, C.int(quality))
	if result == 0 {
		return errors.New("failed to write image to file")
	}

	return nil
}

// GetDimensions 获取画布尺寸
func (p *ImageProcessor) GetDimensions() (width, height, bands int) {
	return p.width, p.height, p.bands
}

// Clear 清空画布（填充透明）
func (p *ImageProcessor) Clear() error {
	if p.canvasDS == nil {
		return errors.New("canvas dataset is nil")
	}

	// 重新创建画布
	C.closeDataset(p.canvasDS)

	canvasDS := C.createBlankMemDataset(C.int(p.width), C.int(p.height), C.int(p.bands))
	if canvasDS == nil {
		return errors.New("failed to recreate canvas dataset")
	}

	p.canvasDS = canvasDS
	return nil
}

// Close 关闭处理器并释放资源
func (p *ImageProcessor) Close() {
	// 关闭所有瓦片数据集
	for _, ds := range p.tileImages {
		if ds != nil {
			C.closeDataset(ds)
		}
	}
	p.tileImages = nil

	// 清理vsimem文件
	for _, path := range p.vsimemPaths {
		cPath := C.CString(path)
		C.cleanupVsimem(cPath)
		C.free(unsafe.Pointer(cPath))
	}
	p.vsimemPaths = nil

	// 释放保存的C字符串
	for _, buf := range p.tileBuffers {
		if buf != nil {
			C.free(unsafe.Pointer(buf))
		}
	}
	p.tileBuffers = nil

	// 关闭画布
	if p.canvasDS != nil {
		C.closeDataset(p.canvasDS)
		p.canvasDS = nil
	}
}
