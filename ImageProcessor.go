package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// 用于生成唯一ID
var (
	tileCounter uint64
	gdalMutex   sync.Mutex // GDAL 全局锁，防止并发问题
)

// ImageProcessor GDAL图像处理器
type ImageProcessor struct {
	canvasDS    C.GDALDatasetH
	width       int
	height      int
	bands       int
	tileImages  []C.GDALDatasetH
	vsimemPaths []string
	memBuffers  []unsafe.Pointer // 保存所有分配的内存
	mu          sync.Mutex       // 实例级别锁
	closed      bool
}

func NewImageProcessor(width, height, bands int) (*ImageProcessor, error) {
	if width <= 0 || height <= 0 || bands <= 0 {
		return nil, errors.New("invalid dimensions")
	}
	// 限制最大尺寸，防止内存溢出
	if width > 8192 || height > 8192 {
		return nil, errors.New("dimensions too large, max 8192x8192")
	}
	InitializeGDAL()
	gdalMutex.Lock()
	canvasDS := C.createBlankMemDataset(C.int(width), C.int(height), C.int(bands))
	gdalMutex.Unlock()
	if canvasDS == nil {
		return nil, errors.New("failed to create canvas dataset")
	}
	p := &ImageProcessor{
		canvasDS:    canvasDS,
		width:       width,
		height:      height,
		bands:       bands,
		tileImages:  make([]C.GDALDatasetH, 0, 16),
		vsimemPaths: make([]string, 0, 16),
		memBuffers:  make([]unsafe.Pointer, 0, 16),
	}
	// 设置 finalizer 作为安全网
	runtime.SetFinalizer(p, func(proc *ImageProcessor) {
		proc.Close()
	})
	return p, nil
}

// NewImageProcessorRGBA 创建RGBA图像处理器（4通道）
func NewImageProcessorRGBA(width, height int) (*ImageProcessor, error) {
	return NewImageProcessor(width, height, 4)
}

// NewImageProcessorRGB 创建RGB图像处理器（3通道）
func NewImageProcessorRGB(width, height int) (*ImageProcessor, error) {
	return NewImageProcessor(width, height, 3)
}

func (p *ImageProcessor) AddTileFromBuffer(data []byte, format string, dstX, dstY int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("processor is closed")
	}
	if len(data) == 0 {
		return errors.New("empty tile data")
	}
	if p.canvasDS == nil {
		return errors.New("canvas dataset is nil")
	}
	// 验证目标坐标
	if dstX < 0 || dstY < 0 || dstX >= p.width || dstY >= p.height {
		return fmt.Errorf("invalid destination coordinates: (%d, %d)", dstX, dstY)
	}
	tileID := atomic.AddUint64(&tileCounter, 1)
	vsimemPath := fmt.Sprintf("/vsimem/tile_%d_%d.%s", tileID, time.Now().UnixNano(), format)
	// 分配 C 内存并复制数据（关键修复：不使用 defer 释放）
	cData := C.malloc(C.size_t(len(data)))
	if cData == nil {
		return errors.New("failed to allocate memory")
	}

	// 复制数据到 C 内存
	C.memcpy(cData, unsafe.Pointer(&data[0]), C.size_t(len(data)))

	// 保存指针，稍后在 Close 时释放
	p.memBuffers = append(p.memBuffers, cData)
	cVsimemPath := C.CString(vsimemPath)
	defer C.free(unsafe.Pointer(cVsimemPath))
	gdalMutex.Lock()
	// 创建 vsimem 文件（设置 bTakeOwnership 为 FALSE，我们自己管理内存）
	fp := C.VSIFileFromMemBuffer(cVsimemPath, (*C.GByte)(cData), C.vsi_l_offset(len(data)), C.FALSE)
	if fp == nil {
		gdalMutex.Unlock()
		return errors.New("failed to create vsimem file")
	}
	C.VSIFCloseL(fp)
	p.vsimemPaths = append(p.vsimemPaths, vsimemPath)
	// 打开数据集
	hDS := C.GDALOpen(cVsimemPath, C.GA_ReadOnly)
	gdalMutex.Unlock()
	if hDS == nil {
		return fmt.Errorf("failed to open tile, format: %s", format)
	}
	p.tileImages = append(p.tileImages, hDS)
	tileWidth := C.GDALGetRasterXSize(hDS)
	tileHeight := C.GDALGetRasterYSize(hDS)
	// 验证瓦片尺寸
	if tileWidth <= 0 || tileHeight <= 0 {
		return errors.New("invalid tile dimensions")
	}
	gdalMutex.Lock()
	result := C.copyTileToCanvas(hDS, p.canvasDS,
		0, 0, tileWidth, tileHeight,
		C.int(dstX), C.int(dstY))
	gdalMutex.Unlock()
	if result != 0 {
		return fmt.Errorf("failed to copy tile: error code %d", result)
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

func (p *ImageProcessor) CropAndExport(cropX, cropY, cropWidth, cropHeight int, format string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("processor is closed")
	}
	if p.canvasDS == nil {
		return nil, errors.New("canvas dataset is nil")
	}
	// 参数验证
	if cropWidth <= 0 || cropHeight <= 0 {
		return nil, errors.New("invalid crop dimensions")
	}
	if cropX < 0 || cropY < 0 {
		return nil, errors.New("invalid crop position")
	}
	if cropX+cropWidth > p.width || cropY+cropHeight > p.height {
		return nil, fmt.Errorf("crop area exceeds canvas: crop(%d,%d,%d,%d) canvas(%d,%d)",
			cropX, cropY, cropWidth, cropHeight, p.width, p.height)
	}
	var outData *C.uchar
	var outLen C.int
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))
	gdalMutex.Lock()
	result := C.cropAndExport(p.canvasDS,
		C.int(cropX), C.int(cropY), C.int(cropWidth), C.int(cropHeight),
		cFormat, &outData, &outLen)
	gdalMutex.Unlock()
	if result != 0 {
		return nil, fmt.Errorf("crop and export failed: error code %d", result)
	}
	if outData == nil || outLen <= 0 {
		return nil, errors.New("export returned empty data")
	}
	// 复制数据到 Go 切片
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

// CropScaleAndExport 裁剪、缩放并导出
func (p *ImageProcessor) CropScaleAndExport(
	cropX, cropY, cropWidth, cropHeight int,
	outputWidth, outputHeight int,
	format string,
) ([]byte, error) {
	if p.canvasDS == nil {
		return nil, errors.New("canvas dataset is nil")
	}

	// 参数验证
	if cropWidth <= 0 || cropHeight <= 0 || outputWidth <= 0 || outputHeight <= 0 {
		return nil, errors.New("invalid dimensions")
	}

	var outData *C.uchar
	var outLen C.int

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	// 调用带缩放的裁剪导出函数
	result := C.cropScaleAndExport(p.canvasDS,
		C.int(cropX), C.int(cropY), C.int(cropWidth), C.int(cropHeight),
		C.int(outputWidth), C.int(outputHeight),
		cFormat, &outData, &outLen)

	if result != 0 {
		return nil, fmt.Errorf("failed to crop, scale and export: error code %d", result)
	}

	if outData == nil || outLen <= 0 {
		return nil, errors.New("export returned empty data")
	}

	// 复制数据到Go切片
	data := C.GoBytes(unsafe.Pointer(outData), outLen)
	C.free(unsafe.Pointer(outData))

	return data, nil
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	gdalMutex.Lock()
	defer gdalMutex.Unlock()
	// 1. 先关闭所有瓦片数据集
	for _, ds := range p.tileImages {
		if ds != nil {
			C.closeDataset(ds)
		}
	}
	p.tileImages = nil
	// 2. 清理 vsimem 文件
	for _, path := range p.vsimemPaths {
		cPath := C.CString(path)
		C.VSIUnlink(cPath)
		C.free(unsafe.Pointer(cPath))
	}
	p.vsimemPaths = nil
	// 3. 释放所有分配的 C 内存
	for _, buf := range p.memBuffers {
		if buf != nil {
			C.free(buf)
		}
	}
	p.memBuffers = nil
	// 4. 最后关闭画布
	if p.canvasDS != nil {
		C.closeDataset(p.canvasDS)
		p.canvasDS = nil
	}
	// 移除 finalizer
	runtime.SetFinalizer(p, nil)
}
