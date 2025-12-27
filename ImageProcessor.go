package Gogeo

/*
   #include "osgeo_utils.h"
   // 从内存创建数据集
   GDALDatasetH createMemDatasetFromBuffer(const char* data, int dataLen, const char* format) {
       char vsimemPath[256];
       snprintf(vsimemPath, sizeof(vsimemPath), "/vsimem/tile_%p.%s", data, format);

       VSILFILE* fp = VSIFileFromMemBuffer(vsimemPath, (GByte*)data, dataLen, FALSE);
       if (fp == NULL) {
           return NULL;
       }
       VSIFCloseL(fp);

       GDALDatasetH hDS = GDALOpen(vsimemPath, GA_ReadOnly);
       return hDS;
   }

   // 创建空白内存数据集
   GDALDatasetH createBlankMemDataset(int width, int height, int bands) {
       GDALDriverH hDriver = GDALGetDriverByName("MEM");
       if (hDriver == NULL) {
           return NULL;
       }

       GDALDatasetH hDS = GDALCreate(hDriver, "", width, height, bands, GDT_Byte, NULL);
       return hDS;
   }

   // 复制瓦片到画布
   int copyTileToCanvas(GDALDatasetH hSrcDS, GDALDatasetH hDstDS,
                        int srcX, int srcY, int srcWidth, int srcHeight,
                        int dstX, int dstY) {
       if (hSrcDS == NULL || hDstDS == NULL) {
           return -1;
       }

       int srcBands = GDALGetRasterCount(hSrcDS);
       int dstBands = GDALGetRasterCount(hDstDS);
       int bands = srcBands < dstBands ? srcBands : dstBands;

       // 分配缓冲区
       GByte* buffer = (GByte*)CPLMalloc(srcWidth * srcHeight);
       if (buffer == NULL) {
           return -2;
       }

       for (int b = 1; b <= bands; b++) {
           GDALRasterBandH hSrcBand = GDALGetRasterBand(hSrcDS, b);
           GDALRasterBandH hDstBand = GDALGetRasterBand(hDstDS, b);

           // 读取源数据
           CPLErr err = GDALRasterIO(hSrcBand, GF_Read,
                                      srcX, srcY, srcWidth, srcHeight,
                                      buffer, srcWidth, srcHeight, GDT_Byte, 0, 0);
           if (err != CE_None) {
               CPLFree(buffer);
               return -3;
           }

           // 写入目标
           err = GDALRasterIO(hDstBand, GF_Write,
                              dstX, dstY, srcWidth, srcHeight,
                              buffer, srcWidth, srcHeight, GDT_Byte, 0, 0);
           if (err != CE_None) {
               CPLFree(buffer);
               return -4;
           }
       }

       CPLFree(buffer);
       return 0;
   }

   // 裁剪并导出为指定格式
   int cropAndExport(GDALDatasetH hSrcDS, int cropX, int cropY, int cropWidth, int cropHeight,
                     const char* format, unsigned char** outData, int* outLen) {
       if (hSrcDS == NULL) {
           return -1;
       }

       int bands = GDALGetRasterCount(hSrcDS);

       // 创建输出驱动
       GDALDriverH hDriver = GDALGetDriverByName(format);
       if (hDriver == NULL) {
           return -2;
       }

       // 创建临时内存文件路径
       char vsimemPath[256];
       snprintf(vsimemPath, sizeof(vsimemPath), "/vsimem/output_%p.%s", hSrcDS,
                strcmp(format, "JPEG") == 0 ? "jpg" : "png");

       // 创建输出数据集
       char** papszOptions = NULL;
       if (strcmp(format, "JPEG") == 0) {
           papszOptions = CSLSetNameValue(papszOptions, "QUALITY", "85");
       } else if (strcmp(format, "PNG") == 0) {
           papszOptions = CSLSetNameValue(papszOptions, "ZLEVEL", "6");
       }

       // 先创建MEM数据集用于裁剪
       GDALDriverH hMemDriver = GDALGetDriverByName("MEM");
       GDALDatasetH hCropDS = GDALCreate(hMemDriver, "", cropWidth, cropHeight, bands, GDT_Byte, NULL);
       if (hCropDS == NULL) {
           CSLDestroy(papszOptions);
           return -3;
       }

       // 复制裁剪区域
       GByte* buffer = (GByte*)CPLMalloc(cropWidth * cropHeight);
       for (int b = 1; b <= bands; b++) {
           GDALRasterBandH hSrcBand = GDALGetRasterBand(hSrcDS, b);
           GDALRasterBandH hDstBand = GDALGetRasterBand(hCropDS, b);

           GDALRasterIO(hSrcBand, GF_Read, cropX, cropY, cropWidth, cropHeight,
                        buffer, cropWidth, cropHeight, GDT_Byte, 0, 0);
           GDALRasterIO(hDstBand, GF_Write, 0, 0, cropWidth, cropHeight,
                        buffer, cropWidth, cropHeight, GDT_Byte, 0, 0);
       }
       CPLFree(buffer);

       // 导出到内存文件
       GDALDatasetH hOutDS = GDALCreateCopy(hDriver, vsimemPath, hCropDS, FALSE, papszOptions, NULL, NULL);
       CSLDestroy(papszOptions);
       GDALClose(hCropDS);

       if (hOutDS == NULL) {
           return -4;
       }
       GDALClose(hOutDS);

       // 读取内存文件内容
       vsi_l_offset nLength;
       GByte* pabyData = VSIGetMemFileBuffer(vsimemPath, &nLength, FALSE);
       if (pabyData == NULL) {
           VSIUnlink(vsimemPath);
           return -5;
       }

       // 复制数据
       *outData = (unsigned char*)malloc(nLength);
       memcpy(*outData, pabyData, nLength);
       *outLen = (int)nLength;

       // 清理
       VSIUnlink(vsimemPath);

       return 0;
   }

   // 清理vsimem文件
   void cleanupVsimem(const char* path) {
       VSIUnlink(path);
   }

   // 关闭数据集
   void closeDataset(GDALDatasetH hDS) {
       if (hDS != NULL) {
           GDALClose(hDS);
       }
   }

   // 获取数据集信息
   void getDatasetInfo(GDALDatasetH hDS, int* width, int* height, int* bands) {
       if (hDS != NULL) {
           *width = GDALGetRasterXSize(hDS);
           *height = GDALGetRasterYSize(hDS);
           *bands = GDALGetRasterCount(hDS);
       }
   }

*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

var gdalInitOnce sync.Once

// InitGDAL 初始化GDAL
func InitGDAL() {
	gdalInitOnce.Do(func() {
		C.initGDAL()
	})
}

// ImageProcessor GDAL图像处理器
type ImageProcessor struct {
	canvasDS    C.GDALDatasetH
	width       int
	height      int
	bands       int
	tileImages  []C.GDALDatasetH
	vsimemPaths []string
}

// NewImageProcessor 创建图像处理器
func NewImageProcessor(width, height, bands int) (*ImageProcessor, error) {
	InitGDAL()

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
	}, nil
}

// AddTileFromBuffer 从内存缓冲区添加瓦片
func (p *ImageProcessor) AddTileFromBuffer(data []byte, format string, dstX, dstY int) error {
	if len(data) == 0 {
		return errors.New("empty tile data")
	}

	cData := C.CString(string(data))
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	// 创建vsimem路径
	vsimemPath := fmt.Sprintf("/vsimem/tile_%p.%s", unsafe.Pointer(cData), format)
	p.vsimemPaths = append(p.vsimemPaths, vsimemPath)

	hDS := C.createMemDatasetFromBuffer(cData, C.int(len(data)), cFormat)
	if hDS == nil {
		C.free(unsafe.Pointer(cData))
		return errors.New("failed to open tile from buffer")
	}

	p.tileImages = append(p.tileImages, hDS)

	// 获取瓦片尺寸
	var tileWidth, tileHeight, tileBands C.int
	C.getDatasetInfo(hDS, &tileWidth, &tileHeight, &tileBands)

	// 复制到画布
	result := C.copyTileToCanvas(hDS, p.canvasDS,
		0, 0, tileWidth, tileHeight,
		C.int(dstX), C.int(dstY))

	if result != 0 {
		return fmt.Errorf("failed to copy tile to canvas: %d", result)
	}

	return nil
}

// CropAndExport 裁剪并导出
func (p *ImageProcessor) CropAndExport(cropX, cropY, cropWidth, cropHeight int, format string) ([]byte, error) {
	var outData *C.uchar
	var outLen C.int

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	result := C.cropAndExport(p.canvasDS,
		C.int(cropX), C.int(cropY), C.int(cropWidth), C.int(cropHeight),
		cFormat, &outData, &outLen)

	if result != 0 {
		return nil, fmt.Errorf("failed to crop and export: %d", result)
	}

	// 复制数据到Go切片
	data := C.GoBytes(unsafe.Pointer(outData), outLen)
	C.free(unsafe.Pointer(outData))

	return data, nil
}

// Close 关闭处理器并释放资源
func (p *ImageProcessor) Close() {
	// 关闭所有瓦片数据集
	for _, ds := range p.tileImages {
		C.closeDataset(ds)
	}

	// 清理vsimem文件
	for _, path := range p.vsimemPaths {
		cPath := C.CString(path)
		C.cleanupVsimem(cPath)
		C.free(unsafe.Pointer(cPath))
	}

	// 关闭画布
	C.closeDataset(p.canvasDS)
}
