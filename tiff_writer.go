// Gogeo/tiff_writer.go
package Gogeo

/*
#include "osgeo_utils.h"

// 创建GeoTIFF数据集
// 创建GeoTIFF数据集（支持指定EPSG代码）
GDALDatasetH createGeoTiffDatasetWithSRS(const char* filename, int width, int height,
                                          int bands, double* geoTransform, int epsgCode) {
    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        return NULL;
    }
    char **papszOptions = NULL;
    papszOptions = CSLSetNameValue(papszOptions, "COMPRESS", "LZW");
    papszOptions = CSLSetNameValue(papszOptions, "TILED", "YES");
    papszOptions = CSLSetNameValue(papszOptions, "BLOCKXSIZE", "256");
    papszOptions = CSLSetNameValue(papszOptions, "BLOCKYSIZE", "256");
    GDALDatasetH hDS = GDALCreate(hDriver, filename, width, height, bands, GDT_Byte, papszOptions);
    CSLDestroy(papszOptions);
    if (hDS == NULL) {
        return NULL;
    }
    // 设置地理变换参数
    if (geoTransform != NULL) {
        GDALSetGeoTransform(hDS, geoTransform);
    }
    // 设置投影（支持自定义EPSG）
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    if (epsgCode > 0) {
        OSRImportFromEPSG(hSRS, epsgCode);
    } else {
        OSRSetWellKnownGeogCS(hSRS, "WGS84");
    }

    char *pszWKT = NULL;
    OSRExportToWkt(hSRS, &pszWKT);
    GDALSetProjection(hDS, pszWKT);
    CPLFree(pszWKT);
    OSRDestroySpatialReference(hSRS);
    return hDS;
}
// 创建内存中的GeoTIFF
GDALDatasetH createMemGeoTiffWithSRS(int width, int height, int bands, int epsgCode) {
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) {
        return NULL;
    }

    GDALDatasetH hDS = GDALCreate(hDriver, "", width, height, bands, GDT_Byte, NULL);

    if (hDS != NULL) {
        // 设置坐标系
        if (epsgCode > 0) {
            OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
            OSRImportFromEPSG(hSRS, epsgCode);
            char *pszWKT = NULL;
            OSRExportToWkt(hSRS, &pszWKT);
            GDALSetProjection(hDS, pszWKT);
            CPLFree(pszWKT);
            OSRDestroySpatialReference(hSRS);
        }

        // 如果有4个波段，设置第4个波段为Alpha通道
        if (bands == 4) {
            GDALRasterBandH hAlphaBand = GDALGetRasterBand(hDS, 4);
            if (hAlphaBand != NULL) {
                GDALSetRasterColorInterpretation(hAlphaBand, GCI_AlphaBand);
                // 初始化Alpha通道为完全不透明(255)
                GDALFillRaster(hAlphaBand, 255.0, 0.0);
            }
        }

        // 设置RGB波段的颜色解释
        if (bands >= 3) {
            GDALSetRasterColorInterpretation(GDALGetRasterBand(hDS, 1), GCI_RedBand);
            GDALSetRasterColorInterpretation(GDALGetRasterBand(hDS, 2), GCI_GreenBand);
            GDALSetRasterColorInterpretation(GDALGetRasterBand(hDS, 3), GCI_BlueBand);
        }
    }

    return hDS;
}
int writeTileToDatasetRGB(GDALDatasetH hDstDS, GDALDatasetH hSrcDS,
                          int dstX, int dstY, int width, int height) {
    if (hDstDS == NULL || hSrcDS == NULL) {
        return -1;
    }

    int srcBands = GDALGetRasterCount(hSrcDS);
    int dstBands = GDALGetRasterCount(hDstDS);

    // 至少需要3个波段（RGB）
    if (srcBands < 3 || dstBands < 3) {
        return -1;
    }

    int srcWidth = GDALGetRasterXSize(hSrcDS);
    int srcHeight = GDALGetRasterYSize(hSrcDS);
    int readWidth = srcWidth < width ? srcWidth : width;
    int readHeight = srcHeight < height ? srcHeight : height;

    unsigned char *buffer = (unsigned char*)CPLMalloc(readWidth * readHeight);
    if (buffer == NULL) {
        return -2;
    }

    // 处理RGB波段（1, 2, 3）
    for (int i = 1; i <= 3; i++) {
        GDALRasterBandH hSrcBand = GDALGetRasterBand(hSrcDS, i);
        GDALRasterBandH hDstBand = GDALGetRasterBand(hDstDS, i);

        if (hSrcBand == NULL || hDstBand == NULL) {
            CPLFree(buffer);
            return -3;
        }

        CPLErr err = GDALRasterIO(hSrcBand, GF_Read, 0, 0, readWidth, readHeight,
                                   buffer, readWidth, readHeight, GDT_Byte, 0, 0);
        if (err != CE_None) {
            CPLFree(buffer);
            return -4;
        }

        err = GDALRasterIO(hDstBand, GF_Write, dstX, dstY, readWidth, readHeight,
                           buffer, readWidth, readHeight, GDT_Byte, 0, 0);
        if (err != CE_None) {
            CPLFree(buffer);
            return -5;
        }
    }

    // 处理Alpha通道（如果存在）
    if (srcBands >= 4 && dstBands >= 4) {
        GDALRasterBandH hSrcAlpha = GDALGetRasterBand(hSrcDS, 4);
        GDALRasterBandH hDstAlpha = GDALGetRasterBand(hDstDS, 4);

        if (hSrcAlpha != NULL && hDstAlpha != NULL) {
            CPLErr err = GDALRasterIO(hSrcAlpha, GF_Read, 0, 0, readWidth, readHeight,
                                       buffer, readWidth, readHeight, GDT_Byte, 0, 0);
            if (err == CE_None) {
                err = GDALRasterIO(hDstAlpha, GF_Write, dstX, dstY, readWidth, readHeight,
                                   buffer, readWidth, readHeight, GDT_Byte, 0, 0);
            }
            // Alpha通道错误不致命，继续处理
        }
    } else if (dstBands >= 4) {
        // 源没有Alpha通道，目标有，则填充为完全不透明(255)
        memset(buffer, 255, readWidth * readHeight);
        GDALRasterBandH hDstAlpha = GDALGetRasterBand(hDstDS, 4);
        if (hDstAlpha != NULL) {
            GDALRasterIO(hDstAlpha, GF_Write, dstX, dstY, readWidth, readHeight,
                         buffer, readWidth, readHeight, GDT_Byte, 0, 0);
        }
    }

    CPLFree(buffer);
    return 0;
}


// 导出为GeoTIFF文件（支持指定EPSG）
int exportToGeoTiffWithSRS(GDALDatasetH hSrcDS, const char* filename,
                           double* geoTransform, int epsgCode) {
    if (hSrcDS == NULL || filename == NULL) {
        return -1;
    }

    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        return -2;
    }

    int bands = GDALGetRasterCount(hSrcDS);

    char **papszOptions = NULL;
    papszOptions = CSLSetNameValue(papszOptions, "COMPRESS", "LZW");
    papszOptions = CSLSetNameValue(papszOptions, "TILED", "YES");
    papszOptions = CSLSetNameValue(papszOptions, "BLOCKXSIZE", "256");
    papszOptions = CSLSetNameValue(papszOptions, "BLOCKYSIZE", "256");

    // 根据波段数设置PHOTOMETRIC
    if (bands == 3) {
        papszOptions = CSLSetNameValue(papszOptions, "PHOTOMETRIC", "RGB");
    } else if (bands == 4) {
        // 4波段时使用RGB+Alpha
        papszOptions = CSLSetNameValue(papszOptions, "PHOTOMETRIC", "RGB");
        papszOptions = CSLSetNameValue(papszOptions, "ALPHA", "YES");
    }

    GDALDatasetH hDstDS = GDALCreateCopy(hDriver, filename, hSrcDS, FALSE,
                                         papszOptions, NULL, NULL);
    CSLDestroy(papszOptions);

    if (hDstDS == NULL) {
        return -3;
    }

    // 设置地理变换
    if (geoTransform != NULL) {
        GDALSetGeoTransform(hDstDS, geoTransform);
    }

    // 设置投影
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    if (epsgCode > 0) {
        OSRImportFromEPSG(hSRS, epsgCode);
    } else {
        OSRSetWellKnownGeogCS(hSRS, "WGS84");
    }

    char *pszWKT = NULL;
    OSRExportToWkt(hSRS, &pszWKT);
    GDALSetProjection(hDstDS, pszWKT);
    CPLFree(pszWKT);
    OSRDestroySpatialReference(hSRS);

    // 确保Alpha通道的颜色解释正确
    if (bands == 4) {
        GDALRasterBandH hAlphaBand = GDALGetRasterBand(hDstDS, 4);
        if (hAlphaBand != NULL) {
            GDALSetRasterColorInterpretation(hAlphaBand, GCI_AlphaBand);
        }
    }

    GDALClose(hDstDS);
    return 0;
}
// 导出为内存中的GeoTIFF
int exportToMemoryGeoTiff(GDALDatasetH hSrcDS, double* geoTransform,
                          unsigned char** outData, int* outLen) {
    if (hSrcDS == NULL || outData == NULL || outLen == NULL) {
        return -1;
    }

    // 创建vsimem路径
    const char* vsimemPath = "/vsimem/output_geotiff.tif";

    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        return -2;
    }

    char **papszOptions = NULL;
    papszOptions = CSLSetNameValue(papszOptions, "COMPRESS", "LZW");

    GDALDatasetH hDstDS = GDALCreateCopy(hDriver, vsimemPath, hSrcDS, FALSE, papszOptions, NULL, NULL);
    CSLDestroy(papszOptions);

    if (hDstDS == NULL) {
        return -3;
    }

    // 设置地理变换
    if (geoTransform != NULL) {
        GDALSetGeoTransform(hDstDS, geoTransform);
    }

    // 设置投影
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    OSRSetWellKnownGeogCS(hSRS, "WGS84");
    char *pszWKT = NULL;
    OSRExportToWkt(hSRS, &pszWKT);
    GDALSetProjection(hDstDS, pszWKT);
    CPLFree(pszWKT);
    OSRDestroySpatialReference(hSRS);

    GDALClose(hDstDS);

    // 读取vsimem文件
    vsi_l_offset nLength = 0;
    GByte* pabyData = VSIGetMemFileBuffer(vsimemPath, &nLength, FALSE);

    if (pabyData == NULL || nLength == 0) {
        VSIUnlink(vsimemPath);
        return -4;
    }

    // 复制数据
    *outData = (unsigned char*)CPLMalloc(nLength);
    memcpy(*outData, pabyData, nLength);
    *outLen = (int)nLength;

    VSIUnlink(vsimemPath);
    return 0;
}

*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// GeoTiffWriter GeoTIFF写入器
type GeoTiffWriter struct {
	dataset      C.GDALDatasetH
	width        int
	height       int
	bands        int
	tileSize     int
	geoTransform [6]float64
	epsgCode     int // 新增：EPSG代码
	mu           sync.Mutex
	closed       bool
	tileImages   []C.GDALDatasetH
	vsimemPaths  []string
	memBuffers   []unsafe.Pointer
}

// NewGeoTiffWriter 创建GeoTIFF写入器
func NewGeoTiffWriter(width, height, bands, tileSize int, geoTransform [6]float64) (*GeoTiffWriter, error) {
	return NewGeoTiffWriterWithSRS(width, height, bands, tileSize, geoTransform, 4326)
}
func NewGeoTiffWriterWithSRS(width, height, bands, tileSize int, geoTransform [6]float64, epsgCode int) (*GeoTiffWriter, error) {
	if width <= 0 || height <= 0 || bands <= 0 {
		return nil, errors.New("invalid dimensions")
	}
	gdalMutex.Lock()
	dataset := C.createMemGeoTiffWithSRS(C.int(width), C.int(height), C.int(bands), C.int(epsgCode))
	gdalMutex.Unlock()
	if dataset == nil {
		return nil, errors.New("failed to create GeoTiff dataset")
	}
	// 设置GeoTransform
	var cGeoTransform [6]C.double
	for i := 0; i < 6; i++ {
		cGeoTransform[i] = C.double(geoTransform[i])
	}
	C.GDALSetGeoTransform(dataset, &cGeoTransform[0])
	return &GeoTiffWriter{
		dataset:      dataset,
		width:        width,
		height:       height,
		bands:        bands,
		tileSize:     tileSize,
		geoTransform: geoTransform,
		epsgCode:     epsgCode,
		tileImages:   make([]C.GDALDatasetH, 0),
		vsimemPaths:  make([]string, 0),
		memBuffers:   make([]unsafe.Pointer, 0),
	}, nil
}

// WriteTile 写入瓦片到指定位置
func (w *GeoTiffWriter) WriteTile(tileData []byte, format string, dstX, dstY int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("writer is closed")
	}
	if len(tileData) == 0 {
		return errors.New("empty tile data")
	}
	// 创建vsimem路径
	tileID := atomic.AddUint64(&tileCounter, 1)
	vsimemPath := fmt.Sprintf("/vsimem/geotiff_tile_%d.%s", tileID, format)
	// 分配C内存
	cData := C.malloc(C.size_t(len(tileData)))
	if cData == nil {
		return errors.New("failed to allocate memory")
	}
	C.memcpy(cData, unsafe.Pointer(&tileData[0]), C.size_t(len(tileData)))
	w.memBuffers = append(w.memBuffers, cData)
	cVsimemPath := C.CString(vsimemPath)
	defer C.free(unsafe.Pointer(cVsimemPath))
	gdalMutex.Lock()
	defer gdalMutex.Unlock()
	// 创建vsimem文件
	fp := C.VSIFileFromMemBuffer(cVsimemPath, (*C.GByte)(cData),
		C.vsi_l_offset(len(tileData)), C.FALSE)
	if fp == nil {
		return errors.New("failed to create vsimem file")
	}
	C.VSIFCloseL(fp)
	w.vsimemPaths = append(w.vsimemPaths, vsimemPath)
	// 打开瓦片数据集
	hTileDS := C.GDALOpen(cVsimemPath, C.GA_ReadOnly)
	if hTileDS == nil {
		return fmt.Errorf("failed to open tile, format: %s", format)
	}
	w.tileImages = append(w.tileImages, hTileDS)
	// 使用RGB顺序写入
	result := C.writeTileToDatasetRGB(w.dataset, hTileDS, C.int(dstX), C.int(dstY),
		C.int(w.tileSize), C.int(w.tileSize))
	if result != 0 {
		return fmt.Errorf("failed to write tile: error code %d", result)
	}
	return nil
}

// ExportToFile 导出为GeoTIFF文件
func (w *GeoTiffWriter) ExportToFile(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("writer is closed")
	}
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))
	var geoTransform [6]C.double
	for i := 0; i < 6; i++ {
		geoTransform[i] = C.double(w.geoTransform[i])
	}
	gdalMutex.Lock()
	result := C.exportToGeoTiffWithSRS(w.dataset, cFilename, &geoTransform[0], C.int(w.epsgCode))
	gdalMutex.Unlock()
	if result != 0 {
		return fmt.Errorf("failed to export GeoTiff: error code %d", result)
	}
	return nil
}

// ExportToMemory 导出为内存中的GeoTIFF
func (w *GeoTiffWriter) ExportToMemory() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, errors.New("writer is closed")
	}

	var outData *C.uchar
	var outLen C.int

	var geoTransform [6]C.double
	for i := 0; i < 6; i++ {
		geoTransform[i] = C.double(w.geoTransform[i])
	}

	gdalMutex.Lock()
	result := C.exportToMemoryGeoTiff(w.dataset, &geoTransform[0], &outData, &outLen)
	gdalMutex.Unlock()

	if result != 0 {
		return nil, fmt.Errorf("failed to export to memory: error code %d", result)
	}

	if outData == nil || outLen <= 0 {
		return nil, errors.New("export returned empty data")
	}

	data := C.GoBytes(unsafe.Pointer(outData), outLen)
	C.free(unsafe.Pointer(outData))

	return data, nil
}

// Close 关闭写入器
func (w *GeoTiffWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}
	w.closed = true

	gdalMutex.Lock()
	defer gdalMutex.Unlock()

	// 关闭瓦片数据集
	for _, ds := range w.tileImages {
		if ds != nil {
			C.closeDataset(ds)
		}
	}
	w.tileImages = nil

	// 删除vsimem文件
	for _, path := range w.vsimemPaths {
		cPath := C.CString(path)
		C.VSIUnlink(cPath)
		C.free(unsafe.Pointer(cPath))
	}
	w.vsimemPaths = nil

	// 释放C内存
	for _, buf := range w.memBuffers {
		if buf != nil {
			C.free(buf)
		}
	}
	w.memBuffers = nil

	// 关闭数据集
	if w.dataset != nil {
		C.closeDataset(w.dataset)
		w.dataset = nil
	}
}

// GetDimensions 获取尺寸
func (w *GeoTiffWriter) GetDimensions() (width, height, bands int) {
	return w.width, w.height, w.bands
}
