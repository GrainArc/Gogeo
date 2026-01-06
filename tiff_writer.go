// Gogeo/tiff_writer.go
package Gogeo

/*
#include "osgeo_utils.h"

// 创建GeoTIFF数据集
GDALDatasetH createGeoTiffDataset(const char* filename, int width, int height, int bands, double* geoTransform) {
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

    // 设置投影为WGS84
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    OSRSetWellKnownGeogCS(hSRS, "WGS84");
    char *pszWKT = NULL;
    OSRExportToWkt(hSRS, &pszWKT);
    GDALSetProjection(hDS, pszWKT);
    CPLFree(pszWKT);
    OSRDestroySpatialReference(hSRS);

    return hDS;
}

// 创建内存中的GeoTIFF
GDALDatasetH createMemGeoTiff(int width, int height, int bands) {
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) {
        return NULL;
    }

    GDALDatasetH hDS = GDALCreate(hDriver, "", width, height, bands, GDT_Byte, NULL);
    return hDS;
}

// 将瓦片写入指定位置
int writeTileToDataset(GDALDatasetH hDstDS, GDALDatasetH hSrcDS, int dstX, int dstY, int width, int height) {
    if (hDstDS == NULL || hSrcDS == NULL) {
        return -1;
    }

    int srcBands = GDALGetRasterCount(hSrcDS);
    int dstBands = GDALGetRasterCount(hDstDS);
    int bands = srcBands < dstBands ? srcBands : dstBands;

    int srcWidth = GDALGetRasterXSize(hSrcDS);
    int srcHeight = GDALGetRasterYSize(hSrcDS);

    // 使用实际的源尺寸
    int readWidth = srcWidth < width ? srcWidth : width;
    int readHeight = srcHeight < height ? srcHeight : height;

    unsigned char *buffer = (unsigned char*)CPLMalloc(readWidth * readHeight);
    if (buffer == NULL) {
        return -2;
    }

    for (int b = 1; b <= bands; b++) {
        GDALRasterBandH hSrcBand = GDALGetRasterBand(hSrcDS, b);
        GDALRasterBandH hDstBand = GDALGetRasterBand(hDstDS, b);

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

    CPLFree(buffer);
    return 0;
}

// 导出为GeoTIFF文件
int exportToGeoTiff(GDALDatasetH hSrcDS, const char* filename, double* geoTransform) {
    if (hSrcDS == NULL || filename == NULL) {
        return -1;
    }

    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        return -2;
    }

    char **papszOptions = NULL;
    papszOptions = CSLSetNameValue(papszOptions, "COMPRESS", "LZW");
    papszOptions = CSLSetNameValue(papszOptions, "TILED", "YES");

    GDALDatasetH hDstDS = GDALCreateCopy(hDriver, filename, hSrcDS, FALSE, papszOptions, NULL, NULL);
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
	mu           sync.Mutex
	closed       bool
	tileImages   []C.GDALDatasetH
	vsimemPaths  []string
	memBuffers   []unsafe.Pointer
}

// NewGeoTiffWriter 创建GeoTIFF写入器
func NewGeoTiffWriter(width, height, bands, tileSize int, geoTransform [6]float64) (*GeoTiffWriter, error) {
	if width <= 0 || height <= 0 || bands <= 0 {
		return nil, errors.New("invalid dimensions")
	}

	gdalMutex.Lock()
	dataset := C.createMemGeoTiff(C.int(width), C.int(height), C.int(bands))
	gdalMutex.Unlock()

	if dataset == nil {
		return nil, errors.New("failed to create GeoTiff dataset")
	}

	return &GeoTiffWriter{
		dataset:      dataset,
		width:        width,
		height:       height,
		bands:        bands,
		tileSize:     tileSize,
		geoTransform: geoTransform,
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

	// 写入到目标数据集
	result := C.writeTileToDataset(w.dataset, hTileDS, C.int(dstX), C.int(dstY),
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
	result := C.exportToGeoTiff(w.dataset, cFilename, &geoTransform[0])
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
