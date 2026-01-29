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
// osgeo_utils.h
#ifndef OSGEO_UTILS_H
#define OSGEO_UTILS_H
#include <math.h>
#include <gdal.h>
#include <gdal_utils.h>
#include <gdal_alg.h>
#include <gdalwarper.h> 
#include <ogr_api.h>
#include <ogr_srs_api.h>
#include <cpl_error.h>
#include <cpl_conv.h>
#include <cpl_string.h>
#include <stdlib.h>
#include <cpl_vsi.h>
#include <gdal_version.h>
#ifdef __cplusplus
extern "C" {
#endif

// 声明外部函数，避免重复定义
extern int handleProgressUpdate(double, char*, void*);
// 获取数据集信息
typedef struct {
    int width;
    int height;
    int bandCount;
    double geoTransform[6];
    char projection[2048];
} DatasetInfo;
typedef struct {
    unsigned char* data;
    size_t size;
} ImageBuffer;
OGRLayerH createMemoryLayer(const char* layerName, OGRwkbGeometryType geomType, OGRSpatialReferenceH srs);
int check_isnan(double x);
int check_isinf(double x);
int addFieldToLayer(OGRLayerH layer, const char* fieldName, OGRFieldType fieldType);
void copyFieldValue(OGRFeatureH srcFeature, OGRFeatureH dstFeature, int srcFieldIndex, int dstFieldIndex);
int progressCallback(double dfComplete, const char *pszMessage, void *pProgressArg);
OGRLayerH cloneLayerToMemory(OGRLayerH sourceLayer, const char* layerName);
int copyFeaturesWithSpatialFilter(OGRLayerH sourceLayer, OGRLayerH targetLayer, OGRGeometryH filterGeom);
int copyAllFeatures(OGRLayerH sourceLayer, OGRLayerH targetLayer);
int isFeatureOnBorder(OGRFeatureH feature, double minX, double minY, double maxX, double maxY, double buffer);
int geometryWKTEqual(OGRGeometryH geom1, OGRGeometryH geom2);
OGRGeometryH setPrecisionIfNeeded(OGRGeometryH geom, double gridSize, int flags);
int setLayerGeometryPrecision(OGRLayerH layer, double gridSize, int flags);
OGRFeatureH setFeatureGeometryPrecision(OGRFeatureH feature, double gridSize, int flags);
OGRGeometryH forceGeometryType(OGRGeometryH geom, OGRwkbGeometryType targetType);
OGRGeometryH mergeGeometryCollection(OGRGeometryH geomCollection, OGRwkbGeometryType targetType);
OGRGeometryH normalizeGeometryType(OGRGeometryH geom, OGRwkbGeometryType expectedType);
OGRGeometryH createTileClipGeometry(double minX, double minY, double maxX, double maxY);
OGRLayerH clipLayerToTile(OGRLayerH sourceLayer, double minX, double minY, double maxX, double maxY, const char* layerName, const char* sourceIdentifier);
void getTileBounds(int x, int y, int zoom, double* minX, double* minY, double* maxX, double* maxY);
GDALDatasetH reprojectToWebMercator(GDALDatasetH hSrcDS);
int readTileData(GDALDatasetH hDS, double minX, double minY, double maxX, double maxY,
                 int tileSize, unsigned char* buffer);
int getDatasetInfo(GDALDatasetH hDS, DatasetInfo* info);
GDALDatasetH clipRasterByGeometry(GDALDatasetH srcDS, OGRGeometryH geom, double *bounds);
int writeJPEG(GDALDatasetH ds, const char* filename, int quality);
int writeImage(GDALDatasetH ds, const char* filename, const char* format, int quality);
ImageBuffer* writeImageToMemory(GDALDatasetH ds, const char* format, int quality);
GDALDatasetH clipPixelRasterByMask(GDALDatasetH srcDS, OGRGeometryH geom, double *bounds);
int readTileDataFloat32(GDALDatasetH dataset,
                        double minX, double minY, double maxX, double maxY,
                        int tileSize, float* buffer);
void freeImageBuffer(ImageBuffer *buffer);

// 创建空白内存数据集
GDALDatasetH createBlankMemDataset(int width, int height, int bands);

// 从内存缓冲区创建数据集
GDALDatasetH createMemDatasetFromBuffer(const char* data, int dataLen, const char* format);

// 获取数据集信息
void getDatasetInfoSimple(GDALDatasetH hDS, int* width, int* height, int* bands);

// 复制瓦片到画布
int copyTileToCanvas(GDALDatasetH srcDS, GDALDatasetH dstDS,
                     int srcX, int srcY, int srcWidth, int srcHeight,
                     int dstX, int dstY);

// 裁剪并导出
int cropAndExport(GDALDatasetH srcDS,
                  int cropX, int cropY, int cropWidth, int cropHeight,
                  const char* format,
                  unsigned char** outData, int* outLen);
// 裁剪、缩放并导出
int cropScaleAndExport(GDALDatasetH hSrcDS,
                       int cropX, int cropY, int cropWidth, int cropHeight,
                       int outputWidth, int outputHeight,
                       const char* format,
                       unsigned char** outData, int* outLen);
// 关闭数据集
void closeDataset(GDALDatasetH hDS);

// 清理vsimem文件
void cleanupVsimem(const char* path);
int readTileDataFast(GDALDatasetH dataset,
                     double minX, double minY, double maxX, double maxY,
                     int tileSize, unsigned char* buffer);


// 创建栅格数据集
GDALDatasetH createRasterDataset(int width, int height, int bands,
                                  double minX, double minY, double maxX, double maxY,
                                  int epsg);

// 单色栅格化
int rasterizeLayerWithColor(GDALDatasetH rasterDS, OGRLayerH layer,
                             int r, int g, int b, int a);

// 按属性栅格化
int rasterizeLayerByAttribute(GDALDatasetH rasterDS, OGRLayerH layer,
                               const char* attrName, const char* attrValue,
                               int r, int g, int b, int a);

// 转换为PNG
ImageBuffer* rasterDatasetToPNG(GDALDatasetH rasterDS);

// 释放ImageBuffer
void freeImageBuffer(ImageBuffer* buffer);

#ifdef __cplusplus
}
#endif

#endif // OSGEO_UTILS_H
