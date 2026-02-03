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
// ==================== 波段/调色板管理 ====================

// 数据类型枚举（与GDAL对应）
typedef enum {
    BAND_Gray8 = 0,
    BAND_Gray16,
    BAND_Red8,
    BAND_Red16,
    BAND_Green8,
    BAND_Green16,
    BAND_Blue8,
    BAND_Blue16,
    BAND_Alpha8,
    BAND_Alpha16,
    BAND_Int8,
    BAND_Int16,
    BAND_Int32,
    BAND_Int64,
    BAND_UInt8,
    BAND_UInt16,
    BAND_UInt32,
    BAND_UInt64,
    BAND_Real32,
    BAND_Real64
} BandDataType;

// 颜色解释枚举
typedef enum {
    COLOR_Undefined = 0,
    COLOR_Gray,
    COLOR_Palette,
    COLOR_Red,
    COLOR_Green,
    COLOR_Blue,
    COLOR_Alpha,
    COLOR_Hue,
    COLOR_Saturation,
    COLOR_Lightness,
    COLOR_Cyan,
    COLOR_Magenta,
    COLOR_Yellow,
    COLOR_Black
} ColorInterpretation;

// 调色板条目
typedef struct {
    short c1;  // Red or Gray
    short c2;  // Green
    short c3;  // Blue
    short c4;  // Alpha
} PaletteEntry;

// 波段信息
typedef struct {
    int bandIndex;
    GDALDataType dataType;
    GDALColorInterp colorInterp;
    double noDataValue;
    int hasNoData;
    double minValue;
    double maxValue;
    int hasStats;
} BandInfo;

// 调色板信息
typedef struct {
    int entryCount;
    GDALPaletteInterp interpType;
    PaletteEntry* entries;
} PaletteInfo;

// ==================== 波段操作函数 ====================

// 获取波段信息
BandInfo* getBandInfo(GDALDatasetH hDS, int bandIndex);

// 获取所有波段信息
BandInfo* getAllBandsInfo(GDALDatasetH hDS, int* bandCount);

// 添加波段到数据集（返回新数据集）
GDALDatasetH addBandToDataset(GDALDatasetH hDS, BandDataType dataType,
                               ColorInterpretation colorInterp, double noDataValue);

// 删除波段（返回新数据集）
GDALDatasetH removeBandFromDataset(GDALDatasetH hDS, int bandIndex);

// 修改波段颜色解释
int setBandColorInterpretation(GDALDatasetH hDS, int bandIndex, ColorInterpretation colorInterp);

// 修改波段NoData值
int setBandNoDataValue(GDALDatasetH hDS, int bandIndex, double noDataValue);

// 删除波段NoData值
int deleteBandNoDataValue(GDALDatasetH hDS, int bandIndex);

// 复制波段数据
int copyBandData(GDALDatasetH srcDS, int srcBand, GDALDatasetH dstDS, int dstBand);

// 重排波段顺序（返回新数据集）
GDALDatasetH reorderBands(GDALDatasetH hDS, int* bandOrder, int bandCount);

// 转换波段数据类型（返回新数据集）
GDALDatasetH convertBandDataType(GDALDatasetH hDS, int bandIndex, BandDataType newType);

// ==================== 调色板操作函数 ====================

// 获取调色板信息
PaletteInfo* getPaletteInfo(GDALDatasetH hDS, int bandIndex);

// 释放调色板信息
void freePaletteInfo(PaletteInfo* info);

// 创建调色板
GDALColorTableH createColorTable(GDALPaletteInterp interpType);

// 添加调色板条目
int addPaletteEntry(GDALColorTableH hTable, int index, short c1, short c2, short c3, short c4);

// 设置波段调色板
int setBandColorTable(GDALDatasetH hDS, int bandIndex, GDALColorTableH hTable);

// 删除波段调色板
int deleteBandColorTable(GDALDatasetH hDS, int bandIndex);

// 修改调色板条目
int modifyPaletteEntry(GDALDatasetH hDS, int bandIndex, int entryIndex,
                       short c1, short c2, short c3, short c4);

// 从调色板图像转换为RGB
GDALDatasetH paletteToRGB(GDALDatasetH hDS);

// 从RGB转换为调色板图像
GDALDatasetH rgbToPalette(GDALDatasetH hDS, int colorCount);

// 释放波段信息
void freeBandInfo(BandInfo* info);

// GDAL数据类型转换
GDALDataType bandDataTypeToGDAL(BandDataType type);
BandDataType gdalToBandDataType(GDALDataType type);

// 颜色解释转换
GDALColorInterp colorInterpToGDAL(ColorInterpretation interp);
ColorInterpretation gdalToColorInterp(GDALColorInterp interp);
GDALDatasetH setBandColorInterpretationForced(GDALDatasetH hDS, int bandIndex, ColorInterpretation colorInterp);
GDALDatasetH ensureMemoryDataset(GDALDatasetH hDS);

// 编译后的表达式（不透明类型）
typedef struct CompiledExpression CompiledExpression;
typedef struct BlockCalculator BlockCalculator;

// 表达式编译与释放
CompiledExpression* compileExpression(const char* expression);
void freeCompiledExpression(CompiledExpression* ce);

// 波段表达式计算
double* calculateBandExpression(GDALDatasetH hDS, const char* expression, int* outSize);

// 带条件的计算
double* calculateBandExpressionWithCondition(GDALDatasetH hDS,
                                              const char* expression,
                                              const char* condition,
                                              double noDataValue,
                                              int* outSize);

// 条件替换
double* conditionalReplace(GDALDatasetH hDS, int bandIndex,
                           double* minValues, double* maxValues,
                           double* newValues, int* includeMin, int* includeMax,
                           int conditionCount, int* outSize);

// 分块计算器（用于超大影像）
BlockCalculator* createBlockCalculator(GDALDatasetH hDS, const char* expression,
                                        int blockWidth, int blockHeight);
void freeBlockCalculator(BlockCalculator* bc);
double* calculateBlock(BlockCalculator* bc, int blockX, int blockY,
                       int* outWidth, int* outHeight);

// 预定义指数计算
double* calculateNDVI(GDALDatasetH hDS, int nirBand, int redBand, int* outSize);
double* calculateNDWI(GDALDatasetH hDS, int greenBand, int nirBand, int* outSize);
double* calculateEVI(GDALDatasetH hDS, int nirBand, int redBand, int blueBand, int* outSize);
// ==================== 栅格镶嵌 ====================

// 镶嵌选项
typedef struct {
    int forceBandMatch;      // 强制波段匹配（删除多余波段）
    int resampleMethod;      // 重采样方法: 0=Nearest, 1=Bilinear, 2=Cubic, 3=CubicSpline, 4=Lanczos
    double noDataValue;      // 输出NoData值
    int hasNoData;           // 是否设置NoData
    int numThreads;          // 并行线程数，0表示自动
} MosaicOptions;

// 镶嵌信息结构
typedef struct {
    double minX, minY, maxX, maxY;  // 输出范围
    double resX, resY;               // 输出分辨率
    int width, height;               // 输出尺寸
    int bandCount;                   // 输出波段数
    GDALDataType dataType;           // 输出数据类型
    char projection[2048];           // 输出投影
} MosaicInfo;

// 数据集信息（用于镶嵌预处理）
typedef struct {
    GDALDatasetH dataset;
    double geoTransform[6];
    double minX, minY, maxX, maxY;
    double resX, resY;
    int width, height;
    int bandCount;
    GDALDataType dataType;
    int needsReproject;
    int needsResample;
    GDALDatasetH warpedDS;  // 重投影/重采样后的数据集
} MosaicInputInfo;

// 计算镶嵌参数
MosaicInfo* calculateMosaicInfo(GDALDatasetH* datasets, int datasetCount,
                                 MosaicOptions* options, char* errorMsg);

// 执行镶嵌
GDALDatasetH mosaicDatasets(GDALDatasetH* datasets, int datasetCount,
                            MosaicOptions* options, char* errorMsg);

// 释放镶嵌信息
void freeMosaicInfo(MosaicInfo* info);

// 内部函数
MosaicInputInfo* prepareMosaicInputs(GDALDatasetH* datasets, int datasetCount,
                                      MosaicInfo* info, MosaicOptions* options,
                                      char* errorMsg);
void freeMosaicInputs(MosaicInputInfo* inputs, int count);
int copyRasterToMosaic(MosaicInputInfo* input, GDALDatasetH outputDS, MosaicInfo* info);
GDALResampleAlg getResampleAlgorithm(int method);

#ifdef __cplusplus
}
#endif

#endif // OSGEO_UTILS_H
