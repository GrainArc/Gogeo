#include "osgeo_utils.h"
#include <string.h>

// ==================== 类型转换函数 ====================

GDALDataType bandDataTypeToGDAL(BandDataType type) {
    switch (type) {
        case BAND_Gray8:
        case BAND_Red8:
        case BAND_Green8:
        case BAND_Blue8:
        case BAND_Alpha8:
        case BAND_UInt8:
            return GDT_Byte;
        case BAND_Gray16:
        case BAND_Red16:
        case BAND_Green16:
        case BAND_Blue16:
        case BAND_Alpha16:
        case BAND_UInt16:
            return GDT_UInt16;
        case BAND_Int8:
#if GDAL_VERSION_NUM >= 3070000
            return GDT_Int8;
#else
            return GDT_Int16;
#endif
        case BAND_Int16:
            return GDT_Int16;
        case BAND_Int32:
            return GDT_Int32;
        case BAND_Int64:
#if GDAL_VERSION_NUM >= 3050000
            return GDT_Int64;
#else
            return GDT_Int32;
#endif
        case BAND_UInt32:
            return GDT_UInt32;
        case BAND_UInt64:
#if GDAL_VERSION_NUM >= 3050000
            return GDT_UInt64;
#else
            return GDT_UInt32;
#endif
        case BAND_Real32:
            return GDT_Float32;
        case BAND_Real64:
            return GDT_Float64;
        default:
            return GDT_Byte;
    }
}

BandDataType gdalToBandDataType(GDALDataType type) {
    switch (type) {
        case GDT_Byte:
            return BAND_UInt8;
        case GDT_UInt16:
            return BAND_UInt16;
        case GDT_Int16:
            return BAND_Int16;
        case GDT_UInt32:
            return BAND_UInt32;
        case GDT_Int32:
            return BAND_Int32;
#if GDAL_VERSION_NUM >= 3050000
        case GDT_UInt64:
            return BAND_UInt64;
        case GDT_Int64:
            return BAND_Int64;
#endif
#if GDAL_VERSION_NUM >= 3070000
        case GDT_Int8:
            return BAND_Int8;
#endif
        case GDT_Float32:
            return BAND_Real32;
        case GDT_Float64:
            return BAND_Real64;
        default:
            return BAND_UInt8;
    }
}

GDALColorInterp colorInterpToGDAL(ColorInterpretation interp) {
    switch (interp) {
        case COLOR_Undefined: return GCI_Undefined;
        case COLOR_Gray: return GCI_GrayIndex;
        case COLOR_Palette: return GCI_PaletteIndex;
        case COLOR_Red: return GCI_RedBand;
        case COLOR_Green: return GCI_GreenBand;
        case COLOR_Blue: return GCI_BlueBand;
        case COLOR_Alpha: return GCI_AlphaBand;
        case COLOR_Hue: return GCI_HueBand;
        case COLOR_Saturation: return GCI_SaturationBand;
        case COLOR_Lightness: return GCI_LightnessBand;
        case COLOR_Cyan: return GCI_CyanBand;
        case COLOR_Magenta: return GCI_MagentaBand;
        case COLOR_Yellow: return GCI_YellowBand;
        case COLOR_Black: return GCI_BlackBand;
        default: return GCI_Undefined;
    }
}

ColorInterpretation gdalToColorInterp(GDALColorInterp interp) {
    switch (interp) {
        case GCI_Undefined: return COLOR_Undefined;
        case GCI_GrayIndex: return COLOR_Gray;
        case GCI_PaletteIndex: return COLOR_Palette;
        case GCI_RedBand: return COLOR_Red;
        case GCI_GreenBand: return COLOR_Green;
        case GCI_BlueBand: return COLOR_Blue;
        case GCI_AlphaBand: return COLOR_Alpha;
        case GCI_HueBand: return COLOR_Hue;
        case GCI_SaturationBand: return COLOR_Saturation;
        case GCI_LightnessBand: return COLOR_Lightness;
        case GCI_CyanBand: return COLOR_Cyan;
        case GCI_MagentaBand: return COLOR_Magenta;
        case GCI_YellowBand: return COLOR_Yellow;
        case GCI_BlackBand: return COLOR_Black;
        default: return COLOR_Undefined;
    }
}

// ==================== 波段信息函数 ====================

BandInfo* getBandInfo(GDALDatasetH hDS, int bandIndex) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return NULL;
    }

    BandInfo* info = (BandInfo*)CPLMalloc(sizeof(BandInfo));
    if (info == NULL) return NULL;

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) {
        CPLFree(info);
        return NULL;
    }

    info->bandIndex = bandIndex;
    info->dataType = GDALGetRasterDataType(hBand);
    info->colorInterp = GDALGetRasterColorInterpretation(hBand);

    int hasNoData = 0;
    info->noDataValue = GDALGetRasterNoDataValue(hBand, &hasNoData);
    info->hasNoData = hasNoData;

    // 获取统计信息
    double minVal, maxVal, meanVal, stdDev;
    CPLErr err = GDALGetRasterStatistics(hBand, FALSE, FALSE, &minVal, &maxVal, &meanVal, &stdDev);
    if (err == CE_None) {
        info->minValue = minVal;
        info->maxValue = maxVal;
        info->hasStats = 1;
    } else {
        info->minValue = 0;
        info->maxValue = 0;
        info->hasStats = 0;
    }

    return info;
}

BandInfo* getAllBandsInfo(GDALDatasetH hDS, int* bandCount) {
    if (hDS == NULL || bandCount == NULL) {
        return NULL;
    }

    *bandCount = GDALGetRasterCount(hDS);
    if (*bandCount == 0) {
        return NULL;
    }

    BandInfo* infos = (BandInfo*)CPLMalloc(sizeof(BandInfo) * (*bandCount));
    if (infos == NULL) return NULL;

    for (int i = 0; i < *bandCount; i++) {
        GDALRasterBandH hBand = GDALGetRasterBand(hDS, i + 1);
        if (hBand == NULL) {
            CPLFree(infos);
            return NULL;
        }

        infos[i].bandIndex = i + 1;
        infos[i].dataType = GDALGetRasterDataType(hBand);
        infos[i].colorInterp = GDALGetRasterColorInterpretation(hBand);

        int hasNoData = 0;
        infos[i].noDataValue = GDALGetRasterNoDataValue(hBand, &hasNoData);
        infos[i].hasNoData = hasNoData;

        double minVal, maxVal, meanVal, stdDev;
        CPLErr err = GDALGetRasterStatistics(hBand, FALSE, FALSE, &minVal, &maxVal, &meanVal, &stdDev);
        if (err == CE_None) {
            infos[i].minValue = minVal;
            infos[i].maxValue = maxVal;
            infos[i].hasStats = 1;
        } else {
            infos[i].minValue = 0;
            infos[i].maxValue = 0;
            infos[i].hasStats = 0;
        }
    }

    return infos;
}

void freeBandInfo(BandInfo* info) {
    if (info != NULL) {
        CPLFree(info);
    }
}

// ==================== 波段操作函数 ====================
// osgeo_utils.c - 修复 addBandToDataset 函数

GDALDatasetH addBandToDataset(GDALDatasetH hDS, BandDataType dataType,
                               ColorInterpretation colorInterp, double noDataValue) {
    if (hDS == NULL) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    GDALDataType gdalNewBandType = bandDataTypeToGDAL(dataType);

    // 获取原始数据集第一个波段的数据类型作为基础类型
    GDALDataType baseType = GDT_Byte;
    if (bandCount > 0) {
        baseType = GDALGetRasterDataType(GDALGetRasterBand(hDS, 1));
    }

    // 【关键】先收集所有源波段的颜色解释信息
    GDALColorInterp* srcColorInterps = NULL;
    GDALColorTableH* srcColorTables = NULL;
    double* srcNoDataValues = NULL;
    int* srcHasNoData = NULL;

    if (bandCount > 0) {
        srcColorInterps = (GDALColorInterp*)CPLMalloc(sizeof(GDALColorInterp) * bandCount);
        srcColorTables = (GDALColorTableH*)CPLMalloc(sizeof(GDALColorTableH) * bandCount);
        srcNoDataValues = (double*)CPLMalloc(sizeof(double) * bandCount);
        srcHasNoData = (int*)CPLMalloc(sizeof(int) * bandCount);

        for (int i = 0; i < bandCount; i++) {
            GDALRasterBandH srcBand = GDALGetRasterBand(hDS, i + 1);
            srcColorInterps[i] = GDALGetRasterColorInterpretation(srcBand);
            srcColorTables[i] = GDALGetRasterColorTable(srcBand);
            if (srcColorTables[i] != NULL) {
                srcColorTables[i] = GDALCloneColorTable(srcColorTables[i]);
            }
            srcNoDataValues[i] = GDALGetRasterNoDataValue(srcBand, &srcHasNoData[i]);
        }
    }

    // 创建新的内存数据集
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) {
        if (srcColorInterps) CPLFree(srcColorInterps);
        if (srcColorTables) {
            for (int i = 0; i < bandCount; i++) {
                if (srcColorTables[i]) GDALDestroyColorTable(srcColorTables[i]);
            }
            CPLFree(srcColorTables);
        }
        if (srcNoDataValues) CPLFree(srcNoDataValues);
        if (srcHasNoData) CPLFree(srcHasNoData);
        return NULL;
    }

    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, bandCount + 1, baseType, NULL);
    if (hNewDS == NULL) {
        if (srcColorInterps) CPLFree(srcColorInterps);
        if (srcColorTables) {
            for (int i = 0; i < bandCount; i++) {
                if (srcColorTables[i]) GDALDestroyColorTable(srcColorTables[i]);
            }
            CPLFree(srcColorTables);
        }
        if (srcNoDataValues) CPLFree(srcNoDataValues);
        if (srcHasNoData) CPLFree(srcHasNoData);
        return NULL;
    }

    // 复制地理变换和投影
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }
    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }

    // 复制现有波段数据和所有属性
    for (int i = 1; i <= bandCount; i++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, i);
        GDALRasterBandH dstBand = GDALGetRasterBand(hNewDS, i);
        GDALDataType srcType = GDALGetRasterDataType(srcBand);
        int typeSize = GDALGetDataTypeSizeBytes(srcType);

        // 复制数据
        void* buffer = CPLMalloc((size_t)width * (size_t)height * typeSize);
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        CPLFree(buffer);

        // 【关键】使用预先保存的颜色解释
        GDALSetRasterColorInterpretation(dstBand, srcColorInterps[i-1]);

        // 复制调色板
        if (srcColorTables[i-1] != NULL) {
            GDALSetRasterColorTable(dstBand, srcColorTables[i-1]);
        }

        // 复制NoData
        if (srcHasNoData[i-1]) {
            GDALSetRasterNoDataValue(dstBand, srcNoDataValues[i-1]);
        }

        // 复制描述
        const char* desc = GDALGetDescription((GDALMajorObjectH)srcBand);
        if (desc != NULL && strlen(desc) > 0) {
            GDALSetDescription((GDALMajorObjectH)dstBand, desc);
        }

        // 复制元数据
        char** metadata = GDALGetMetadata((GDALMajorObjectH)srcBand, NULL);
        if (metadata != NULL) {
            GDALSetMetadata((GDALMajorObjectH)dstBand, metadata, NULL);
        }
    }

    // 清理临时存储
    if (srcColorInterps) CPLFree(srcColorInterps);
    if (srcColorTables) {
        for (int i = 0; i < bandCount; i++) {
            if (srcColorTables[i]) GDALDestroyColorTable(srcColorTables[i]);
        }
        CPLFree(srcColorTables);
    }
    if (srcNoDataValues) CPLFree(srcNoDataValues);
    if (srcHasNoData) CPLFree(srcHasNoData);

    // 设置新波段
    GDALRasterBandH newBand = GDALGetRasterBand(hNewDS, bandCount + 1);
    GDALSetRasterColorInterpretation(newBand, colorInterpToGDAL(colorInterp));
    GDALSetRasterNoDataValue(newBand, noDataValue);

    // 初始化新波段为NoData值
    int newTypeSize = GDALGetDataTypeSizeBytes(gdalNewBandType);
    size_t bufSize = (size_t)width * (size_t)height * newTypeSize;
    void* initBuffer = CPLMalloc(bufSize);
    memset(initBuffer, 0, bufSize);

    // 对于Byte类型，填充noDataValue
    if (gdalNewBandType == GDT_Byte) {
        memset(initBuffer, (unsigned char)noDataValue, (size_t)width * (size_t)height);
    }

    GDALRasterIO(newBand, GF_Write, 0, 0, width, height, initBuffer, width, height, gdalNewBandType, 0, 0);
    CPLFree(initBuffer);

    return hNewDS;
}


GDALDatasetH removeBandFromDataset(GDALDatasetH hDS, int bandIndex) {
    if (hDS == NULL) return NULL;
    int bandCount = GDALGetRasterCount(hDS);
    if (bandIndex < 1 || bandIndex > bandCount || bandCount <= 1) {
        return NULL;
    }
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    // 获取第一个保留波段的数据类型
    int firstBand = (bandIndex == 1) ? 2 : 1;
    GDALDataType dataType = GDALGetRasterDataType(GDALGetRasterBand(hDS, firstBand));
    // 创建新数据集
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) return NULL;
    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, bandCount - 1, dataType, NULL);
    if (hNewDS == NULL) return NULL;
    // 复制地理变换和投影
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }
    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }
    // 复制波段（跳过要删除的波段）
    int dstBandIdx = 1;
    for (int i = 1; i <= bandCount; i++) {
        if (i == bandIndex) continue;
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, i);
        GDALRasterBandH dstBand = GDALGetRasterBand(hNewDS, dstBandIdx);
        GDALDataType srcType = GDALGetRasterDataType(srcBand);
        int typeSize = GDALGetDataTypeSizeBytes(srcType);
        void* buffer = CPLMalloc(width * height * typeSize);
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        CPLFree(buffer);
        // 【关键】复制颜色解释
        GDALSetRasterColorInterpretation(dstBand, GDALGetRasterColorInterpretation(srcBand));
        // 复制NoData
        int hasNoData = 0;
        double noData = GDALGetRasterNoDataValue(srcBand, &hasNoData);
        if (hasNoData) {
            GDALSetRasterNoDataValue(dstBand, noData);
        }
        // 复制调色板
        GDALColorTableH colorTable = GDALGetRasterColorTable(srcBand);
        if (colorTable != NULL) {
            GDALSetRasterColorTable(dstBand, GDALCloneColorTable(colorTable));
        }
        // 复制描述
        const char* desc = GDALGetDescription(srcBand);
        if (desc != NULL && strlen(desc) > 0) {
            GDALSetDescription(dstBand, desc);
        }
        // 复制元数据
        char** metadata = GDALGetMetadata(srcBand, NULL);
        if (metadata != NULL) {
            GDALSetMetadata(dstBand, metadata, NULL);
        }
        dstBandIdx++;
    }
    return hNewDS;
}
int setBandColorInterpretation(GDALDatasetH hDS, int bandIndex, ColorInterpretation colorInterp) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

    CPLErr err = GDALSetRasterColorInterpretation(hBand, colorInterpToGDAL(colorInterp));
    return (err == CE_None) ? 1 : 0;
}

int setBandNoDataValue(GDALDatasetH hDS, int bandIndex, double noDataValue) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

    CPLErr err = GDALSetRasterNoDataValue(hBand, noDataValue);
    return (err == CE_None) ? 1 : 0;
}

int deleteBandNoDataValue(GDALDatasetH hDS, int bandIndex) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

#if GDAL_VERSION_NUM >= 3030000
    CPLErr err = GDALDeleteRasterNoDataValue(hBand);
    return (err == CE_None) ? 1 : 0;
#else
    // 旧版本GDAL不支持删除NoData，设置为NaN作为替代
    return 0;
#endif
}

int copyBandData(GDALDatasetH srcDS, int srcBandIdx, GDALDatasetH dstDS, int dstBandIdx) {
    if (srcDS == NULL || dstDS == NULL) return 0;
    if (srcBandIdx < 1 || srcBandIdx > GDALGetRasterCount(srcDS)) return 0;
    if (dstBandIdx < 1 || dstBandIdx > GDALGetRasterCount(dstDS)) return 0;

    GDALRasterBandH srcBand = GDALGetRasterBand(srcDS, srcBandIdx);
    GDALRasterBandH dstBand = GDALGetRasterBand(dstDS, dstBandIdx);

    int srcWidth = GDALGetRasterBandXSize(srcBand);
    int srcHeight = GDALGetRasterBandYSize(srcBand);
    int dstWidth = GDALGetRasterBandXSize(dstBand);
    int dstHeight = GDALGetRasterBandYSize(dstBand);

    if (srcWidth != dstWidth || srcHeight != dstHeight) return 0;

    GDALDataType srcType = GDALGetRasterDataType(srcBand);
    int typeSize = GDALGetDataTypeSizeBytes(srcType);
    void* buffer = CPLMalloc(srcWidth * srcHeight * typeSize);

    CPLErr err1 = GDALRasterIO(srcBand, GF_Read, 0, 0, srcWidth, srcHeight,
                                buffer, srcWidth, srcHeight, srcType, 0, 0);
    CPLErr err2 = GDALRasterIO(dstBand, GF_Write, 0, 0, dstWidth, dstHeight,
                                buffer, dstWidth, dstHeight, srcType, 0, 0);

    CPLFree(buffer);
    return (err1 == CE_None && err2 == CE_None) ? 1 : 0;
}
GDALDatasetH reorderBands(GDALDatasetH hDS, int* bandOrder, int newBandCount) {
    if (hDS == NULL || bandOrder == NULL || newBandCount <= 0) return NULL;
    int srcBandCount = GDALGetRasterCount(hDS);
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    // 验证波段索引
    for (int i = 0; i < newBandCount; i++) {
        if (bandOrder[i] < 1 || bandOrder[i] > srcBandCount) {
            return NULL;
        }
    }
    GDALDataType dataType = GDALGetRasterDataType(GDALGetRasterBand(hDS, bandOrder[0]));
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) return NULL;
    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, newBandCount, dataType, NULL);
    if (hNewDS == NULL) return NULL;
    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }
    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }
    // 按新顺序复制波段
    for (int i = 0; i < newBandCount; i++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, bandOrder[i]);
        GDALRasterBandH dstBand = GDALGetRasterBand(hNewDS, i + 1);
        GDALDataType srcType = GDALGetRasterDataType(srcBand);
        int typeSize = GDALGetDataTypeSizeBytes(srcType);
        void* buffer = CPLMalloc(width * height * typeSize);
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        CPLFree(buffer);
        // 【关键】复制颜色解释
        GDALSetRasterColorInterpretation(dstBand, GDALGetRasterColorInterpretation(srcBand));
        // 复制NoData
        int hasNoData = 0;
        double noData = GDALGetRasterNoDataValue(srcBand, &hasNoData);
        if (hasNoData) {
            GDALSetRasterNoDataValue(dstBand, noData);
        }
        // 复制调色板
        GDALColorTableH colorTable = GDALGetRasterColorTable(srcBand);
        if (colorTable != NULL) {
            GDALSetRasterColorTable(dstBand, GDALCloneColorTable(colorTable));
        }
        // 复制描述
        const char* desc = GDALGetDescription(srcBand);
        if (desc != NULL && strlen(desc) > 0) {
            GDALSetDescription(dstBand, desc);
        }
        // 复制元数据
        char** metadata = GDALGetMetadata(srcBand, NULL);
        if (metadata != NULL) {
            GDALSetMetadata(dstBand, metadata, NULL);
        }
    }
    return hNewDS;
}
GDALDatasetH convertBandDataType(GDALDatasetH hDS, int bandIndex, BandDataType newType) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return NULL;
    }
    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);
    GDALDataType gdalNewType = bandDataTypeToGDAL(newType);
    // 使用第一个波段的数据类型作为基础（除非是要转换的波段）
    GDALDataType baseType = (bandIndex == 1) ? gdalNewType :
                            GDALGetRasterDataType(GDALGetRasterBand(hDS, 1));
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) return NULL;
    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, bandCount, baseType, NULL);
    if (hNewDS == NULL) return NULL;
    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }
    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }
    // 复制所有波段
    for (int i = 1; i <= bandCount; i++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, i);
        GDALRasterBandH dstBand = GDALGetRasterBand(hNewDS, i);
        GDALDataType srcType = GDALGetRasterDataType(srcBand);
        GDALDataType dstType = (i == bandIndex) ? gdalNewType : srcType;
        // 使用double作为中间类型进行转换
        double* buffer = (double*)CPLMalloc(width * height * sizeof(double));
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Float64, 0, 0);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Float64, 0, 0);
        CPLFree(buffer);
        // 【关键】复制颜色解释
        GDALSetRasterColorInterpretation(dstBand, GDALGetRasterColorInterpretation(srcBand));
        // 复制NoData
        int hasNoData = 0;
        double noData = GDALGetRasterNoDataValue(srcBand, &hasNoData);
        if (hasNoData) {
            GDALSetRasterNoDataValue(dstBand, noData);
        }
        // 复制调色板（仅对非转换波段）
        if (i != bandIndex) {
            GDALColorTableH colorTable = GDALGetRasterColorTable(srcBand);
            if (colorTable != NULL) {
                GDALSetRasterColorTable(dstBand, GDALCloneColorTable(colorTable));
            }
        }
        // 复制描述
        const char* desc = GDALGetDescription(srcBand);
        if (desc != NULL && strlen(desc) > 0) {
            GDALSetDescription(dstBand, desc);
        }
        // 复制元数据
        char** metadata = GDALGetMetadata(srcBand, NULL);
        if (metadata != NULL) {
            GDALSetMetadata(dstBand, metadata, NULL);
        }
    }
    return hNewDS;
}
// ==================== 调色板操作函数 ====================

PaletteInfo* getPaletteInfo(GDALDatasetH hDS, int bandIndex) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return NULL;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return NULL;

    GDALColorTableH hColorTable = GDALGetRasterColorTable(hBand);
    if (hColorTable == NULL) return NULL;

    PaletteInfo* info = (PaletteInfo*)CPLMalloc(sizeof(PaletteInfo));
    if (info == NULL) return NULL;

    info->entryCount = GDALGetColorEntryCount(hColorTable);
    info->interpType = GDALGetPaletteInterpretation(hColorTable);

    if (info->entryCount > 0) {
        info->entries = (PaletteEntry*)CPLMalloc(sizeof(PaletteEntry) * info->entryCount);
        if (info->entries == NULL) {
            CPLFree(info);
            return NULL;
        }

        for (int i = 0; i < info->entryCount; i++) {
            const GDALColorEntry* entry = GDALGetColorEntry(hColorTable, i);
            if (entry != NULL) {
                info->entries[i].c1 = entry->c1;
                info->entries[i].c2 = entry->c2;
                info->entries[i].c3 = entry->c3;
                info->entries[i].c4 = entry->c4;
            }
        }
    } else {
        info->entries = NULL;
    }

    return info;
}

void freePaletteInfo(PaletteInfo* info) {
    if (info != NULL) {
        if (info->entries != NULL) {
            CPLFree(info->entries);
        }
        CPLFree(info);
    }
}

GDALColorTableH createColorTable(GDALPaletteInterp interpType) {
    return GDALCreateColorTable(interpType);
}

int addPaletteEntry(GDALColorTableH hTable, int index, short c1, short c2, short c3, short c4) {
    if (hTable == NULL || index < 0) return 0;

    GDALColorEntry entry;
    entry.c1 = c1;
    entry.c2 = c2;
    entry.c3 = c3;
    entry.c4 = c4;

    GDALSetColorEntry(hTable, index, &entry);
    return 1;
}

int setBandColorTable(GDALDatasetH hDS, int bandIndex, GDALColorTableH hTable) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

    CPLErr err = GDALSetRasterColorTable(hBand, hTable);
    return (err == CE_None) ? 1 : 0;
}

int deleteBandColorTable(GDALDatasetH hDS, int bandIndex) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

    CPLErr err = GDALSetRasterColorTable(hBand, NULL);
    return (err == CE_None) ? 1 : 0;
}

int modifyPaletteEntry(GDALDatasetH hDS, int bandIndex, int entryIndex,
                       short c1, short c2, short c3, short c4) {
    if (hDS == NULL || bandIndex < 1 || bandIndex > GDALGetRasterCount(hDS)) {
        return 0;
    }

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, bandIndex);
    if (hBand == NULL) return 0;

    GDALColorTableH hColorTable = GDALGetRasterColorTable(hBand);
    if (hColorTable == NULL) return 0;

    if (entryIndex < 0 || entryIndex >= GDALGetColorEntryCount(hColorTable)) {
        return 0;
    }

    GDALColorEntry entry;
    entry.c1 = c1;
    entry.c2 = c2;
    entry.c3 = c3;
    entry.c4 = c4;

    GDALSetColorEntry(hColorTable, entryIndex, &entry);
    return 1;
}

GDALDatasetH paletteToRGB(GDALDatasetH hDS) {
    if (hDS == NULL) return NULL;

    int bandCount = GDALGetRasterCount(hDS);
    if (bandCount < 1) return NULL;

    GDALRasterBandH hBand = GDALGetRasterBand(hDS, 1);
    GDALColorTableH hColorTable = GDALGetRasterColorTable(hBand);

    if (hColorTable == NULL) {
        // 不是调色板图像，直接返回NULL
        return NULL;
    }

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);

    // 创建RGB数据集
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) return NULL;

    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, 4, GDT_Byte, NULL);
    if (hNewDS == NULL) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }

    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }

    // 读取调色板索引
    unsigned char* indexBuffer = (unsigned char*)CPLMalloc(width * height);
    GDALRasterIO(hBand, GF_Read, 0, 0, width, height, indexBuffer, width, height, GDT_Byte, 0, 0);

    // 分配RGBA缓冲区
    unsigned char* redBuffer = (unsigned char*)CPLMalloc(width * height);
    unsigned char* greenBuffer = (unsigned char*)CPLMalloc(width * height);
    unsigned char* blueBuffer = (unsigned char*)CPLMalloc(width * height);
    unsigned char* alphaBuffer = (unsigned char*)CPLMalloc(width * height);

    int colorCount = GDALGetColorEntryCount(hColorTable);

    // 转换
    for (int i = 0; i < width * height; i++) {
        int idx = indexBuffer[i];
        if (idx < colorCount) {
            const GDALColorEntry* entry = GDALGetColorEntry(hColorTable, idx);
            if (entry != NULL) {
                redBuffer[i] = (unsigned char)entry->c1;
                greenBuffer[i] = (unsigned char)entry->c2;
                blueBuffer[i] = (unsigned char)entry->c3;
                alphaBuffer[i] = (unsigned char)entry->c4;
            } else {
                redBuffer[i] = greenBuffer[i] = blueBuffer[i] = 0;
                alphaBuffer[i] = 255;
            }
        } else {
            redBuffer[i] = greenBuffer[i] = blueBuffer[i] = 0;
            alphaBuffer[i] = 255;
        }
    }

    // 写入各波段
    GDALRasterBandH redBand = GDALGetRasterBand(hNewDS, 1);
    GDALRasterBandH greenBand = GDALGetRasterBand(hNewDS, 2);
    GDALRasterBandH blueBand = GDALGetRasterBand(hNewDS, 3);
    GDALRasterBandH alphaBand = GDALGetRasterBand(hNewDS, 4);

    GDALRasterIO(redBand, GF_Write, 0, 0, width, height, redBuffer, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(greenBand, GF_Write, 0, 0, width, height, greenBuffer, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(blueBand, GF_Write, 0, 0, width, height, blueBuffer, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(alphaBand, GF_Write, 0, 0, width, height, alphaBuffer, width, height, GDT_Byte, 0, 0);

    GDALSetRasterColorInterpretation(redBand, GCI_RedBand);
    GDALSetRasterColorInterpretation(greenBand, GCI_GreenBand);
    GDALSetRasterColorInterpretation(blueBand, GCI_BlueBand);
    GDALSetRasterColorInterpretation(alphaBand, GCI_AlphaBand);

    CPLFree(indexBuffer);
    CPLFree(redBuffer);
    CPLFree(greenBuffer);
    CPLFree(blueBuffer);
    CPLFree(alphaBuffer);

    return hNewDS;
}

GDALDatasetH rgbToPalette(GDALDatasetH hDS, int colorCount) {
    if (hDS == NULL || colorCount <= 0 || colorCount > 256) return NULL;

    int bandCount = GDALGetRasterCount(hDS);
    if (bandCount < 3) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);

    // 读取RGB数据
    unsigned char* redBuffer = (unsigned char*)CPLMalloc(width * height);
    unsigned char* greenBuffer = (unsigned char*)CPLMalloc(width * height);
    unsigned char* blueBuffer = (unsigned char*)CPLMalloc(width * height);

    GDALRasterIO(GDALGetRasterBand(hDS, 1), GF_Read, 0, 0, width, height,
                 redBuffer, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 2), GF_Read, 0, 0, width, height,
                 greenBuffer, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 3), GF_Read, 0, 0, width, height,
                 blueBuffer, width, height, GDT_Byte, 0, 0);

    // 使用GDAL的中值切割算法进行颜色量化
    GDALColorTableH hColorTable = GDALCreateColorTable(GPI_RGB);
    unsigned char* indexBuffer = (unsigned char*)CPLMalloc(width * height);

    // 简单的颜色量化（可以替换为更复杂的算法）
    int* colorHistogram = (int*)CPLCalloc(256 * 256 * 256 / 64, sizeof(int));

    // 统计颜色（降低精度以减少颜色数）
    for (int i = 0; i < width * height; i++) {
        int r = redBuffer[i] >> 2;
        int g = greenBuffer[i] >> 2;
        int b = blueBuffer[i] >> 2;
        int idx = (r << 12) | (g << 6) | b;
        colorHistogram[idx]++;
    }

    // 找出最常用的颜色
    typedef struct {
        int count;
        int r, g, b;
    } ColorFreq;

    ColorFreq* colors = (ColorFreq*)CPLMalloc(sizeof(ColorFreq) * 262144);
    int uniqueColors = 0;

    for (int i = 0; i < 262144; i++) {
        if (colorHistogram[i] > 0) {
            colors[uniqueColors].count = colorHistogram[i];
            colors[uniqueColors].r = ((i >> 12) & 0x3F) << 2;
            colors[uniqueColors].g = ((i >> 6) & 0x3F) << 2;
            colors[uniqueColors].b = (i & 0x3F) << 2;
            uniqueColors++;
        }
    }

    // 按频率排序（简单冒泡排序，实际应用中应使用快速排序）
    for (int i = 0; i < uniqueColors - 1 && i < colorCount; i++) {
        for (int j = i + 1; j < uniqueColors; j++) {
            if (colors[j].count > colors[i].count) {
                ColorFreq temp = colors[i];
                colors[i] = colors[j];
                colors[j] = temp;
            }
        }
    }

    // 创建调色板
    int paletteSize = (uniqueColors < colorCount) ? uniqueColors : colorCount;
    for (int i = 0; i < paletteSize; i++) {
        GDALColorEntry entry;
        entry.c1 = colors[i].r;
        entry.c2 = colors[i].g;
        entry.c3 = colors[i].b;
        entry.c4 = 255;
        GDALSetColorEntry(hColorTable, i, &entry);
    }

    // 将像素映射到最近的调色板颜色
    for (int i = 0; i < width * height; i++) {
        int r = redBuffer[i];
        int g = greenBuffer[i];
        int b = blueBuffer[i];

        int bestIdx = 0;
        int bestDist = INT_MAX;

        for (int j = 0; j < paletteSize; j++) {
            int dr = r - colors[j].r;
            int dg = g - colors[j].g;
            int db = b - colors[j].b;
            int dist = dr * dr + dg * dg + db * db;

            if (dist < bestDist) {
                bestDist = dist;
                bestIdx = j;
            }
        }

        indexBuffer[i] = (unsigned char)bestIdx;
    }

    // 创建调色板数据集
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, 1, GDT_Byte, NULL);

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }

    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }

    // 写入数据
    GDALRasterBandH hBand = GDALGetRasterBand(hNewDS, 1);
    GDALRasterIO(hBand, GF_Write, 0, 0, width, height, indexBuffer, width, height, GDT_Byte, 0, 0);
    GDALSetRasterColorInterpretation(hBand, GCI_PaletteIndex);
    GDALSetRasterColorTable(hBand, hColorTable);

    // 清理
    CPLFree(redBuffer);
    CPLFree(greenBuffer);
    CPLFree(blueBuffer);
    CPLFree(indexBuffer);
    CPLFree(colorHistogram);
    CPLFree(colors);
    GDALDestroyColorTable(hColorTable);

    return hNewDS;
}

// 强制修改颜色解释（创建新的内存数据集）
GDALDatasetH setBandColorInterpretationForced(GDALDatasetH hDS, int bandIndex, ColorInterpretation colorInterp) {
    if (hDS == NULL) return NULL;

    int bandCount = GDALGetRasterCount(hDS);
    if (bandIndex < 1 || bandIndex > bandCount) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);

    // 获取第一个波段的数据类型
    GDALDataType dataType = GDALGetRasterDataType(GDALGetRasterBand(hDS, 1));

    // 创建新的内存数据集
    GDALDriverH hDriver = GDALGetDriverByName("MEM");
    if (hDriver == NULL) return NULL;

    GDALDatasetH hNewDS = GDALCreate(hDriver, "", width, height, bandCount, dataType, NULL);
    if (hNewDS == NULL) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(hNewDS, geoTransform);
    }
    const char* projection = GDALGetProjectionRef(hDS);
    if (projection != NULL && strlen(projection) > 0) {
        GDALSetProjection(hNewDS, projection);
    }

    // 复制所有波段
    for (int i = 1; i <= bandCount; i++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, i);
        GDALRasterBandH dstBand = GDALGetRasterBand(hNewDS, i);

        GDALDataType srcType = GDALGetRasterDataType(srcBand);
        int typeSize = GDALGetDataTypeSizeBytes(srcType);

        // 复制数据
        void* buffer = CPLMalloc((size_t)width * (size_t)height * typeSize);
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, srcType, 0, 0);
        CPLFree(buffer);

        // 【关键】设置颜色解释
        if (i == bandIndex) {
            // 目标波段：设置新的颜色解释
            GDALSetRasterColorInterpretation(dstBand, colorInterpToGDAL(colorInterp));
        } else {
            // 其他波段：保持原有颜色解释
            GDALSetRasterColorInterpretation(dstBand, GDALGetRasterColorInterpretation(srcBand));
        }

        // 复制NoData
        int hasNoData = 0;
        double noData = GDALGetRasterNoDataValue(srcBand, &hasNoData);
        if (hasNoData) {
            GDALSetRasterNoDataValue(dstBand, noData);
        }

        // 复制调色板
        GDALColorTableH colorTable = GDALGetRasterColorTable(srcBand);
        if (colorTable != NULL) {
            GDALSetRasterColorTable(dstBand, GDALCloneColorTable(colorTable));
        }
    }

    return hNewDS;
}
