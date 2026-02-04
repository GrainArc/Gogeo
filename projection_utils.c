// projection_utils.c
#include "osgeo_utils.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/**
 * 为栅格数据定义投影（不改变像素数据，仅设置坐标系信息）
 */
int defineProjection(const char* inputPath, const char* outputPath, int epsgCode) {
    if (!inputPath || !outputPath || epsgCode <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Invalid input parameters");
        return 0;
    }

    GDALDatasetH hSrcDS = GDALOpen(inputPath, GA_ReadOnly);
    if (hSrcDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to open source dataset: %s", inputPath);
        return 0;
    }

    // 创建空间参考对象
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    if (OSRImportFromEPSG(hSRS, epsgCode) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to import EPSG:%d", epsgCode);
        OSRDestroySpatialReference(hSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    char *pszWKT = NULL;
    if (OSRExportToWkt(hSRS, &pszWKT) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to export WKT");
        OSRDestroySpatialReference(hSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 使用创建副本的方式
    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "GTiff driver not available");
        OSRDestroySpatialReference(hSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 创建输出数据集（直接复制）
    GDALDatasetH hDstDS = GDALCreateCopy(hDriver, outputPath, hSrcDS,
                                         FALSE, NULL, NULL, NULL);
    if (hDstDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to create output dataset: %s", outputPath);
        OSRDestroySpatialReference(hSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 设置投影信息
    if (GDALSetProjection(hDstDS, pszWKT) != CE_None) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to set projection");
        GDALClose(hDstDS);
        GDALClose(hSrcDS);
        OSRDestroySpatialReference(hSRS);
        return 0;
    }

    // 如果原数据集没有地理变换，设置默认值
    double adfGeoTransform[6];
    if (GDALGetGeoTransform(hDstDS, adfGeoTransform) != CE_None) {
        // 设置默认地理变换（像素坐标）
        adfGeoTransform[0] = 0.0;      // 左上角X
        adfGeoTransform[1] = 1.0;      // 像素宽度
        adfGeoTransform[2] = 0.0;      // 旋转
        adfGeoTransform[3] = 0.0;      // 左上角Y
        adfGeoTransform[4] = 0.0;      // 旋转
        adfGeoTransform[5] = -1.0;     // 像素高度
        GDALSetGeoTransform(hDstDS, adfGeoTransform);
    }

    GDALFlushCache(hDstDS);
    GDALClose(hDstDS);
    GDALClose(hSrcDS);
    OSRDestroySpatialReference(hSRS);

    CPLFree(pszWKT);

    return 1;
}

/**
 * 重投影栅格数据到目标坐标系
 */
int reprojectionRaster(const char* inputPath, const char* outputPath,
                       int targetEpsgCode, int resampleMethod) {
    if (!inputPath || !outputPath || targetEpsgCode <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Invalid input parameters");
        return 0;
    }

    GDALDatasetH hSrcDS = GDALOpen(inputPath, GA_ReadOnly);
    if (hSrcDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to open source dataset: %s", inputPath);
        return 0;
    }

    // 检查源数据集是否有投影信息
    const char *pszSrcWKT = GDALGetProjectionRef(hSrcDS);
    if (pszSrcWKT == NULL || strlen(pszSrcWKT) == 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Source dataset has no projection information");
        GDALClose(hSrcDS);
        return 0;
    }

    // 创建目标空间参考对象
    OGRSpatialReferenceH hDstSRS = OSRNewSpatialReference(NULL);
    if (OSRImportFromEPSG(hDstSRS, targetEpsgCode) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to import target EPSG:%d", targetEpsgCode);
        OSRDestroySpatialReference(hDstSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    char *pszDstWKT = NULL;
    if (OSRExportToWkt(hDstSRS, &pszDstWKT) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to export target WKT");
        OSRDestroySpatialReference(hDstSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 设置重采样方法
    GDALResampleAlg eResampleAlg = GRA_Bilinear;  // 默认双线性
    switch (resampleMethod) {
        case 0:
            eResampleAlg = GRA_NearestNeighbour;
            break;
        case 1:
            eResampleAlg = GRA_Bilinear;
            break;
        case 2:
            eResampleAlg = GRA_Cubic;
            break;
        case 3:
            eResampleAlg = GRA_CubicSpline;
            break;
        case 4:
            eResampleAlg = GRA_Lanczos;
            break;
        default:
            eResampleAlg = GRA_Bilinear;
    }

    // 执行重投影
    GDALDatasetH hWarpedDS = GDALAutoCreateWarpedVRT(hSrcDS, pszSrcWKT, pszDstWKT,
                                                      eResampleAlg, 1.0, NULL);
    if (hWarpedDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to create warped VRT");
        OSRDestroySpatialReference(hDstSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 获取输出数据集的尺寸
    int nDstXSize = GDALGetRasterXSize(hWarpedDS);
    int nDstYSize = GDALGetRasterYSize(hWarpedDS);

    // 创建输出文件
    GDALDriverH hDriver = GDALGetDriverByName("GTiff");
    if (hDriver == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "GTiff driver not available");
        GDALClose(hWarpedDS);
        OSRDestroySpatialReference(hDstSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    GDALDatasetH hDstDS = GDALCreate(hDriver, outputPath, nDstXSize, nDstYSize,
                                     GDALGetRasterCount(hWarpedDS),
                                     GDALGetRasterDataType(GDALGetRasterBand(hWarpedDS, 1)),
                                     NULL);
    if (hDstDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to create output dataset: %s", outputPath);
        GDALClose(hWarpedDS);
        OSRDestroySpatialReference(hDstSRS);
        GDALClose(hSrcDS);
        return 0;
    }

    // 设置投影和地理变换
    GDALSetProjection(hDstDS, pszDstWKT);
    double adfGeoTransform[6];
    if (GDALGetGeoTransform(hWarpedDS, adfGeoTransform) == CE_None) {
        GDALSetGeoTransform(hDstDS, adfGeoTransform);
    }

    // 复制数据
    int nBands = GDALGetRasterCount(hWarpedDS);
    for (int i = 1; i <= nBands; i++) {
        GDALRasterBandH hSrcBand = GDALGetRasterBand(hWarpedDS, i);
        GDALRasterBandH hDstBand = GDALGetRasterBand(hDstDS, i);

        GDALDataType eDataType = GDALGetRasterDataType(hSrcBand);
        int nBlockXSize, nBlockYSize;
        GDALGetBlockSize(hSrcBand, &nBlockXSize, &nBlockYSize);

        void *pData = CPLMalloc(nBlockXSize * nBlockYSize *
                               GDALGetDataTypeSize(eDataType) / 8);

        for (int iY = 0; iY < nDstYSize; iY += nBlockYSize) {
            int nThisYSize = (iY + nBlockYSize > nDstYSize) ?
                            (nDstYSize - iY) : nBlockYSize;

            for (int iX = 0; iX < nDstXSize; iX += nBlockXSize) {
                int nThisXSize = (iX + nBlockXSize > nDstXSize) ?
                                (nDstXSize - iX) : nBlockXSize;

                if (GDALRasterIO(hSrcBand, GF_Read, iX, iY, nThisXSize, nThisYSize,
                                pData, nThisXSize, nThisYSize, eDataType, 0, 0) == CE_None) {
                    GDALRasterIO(hDstBand, GF_Write, iX, iY, nThisXSize, nThisYSize,
                                pData, nThisXSize, nThisYSize, eDataType, 0, 0);
                }
            }
        }

        CPLFree(pData);

        // 复制NoData值
        int bHasNoData = 0;
        double dfNoData = GDALGetRasterNoDataValue(hSrcBand, &bHasNoData);
        if (bHasNoData) {
            GDALSetRasterNoDataValue(hDstBand, dfNoData);
        }
    }

    GDALFlushCache(hDstDS);
    GDALClose(hDstDS);
    GDALClose(hWarpedDS);
    GDALClose(hSrcDS);
    OSRDestroySpatialReference(hDstSRS);
    CPLFree(pszDstWKT);

    return 1;
}

/**
 * 重投影栅格数据到目标坐标系（支持直接覆盖）
 */
int reprojectionRasterInPlace(const char* inputPath, int targetEpsgCode,
                              int resampleMethod, const char* tempDir) {
    if (!inputPath || targetEpsgCode <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Invalid input parameters");
        return 0;
    }

    // 构建临时文件路径
    char szTempPath[2048];
    if (tempDir == NULL || strlen(tempDir) == 0) {
        // 使用系统临时目录
        const char *pszTemp = CPLGetBasename(inputPath);
        snprintf(szTempPath, sizeof(szTempPath), "/tmp/gdal_temp_%s.tif", pszTemp);
    } else {
        const char *pszBasename = CPLGetBasename(inputPath);
        snprintf(szTempPath, sizeof(szTempPath), "%s/gdal_temp_%s.tif", tempDir, pszBasename);
    }

    // 执行重投影到临时文件
    if (!reprojectionRaster(inputPath, szTempPath, targetEpsgCode, resampleMethod)) {
        CPLError(CE_Failure, CPLE_AppDefined, "Reprojection failed");
        return 0;
    }

    // 删除原文件并用临时文件替换
    if (unlink(inputPath) != 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to delete original file");
        unlink(szTempPath);
        return 0;
    }

    if (rename(szTempPath, inputPath) != 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to rename temporary file");
        return 0;
    }

    return 1;
}

/**
 * 定义投影（直接修改文件）
 */
int defineProjectionInPlace(const char* filePath, int epsgCode) {
    if (!filePath || epsgCode <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Invalid input parameters");
        return 0;
    }

    // 以读写模式打开数据集
    GDALDatasetH hDS = GDALOpen(filePath, GA_Update);
    if (hDS == NULL) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to open dataset: %s", filePath);
        return 0;
    }

    // 创建空间参考对象
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    if (OSRImportFromEPSG(hSRS, epsgCode) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to import EPSG:%d", epsgCode);
        OSRDestroySpatialReference(hSRS);
        GDALClose(hDS);
        return 0;
    }

    char *pszWKT = NULL;
    if (OSRExportToWkt(hSRS, &pszWKT) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to export WKT");
        OSRDestroySpatialReference(hSRS);
        GDALClose(hDS);
        return 0;
    }

    // 设置投影信息
    if (GDALSetProjection(hDS, pszWKT) != CE_None) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to set projection");
        GDALClose(hDS);
        OSRDestroySpatialReference(hSRS);
        CPLFree(pszWKT);
        return 0;
    }

    // 如果没有地理变换，设置默认值
    double adfGeoTransform[6];
    if (GDALGetGeoTransform(hDS, adfGeoTransform) != CE_None) {
        adfGeoTransform[0] = 0.0;
        adfGeoTransform[1] = 1.0;
        adfGeoTransform[2] = 0.0;
        adfGeoTransform[3] = 0.0;
        adfGeoTransform[4] = 0.0;
        adfGeoTransform[5] = -1.0;
        GDALSetGeoTransform(hDS, adfGeoTransform);
    }

    GDALFlushCache(hDS);
    GDALClose(hDS);
    OSRDestroySpatialReference(hSRS);
    CPLFree(pszWKT);

    return 1;
}
