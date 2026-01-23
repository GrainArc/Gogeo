package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
#include <string.h>
typedef struct {
    unsigned char* data;
    size_t size;
    int success;
} SerializeResult;
// 创建几何对象表示矩形边界
OGRGeometryH createBoundingBoxGeometry(double minX, double minY, double maxX, double maxY) {
    OGRGeometryH hRing = OGR_G_CreateGeometry(wkbLinearRing);
    if (!hRing) return NULL;

    // 创建矩形的5个点（闭合）
    OGR_G_AddPoint_2D(hRing, minX, minY);
    OGR_G_AddPoint_2D(hRing, maxX, minY);
    OGR_G_AddPoint_2D(hRing, maxX, maxY);
    OGR_G_AddPoint_2D(hRing, minX, maxY);
    OGR_G_AddPoint_2D(hRing, minX, minY);

    // 创建多边形
    OGRGeometryH hPolygon = OGR_G_CreateGeometry(wkbPolygon);
    if (!hPolygon) {
        OGR_G_DestroyGeometry(hRing);
        return NULL;
    }

    OGR_G_AddGeometry(hPolygon, hRing);
    OGR_G_DestroyGeometry(hRing);

    return hPolygon;
}

// 创建包含所有瓦片范围的联合边界
OGRGeometryH createUnionBoundingBox(double* minXs, double* minYs, double* maxXs, double* maxYs, int tileCount) {
    if (tileCount == 0) return NULL;

    // 计算所有瓦片的总边界
    double unionMinX = minXs[0];
    double unionMinY = minYs[0];
    double unionMaxX = maxXs[0];
    double unionMaxY = maxYs[0];

    for (int i = 1; i < tileCount; i++) {
        if (minXs[i] < unionMinX) unionMinX = minXs[i];
        if (minYs[i] < unionMinY) unionMinY = minYs[i];
        if (maxXs[i] > unionMaxX) unionMaxX = maxXs[i];
        if (maxYs[i] > unionMaxY) unionMaxY = maxYs[i];
    }

    return createBoundingBoxGeometry(unionMinX, unionMinY, unionMaxX, unionMaxY);
}



// 检查点是否在矩形边界内
int isPointInBounds(double x, double y, double minX, double minY, double maxX, double maxY) {
    return (x >= minX && x <= maxX && y >= minY && y <= maxY);
}

// 检查几何对象是否与边界相交
int geometryIntersectsBounds(OGRGeometryH hGeom, double minX, double minY, double maxX, double maxY) {
    if (!hGeom) return 0;

    OGRGeometryH hBounds = createBoundingBoxGeometry(minX, minY, maxX, maxY);
    if (!hBounds) return 0;

    int intersects = OGR_G_Intersects(hGeom, hBounds);
    OGR_G_DestroyGeometry(hBounds);

    return intersects;
}
// 序列化单个要素（完全重写版本）
int serializeFeature(OGRFeatureH hFeature, unsigned char** buffer, size_t* size) {
    if (!hFeature || !buffer || !size) return 0;

    OGRFeatureDefnH hDefn = OGR_F_GetDefnRef(hFeature);
    if (!hDefn) return 0;

    size_t totalSize = 0;
    int fieldCount = OGR_FD_GetFieldCount(hDefn);

    // 1. 要素ID (8字节)
    totalSize += sizeof(GIntBig);

    // 2. 字段数量 (4字节)
    totalSize += sizeof(int);

    // 3. 为每个字段分配空间（无论是否设置）
    for (int i = 0; i < fieldCount; i++) {
        totalSize += sizeof(int); // 字段设置标志
        totalSize += sizeof(int); // 字段类型

        if (OGR_F_IsFieldSet(hFeature, i)) {
            totalSize += sizeof(int); // 数据长度

            OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hDefn, i);
            OGRFieldType fieldType = OGR_Fld_GetType(hFieldDefn);

            switch (fieldType) {
                case OFTInteger:
                    totalSize += sizeof(int);
                    break;
                case OFTInteger64:
                    totalSize += sizeof(GIntBig);
                    break;
                case OFTReal:
                    totalSize += sizeof(double);
                    break;
                case OFTString: {
                    const char* str = OGR_F_GetFieldAsString(hFeature, i);
                    totalSize += strlen(str);  // 不包含null终止符
                    break;
                }
                case OFTBinary: {
                    int binarySize;
                    OGR_F_GetFieldAsBinary(hFeature, i, &binarySize);
                    totalSize += binarySize;
                    break;
                }
                default:
                    break;
            }
        }
    }

    // 4. 几何数据
    OGRGeometryH hGeom = OGR_F_GetGeometryRef(hFeature);
    int wkbSize = 0;
    if (hGeom) {
        wkbSize = OGR_G_WkbSize(hGeom);
    }
    totalSize += sizeof(int) + wkbSize;

    // 分配缓冲区
    *buffer = (unsigned char*)malloc(totalSize);
    if (!*buffer) return 0;

    unsigned char* ptr = *buffer;
    *size = totalSize;

    // 写入数据
    // 1. 要素ID
    GIntBig fid = OGR_F_GetFID(hFeature);
    memcpy(ptr, &fid, sizeof(GIntBig));
    ptr += sizeof(GIntBig);

    // 2. 字段数量
    memcpy(ptr, &fieldCount, sizeof(int));
    ptr += sizeof(int);

    // 3. 处理所有字段（按顺序）
    for (int i = 0; i < fieldCount; i++) {
        int isSet = OGR_F_IsFieldSet(hFeature, i) ? 1 : 0;
        OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hDefn, i);
        OGRFieldType fieldType = OGR_Fld_GetType(hFieldDefn);

        // 写入字段设置标志
        memcpy(ptr, &isSet, sizeof(int));
        ptr += sizeof(int);

        // 写入字段类型
        memcpy(ptr, &fieldType, sizeof(int));
        ptr += sizeof(int);

        if (isSet) {
            // 写入数据长度和数据
            switch (fieldType) {
                case OFTInteger: {
                    int value = OGR_F_GetFieldAsInteger(hFeature, i);
                    int dataSize = sizeof(int);
                    memcpy(ptr, &dataSize, sizeof(int));
                    ptr += sizeof(int);
                    memcpy(ptr, &value, sizeof(int));
                    ptr += sizeof(int);
                    break;
                }
                case OFTInteger64: {
                    GIntBig value = OGR_F_GetFieldAsInteger64(hFeature, i);
                    int dataSize = sizeof(GIntBig);
                    memcpy(ptr, &dataSize, sizeof(int));
                    ptr += sizeof(int);
                    memcpy(ptr, &value, sizeof(GIntBig));
                    ptr += sizeof(GIntBig);
                    break;
                }
                case OFTReal: {
                    double value = OGR_F_GetFieldAsDouble(hFeature, i);
                    int dataSize = sizeof(double);
                    memcpy(ptr, &dataSize, sizeof(int));
                    ptr += sizeof(int);
                    memcpy(ptr, &value, sizeof(double));
                    ptr += sizeof(double);
                    break;
                }
                case OFTString: {
                    const char* str = OGR_F_GetFieldAsString(hFeature, i);
                    int strLen = strlen(str);  // 不包含null终止符
                    memcpy(ptr, &strLen, sizeof(int));
                    ptr += sizeof(int);
                    if (strLen > 0) {
                        memcpy(ptr, str, strLen);
                        ptr += strLen;
                    }
                    break;
                }
                case OFTBinary: {
                    int binarySize;
                    GByte* binaryData = OGR_F_GetFieldAsBinary(hFeature, i, &binarySize);
                    memcpy(ptr, &binarySize, sizeof(int));
                    ptr += sizeof(int);
                    if (binarySize > 0) {
                        memcpy(ptr, binaryData, binarySize);
                        ptr += binarySize;
                    }
                    break;
                }
                default: {
                    // 未知类型，写入0长度
                    int zeroSize = 0;
                    memcpy(ptr, &zeroSize, sizeof(int));
                    ptr += sizeof(int);
                    break;
                }
            }
        }
        // 如果字段未设置，不写入数据长度和数据部分
    }

    // 4. 几何数据
    memcpy(ptr, &wkbSize, sizeof(int));
    ptr += sizeof(int);

    if (hGeom && wkbSize > 0) {
        OGRErr err = OGR_G_ExportToWkb(hGeom, wkbNDR, ptr);
        if (err != OGRERR_NONE) {
            free(*buffer);
            *buffer = NULL;
            return 0;
        }
        ptr += wkbSize;
    }

    return 1;
}


// 改进的序列化函数，增加性能优化
SerializeResult serializeLayerToBinary(OGRLayerH hLayer, int preallocSize) {
    SerializeResult result = {NULL, 0, 0};

    if (!hLayer) return result;

    // 获取图层信息
    OGRFeatureDefnH hDefn = OGR_L_GetLayerDefn(hLayer);
    if (!hDefn) return result;

    OGRSpatialReferenceH hSRS = OGR_L_GetSpatialRef(hLayer);
    OGRwkbGeometryType geomType = OGR_L_GetGeomType(hLayer);

    // 预估缓冲区大小
    size_t estimatedSize = preallocSize > 0 ? preallocSize : 1024 * 1024; // 默认1MB
    size_t bufferSize = estimatedSize;
    unsigned char* buffer = (unsigned char*)malloc(bufferSize);
    if (!buffer) return result;

    size_t currentPos = 0;

    // 写入魔数标识
    const char* magic = "GDALLYR2"; // 版本2
    if (currentPos + 8 > bufferSize) {
        bufferSize = bufferSize * 2;
        buffer = (unsigned char*)realloc(buffer, bufferSize);
        if (!buffer) return result;
    }
    memcpy(buffer + currentPos, magic, 8);
    currentPos += 8;

    // 写入版本号
    int version = 2;
    if (currentPos + sizeof(int) > bufferSize) {
        bufferSize = bufferSize * 2;
        buffer = (unsigned char*)realloc(buffer, bufferSize);
        if (!buffer) return result;
    }
    memcpy(buffer + currentPos, &version, sizeof(int));
    currentPos += sizeof(int);

    // 写入几何类型
    if (currentPos + sizeof(int) > bufferSize) {
        bufferSize = bufferSize * 2;
        buffer = (unsigned char*)realloc(buffer, bufferSize);
        if (!buffer) return result;
    }
    memcpy(buffer + currentPos, &geomType, sizeof(int));
    currentPos += sizeof(int);

    // 写入空间参考系统
    char* srsWKT = NULL;
    int srsWKTSize = 0;
    if (hSRS) {
        OSRExportToWkt(hSRS, &srsWKT);
        if (srsWKT) {
            srsWKTSize = strlen(srsWKT) + 1;
        }
    }

    if (currentPos + sizeof(int) + srsWKTSize > bufferSize) {
        size_t newSize = bufferSize;
        while (newSize < currentPos + sizeof(int) + srsWKTSize) {
            newSize *= 2;
        }
        buffer = (unsigned char*)realloc(buffer, newSize);
        if (!buffer) {
            if (srsWKT) CPLFree(srsWKT);
            return result;
        }
        bufferSize = newSize;
    }

    memcpy(buffer + currentPos, &srsWKTSize, sizeof(int));
    currentPos += sizeof(int);
    if (srsWKTSize > 0) {
        memcpy(buffer + currentPos, srsWKT, srsWKTSize);
        currentPos += srsWKTSize;
    }

    // 写入字段定义
    int fieldCount = OGR_FD_GetFieldCount(hDefn);
    if (currentPos + sizeof(int) > bufferSize) {
        bufferSize = bufferSize * 2;
        buffer = (unsigned char*)realloc(buffer, bufferSize);
        if (!buffer) {
            if (srsWKT) CPLFree(srsWKT);
            return result;
        }
    }
    memcpy(buffer + currentPos, &fieldCount, sizeof(int));
    currentPos += sizeof(int);

    // ... 字段定义写入逻辑（类似原始代码）
    for (int i = 0; i < fieldCount; i++) {
        OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hDefn, i);
        const char* fieldName = OGR_Fld_GetNameRef(hFieldDefn);
        OGRFieldType fieldType = OGR_Fld_GetType(hFieldDefn);
        int fieldWidth = OGR_Fld_GetWidth(hFieldDefn);
        int fieldPrecision = OGR_Fld_GetPrecision(hFieldDefn);

        int nameLen = strlen(fieldName) + 1;
        size_t fieldMetaSize = sizeof(int) * 4 + nameLen; // type + nameLen + name + width + precision

        if (currentPos + fieldMetaSize > bufferSize) {
            size_t newSize = bufferSize;
            while (newSize < currentPos + fieldMetaSize) {
                newSize *= 2;
            }
            buffer = (unsigned char*)realloc(buffer, newSize);
            if (!buffer) {
                if (srsWKT) CPLFree(srsWKT);
                return result;
            }
            bufferSize = newSize;
        }

        memcpy(buffer + currentPos, &fieldType, sizeof(int));
        currentPos += sizeof(int);
        memcpy(buffer + currentPos, &nameLen, sizeof(int));
        currentPos += sizeof(int);
        memcpy(buffer + currentPos, fieldName, nameLen);
        currentPos += nameLen;
        memcpy(buffer + currentPos, &fieldWidth, sizeof(int));
        currentPos += sizeof(int);
        memcpy(buffer + currentPos, &fieldPrecision, sizeof(int));
        currentPos += sizeof(int);
    }

    // 批量处理要素
    int featureCount = OGR_L_GetFeatureCount(hLayer, 1);
    if (currentPos + sizeof(int) > bufferSize) {
        bufferSize = bufferSize * 2;
        buffer = (unsigned char*)realloc(buffer, bufferSize);
        if (!buffer) {
            if (srsWKT) CPLFree(srsWKT);
            return result;
        }
    }
    memcpy(buffer + currentPos, &featureCount, sizeof(int));
    currentPos += sizeof(int);

    // 处理要素数据
    OGR_L_ResetReading(hLayer);
    OGRFeatureH hFeature;
    int processedFeatures = 0;

    while ((hFeature = OGR_L_GetNextFeature(hLayer)) != NULL) {
        // 序列化单个要素
        unsigned char* featureBuffer;
        size_t featureSize;

        if (serializeFeature(hFeature, &featureBuffer, &featureSize)) {
            // 确保缓冲区足够大
            if (currentPos + sizeof(size_t) + featureSize > bufferSize) {
                size_t newSize = bufferSize;
                while (newSize < currentPos + sizeof(size_t) + featureSize) {
                    newSize *= 2;
                }
                buffer = (unsigned char*)realloc(buffer, newSize);
                if (!buffer) {
                    free(featureBuffer);
                    OGR_F_Destroy(hFeature);
                    if (srsWKT) CPLFree(srsWKT);
                    return result;
                }
                bufferSize = newSize;
            }

            // 写入要素大小和数据
            memcpy(buffer + currentPos, &featureSize, sizeof(size_t));
            currentPos += sizeof(size_t);
            memcpy(buffer + currentPos, featureBuffer, featureSize);
            currentPos += featureSize;

            free(featureBuffer);
            processedFeatures++;
        }

        OGR_F_Destroy(hFeature);
    }

    if (srsWKT) CPLFree(srsWKT);

    result.data = buffer;
    result.size = currentPos;
    result.success = 1;

    return result;
}

// 按瓦片边界分组要素并序列化
int groupAndSerializeByTiles(OGRLayerH hClippedLayer, double* minXs, double* minYs,
                            double* maxXs, double* maxYs, int* tileIndices,
                            int tileCount, const char* outputDir, int bufferSize) {
    if (!hClippedLayer || tileCount == 0) return 0;

    // 为每个瓦片创建内存图层
    OGRSFDriverH hDriver = OGRGetDriverByName("MEM");
    if (!hDriver) return 0;

    OGRDataSourceH* tileDatasources = (OGRDataSourceH*)malloc(sizeof(OGRDataSourceH) * tileCount);
    OGRLayerH* tileLayers = (OGRLayerH*)malloc(sizeof(OGRLayerH) * tileCount);

    if (!tileDatasources || !tileLayers) {
        if (tileDatasources) free(tileDatasources);
        if (tileLayers) free(tileLayers);
        return 0;
    }

    // 获取源图层信息
    OGRSpatialReferenceH hSRS = OGR_L_GetSpatialRef(hClippedLayer);
    OGRwkbGeometryType geomType = OGR_L_GetGeomType(hClippedLayer);
    OGRFeatureDefnH hInputDefn = OGR_L_GetLayerDefn(hClippedLayer);

    // 为每个瓦片创建图层
    for (int i = 0; i < tileCount; i++) {
        tileDatasources[i] = OGR_Dr_CreateDataSource(hDriver, "", NULL);
        if (!tileDatasources[i]) continue;

        char layerName[64];
        snprintf(layerName, sizeof(layerName), "tile_%d", tileIndices[i]);

        tileLayers[i] = OGR_DS_CreateLayer(tileDatasources[i], layerName, hSRS, geomType, NULL);
        if (!tileLayers[i]) continue;

        // 复制字段定义
        int fieldCount = OGR_FD_GetFieldCount(hInputDefn);
        for (int j = 0; j < fieldCount; j++) {
            OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hInputDefn, j);
            OGR_L_CreateField(tileLayers[i], hFieldDefn, 1);
        }
    }

    // 遍历裁剪后图层的所有要素，分配到对应的瓦片图层
    OGR_L_ResetReading(hClippedLayer);
    OGRFeatureH hFeature;

    while ((hFeature = OGR_L_GetNextFeature(hClippedLayer)) != NULL) {
        OGRGeometryH hGeom = OGR_F_GetGeometryRef(hFeature);

        if (hGeom) {
            // 检查此要素属于哪些瓦片
            for (int i = 0; i < tileCount; i++) {
                if (geometryIntersectsBounds(hGeom, minXs[i], minYs[i], maxXs[i], maxYs[i])) {
                    if (tileLayers[i]) {
                        // 克隆要素并添加到对应瓦片图层
                        OGRFeatureH hClonedFeature = OGR_F_Clone(hFeature);
                        if (hClonedFeature) {
                            OGR_L_CreateFeature(tileLayers[i], hClonedFeature);
                            OGR_F_Destroy(hClonedFeature);
                        }
                    }
                }
            }
        }

        OGR_F_Destroy(hFeature);
    }

    // 序列化每个瓦片图层到bin文件
    int successCount = 0;
    for (int i = 0; i < tileCount; i++) {
        if (tileLayers[i]) {
            char outputPath[512];
            snprintf(outputPath, sizeof(outputPath), "%s/%d.bin", outputDir, tileIndices[i]);

            SerializeResult result = serializeLayerToBinary(tileLayers[i], bufferSize);
            if (result.success) {
                FILE* file = fopen(outputPath, "wb");
                if (file) {
                    size_t written = fwrite(result.data, 1, result.size, file);
                    fclose(file);
                    if (written == result.size) {
                        successCount++;
                    }
                }
                free(result.data);
            }
        }

        if (tileDatasources[i]) {
            OGR_DS_Destroy(tileDatasources[i]);
        }
    }

    free(tileDatasources);
    free(tileLayers);

    return successCount;
}
// 在C代码部分添加新函数

// 创建瓦片图层，包含clip_index字段
OGRLayerH createTileLayer(double* minXs, double* minYs, double* maxXs, double* maxYs,
                         int* tileIndices, int tileCount, OGRSpatialReferenceH hSRS) {
    // 创建内存数据源
    OGRSFDriverH hDriver = OGRGetDriverByName("MEM");
    if (!hDriver) return NULL;

    OGRDataSourceH hTileDS = OGR_Dr_CreateDataSource(hDriver, "tiles", NULL);
    if (!hTileDS) return NULL;

    // 创建瓦片图层
    OGRLayerH hTileLayer = OGR_DS_CreateLayer(hTileDS, "tiles", hSRS, wkbPolygon, NULL);
    if (!hTileLayer) {
        OGR_DS_Destroy(hTileDS);
        return NULL;
    }

    // 创建clip_index字段
    OGRFieldDefnH hFieldDefn = OGR_Fld_Create("clip_index", OFTInteger);
    if (OGR_L_CreateField(hTileLayer, hFieldDefn, 1) != OGRERR_NONE) {
        OGR_Fld_Destroy(hFieldDefn);
        OGR_DS_Destroy(hTileDS);
        return NULL;
    }
    OGR_Fld_Destroy(hFieldDefn);

    // 为每个瓦片创建要素
    for (int i = 0; i < tileCount; i++) {
        // 创建瓦片几何
        OGRGeometryH hTileGeom = createBoundingBoxGeometry(minXs[i], minYs[i], maxXs[i], maxYs[i]);
        if (!hTileGeom) continue;

        // 创建要素
        OGRFeatureDefnH hDefn = OGR_L_GetLayerDefn(hTileLayer);
        OGRFeatureH hFeature = OGR_F_Create(hDefn);

        // 设置几何
        OGR_F_SetGeometry(hFeature, hTileGeom);

        // 设置clip_index字段
        OGR_F_SetFieldInteger(hFeature, 0, tileIndices[i]);

        // 添加要素到图层
        OGR_L_CreateFeature(hTileLayer, hFeature);

        // 清理
        OGR_F_Destroy(hFeature);
        OGR_G_DestroyGeometry(hTileGeom);
    }

    return hTileLayer;
}

// 优化的裁剪和分组函数
int clipAndGroupByTilesOptimized(OGRLayerH hInputLayer, double* minXs, double* minYs,
                                double* maxXs, double* maxYs, int* tileIndices,
                                int tileCount, const char* outputDir, int bufferSize) {
    if (!hInputLayer || tileCount == 0) return 0;

    // 获取输入图层的空间参考
    OGRSpatialReferenceH hSRS = OGR_L_GetSpatialRef(hInputLayer);
    OGRwkbGeometryType geomType = OGR_L_GetGeomType(hInputLayer);

    // 创建瓦片图层
    OGRLayerH hTileLayer = createTileLayer(minXs, minYs, maxXs, maxYs, tileIndices, tileCount, hSRS);
    if (!hTileLayer) return 0;

    // 创建输出数据源用于存储裁剪结果
    OGRSFDriverH hDriver = OGRGetDriverByName("MEM");
    if (!hDriver) return 0;

    OGRDataSourceH hOutputDS = OGR_Dr_CreateDataSource(hDriver, "output", NULL);
    if (!hOutputDS) return 0;

    // 创建裁剪结果图层
    OGRLayerH hClippedLayer = OGR_DS_CreateLayer(hOutputDS, "clipped", hSRS, geomType, NULL);
    if (!hClippedLayer) {
        OGR_DS_Destroy(hOutputDS);
        return 0;
    }

    // 复制输入图层的字段定义
    OGRFeatureDefnH hInputDefn = OGR_L_GetLayerDefn(hInputLayer);
    int fieldCount = OGR_FD_GetFieldCount(hInputDefn);

    for (int i = 0; i < fieldCount; i++) {
        OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hInputDefn, i);
        OGR_L_CreateField(hClippedLayer, hFieldDefn, 1);
    }

    // 添加clip_index字段到裁剪结果图层
    OGRFieldDefnH hClipIndexField = OGR_Fld_Create("clip_index", OFTInteger);
    OGR_L_CreateField(hClippedLayer, hClipIndexField, 1);
    OGR_Fld_Destroy(hClipIndexField);

    // 执行交集操作 - 这是关键的一次性裁剪
    OGRErr eErr = OGR_L_Intersection(hInputLayer, hTileLayer, hClippedLayer, NULL, NULL, NULL);
    if (eErr != OGRERR_NONE) {
        OGR_DS_Destroy(hOutputDS);
        return 0;
    }

    // 按clip_index分组并序列化
    int successCount = 0;

    // 为每个瓦片创建内存图层用于分组
    OGRLayerH* tileLayers = (OGRLayerH*)calloc(tileCount, sizeof(OGRLayerH));
    OGRDataSourceH* tileDatasources = (OGRDataSourceH*)calloc(tileCount, sizeof(OGRDataSourceH));

    if (!tileLayers || !tileDatasources) {
        if (tileLayers) free(tileLayers);
        if (tileDatasources) free(tileDatasources);
        OGR_DS_Destroy(hOutputDS);
        return 0;
    }

    // 创建索引映射
    int* indexMap = (int*)malloc(sizeof(int) * tileCount);
    for (int i = 0; i < tileCount; i++) {
        indexMap[i] = tileIndices[i];

        // 为每个瓦片创建输出图层
        tileDatasources[i] = OGR_Dr_CreateDataSource(hDriver, "", NULL);
        if (tileDatasources[i]) {
            char layerName[64];
            snprintf(layerName, sizeof(layerName), "tile_%d", tileIndices[i]);

            tileLayers[i] = OGR_DS_CreateLayer(tileDatasources[i], layerName, hSRS, geomType, NULL);
            if (tileLayers[i]) {
                // 复制字段定义（不包括clip_index）
                for (int j = 0; j < fieldCount; j++) {
                    OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(hInputDefn, j);
                    OGR_L_CreateField(tileLayers[i], hFieldDefn, 1);
                }
            }
        }
    }

    // 遍历裁剪结果，按clip_index分组
    OGR_L_ResetReading(hClippedLayer);
    OGRFeatureH hFeature;

    while ((hFeature = OGR_L_GetNextFeature(hClippedLayer)) != NULL) {
        // 获取clip_index值
        int clipIndex = OGR_F_GetFieldAsInteger(hFeature, fieldCount); // clip_index是最后一个字段

        // 找到对应的瓦片索引
        int tileIdx = -1;
        for (int i = 0; i < tileCount; i++) {
            if (indexMap[i] == clipIndex) {
                tileIdx = i;
                break;
            }
        }

        if (tileIdx >= 0 && tileLayers[tileIdx]) {
            // 创建新要素（不包含clip_index字段）
            OGRFeatureDefnH hTileDefn = OGR_L_GetLayerDefn(tileLayers[tileIdx]);
            OGRFeatureH hNewFeature = OGR_F_Create(hTileDefn);

            // 复制几何
            OGRGeometryH hGeom = OGR_F_GetGeometryRef(hFeature);
            if (hGeom) {
                OGR_F_SetGeometry(hNewFeature, hGeom);
            }

            // 复制字段值（排除clip_index）
            for (int i = 0; i < fieldCount; i++) {
                if (OGR_F_IsFieldSet(hFeature, i)) {
                    OGRFieldType fieldType = OGR_Fld_GetType(OGR_FD_GetFieldDefn(hInputDefn, i));

                    switch (fieldType) {
                        case OFTInteger:
                            OGR_F_SetFieldInteger(hNewFeature, i, OGR_F_GetFieldAsInteger(hFeature, i));
                            break;
                        case OFTInteger64:
                            OGR_F_SetFieldInteger64(hNewFeature, i, OGR_F_GetFieldAsInteger64(hFeature, i));
                            break;
                        case OFTReal:
                            OGR_F_SetFieldDouble(hNewFeature, i, OGR_F_GetFieldAsDouble(hFeature, i));
                            break;
                        case OFTString:
                            OGR_F_SetFieldString(hNewFeature, i, OGR_F_GetFieldAsString(hFeature, i));
                            break;
                        case OFTBinary: {
                            int size;
                            GByte* data = OGR_F_GetFieldAsBinary(hFeature, i, &size);
                            OGR_F_SetFieldBinary(hNewFeature, i, size, data);
                            break;
                        }
                        default:
                            break;
                    }
                }
            }

            // 添加到对应瓦片图层
            OGR_L_CreateFeature(tileLayers[tileIdx], hNewFeature);
            OGR_F_Destroy(hNewFeature);
        }

        OGR_F_Destroy(hFeature);
    }

    // 序列化每个瓦片图层
    for (int i = 0; i < tileCount; i++) {
        if (tileLayers[i]) {
            char outputPath[512];
            snprintf(outputPath, sizeof(outputPath), "%s/%d.bin", outputDir, indexMap[i]);

            SerializeResult result = serializeLayerToBinary(tileLayers[i], bufferSize);
            if (result.success) {
                FILE* file = fopen(outputPath, "wb");
                if (file) {
                    size_t written = fwrite(result.data, 1, result.size, file);
                    fclose(file);
                    if (written == result.size) {
                        successCount++;
                    }
                }
                free(result.data);
            }
        }
    }

    // 清理资源
    for (int i = 0; i < tileCount; i++) {
        if (tileDatasources[i]) {
            OGR_DS_Destroy(tileDatasources[i]);
        }
    }

    free(tileLayers);
    free(tileDatasources);
    free(indexMap);
    OGR_DS_Destroy(hOutputDS);

    return successCount;
}


*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// TileInfo 分块信息
type TileInfo struct {
	Index    int     // 分块索引
	MinX     float64 // 边界框
	MinY     float64
	MaxX     float64
	MaxY     float64
	Envelope C.OGRGeometryH // 分块包络几何体
}

// SerializedLayerData 表示序列化的图层数据
type SerializedLayerData struct {
	Data []byte
	Size int
}

// TileProcessingConfig 瓦片处理配置
type TileProcessingConfig struct {
	MaxConcurrency int  // 最大并发数
	BufferSize     int  // 预分配缓冲区大小
	EnableProgress bool // 是否启用进度回调
}

// TileClipResultM 表示瓦片裁剪结果
type TileClipResultM struct {
	Index        int    // 瓦片索引
	Success      bool   // 是否成功
	Error        error  // 错误信息
	OutputPath   string // 输出文件路径
	FeatureCount int    // 要素数量
}

// Extent 表示空间范围
type Extent struct {
	MinX, MinY, MaxX, MaxY float64
}

// ProgressCallback 进度回调函数
type TileProgressCallback func(processed, total int, currentTile *TileInfo)

// OptimizedTileProcessor 优化的瓦片处理器
type OptimizedTileProcessor struct {
	sourceLayer  *GDALLayer
	clippedLayer *GDALLayer
	tiles        []*TileInfo
	config       *TileProcessingConfig
	mutex        sync.RWMutex
}

// createTileInfos 创建瓦片Identity信息
func createTileInfos(extent *Extent, tileCount int) []*TileInfo {
	tiles := make([]*TileInfo, 0, tileCount*tileCount)

	width := extent.MaxX - extent.MinX
	height := extent.MaxY - extent.MinY

	tileWidth := width / float64(tileCount)
	tileHeight := height / float64(tileCount)

	index := 0
	for row := 0; row < tileCount; row++ {
		for col := 0; col < tileCount; col++ {
			minX := extent.MinX + float64(col)*tileWidth
			maxX := extent.MinX + float64(col+1)*tileWidth
			minY := extent.MinY + float64(row)*tileHeight
			maxY := extent.MinY + float64(row+1)*tileHeight

			// 确保最后一行/列覆盖到边界
			if col == tileCount-1 {
				maxX = extent.MaxX
			}
			if row == tileCount-1 {
				maxY = extent.MaxY
			}

			tiles = append(tiles, &TileInfo{
				Index: index,
				MinX:  minX,
				MinY:  minY,
				MaxX:  maxX,
				MaxY:  maxY,
			})
			index++
		}
	}

	return tiles
}

// NewOptimizedTileProcessor 创建优化的瓦片处理器
func NewOptimizedTileProcessor(sourceLayer *GDALLayer, tiles []*TileInfo, config *TileProcessingConfig) (*OptimizedTileProcessor, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, errors.New("invalid source layer")
	}

	if len(tiles) == 0 {
		return nil, errors.New("no tiles provided")
	}

	if config == nil {
		config = &TileProcessingConfig{
			MaxConcurrency: 8,
			BufferSize:     1024 * 1024,
			EnableProgress: false,
		}
	}

	return &OptimizedTileProcessor{
		sourceLayer: sourceLayer,
		tiles:       tiles,
		config:      config,
	}, nil
}

// GroupAndExportByTiles 使用优化的方法按瓦片分组并导出为bin文件
func (p *OptimizedTileProcessor) GroupAndExportByTiles(outputDir string) ([]*TileClipResultM, error) {
	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	// 准备C函数参数
	tileCount := len(p.tiles)
	minXs := make([]C.double, tileCount)
	minYs := make([]C.double, tileCount)
	maxXs := make([]C.double, tileCount)
	maxYs := make([]C.double, tileCount)
	tileIndices := make([]C.int, tileCount)

	for i, tile := range p.tiles {
		minXs[i] = C.double(tile.MinX)
		minYs[i] = C.double(tile.MinY)
		maxXs[i] = C.double(tile.MaxX)
		maxYs[i] = C.double(tile.MaxY)
		tileIndices[i] = C.int(tile.Index)
	}

	// 调用优化的C函数进行一次性裁剪和分组
	cOutputDir := C.CString(outputDir)
	defer C.free(unsafe.Pointer(cOutputDir))

	successCount := C.clipAndGroupByTilesOptimized(
		p.sourceLayer.layer,
		&minXs[0], &minYs[0], &maxXs[0], &maxYs[0], &tileIndices[0],
		C.int(tileCount),
		cOutputDir,
		C.int(p.config.BufferSize),
	)

	// 构建结果
	results := make([]*TileClipResultM, tileCount)
	for i, tile := range p.tiles {
		result := &TileClipResultM{
			Index:      tile.Index,
			OutputPath: filepath.Join(outputDir, fmt.Sprintf("%d.bin", tile.Index)),
		}

		// 检查文件是否存在来判断是否成功
		if _, err := os.Stat(result.OutputPath); err == nil {
			result.Success = true
		} else {
			result.Success = false
			result.Error = fmt.Errorf("output file not created: %v", err)
		}

		results[i] = result
	}

	if int(successCount) < tileCount {
		return results, fmt.Errorf("only %d out of %d tiles processed successfully", successCount, tileCount)
	}

	return results, nil
}

// Cleanup 清理资源
func (p *OptimizedTileProcessor) Cleanup() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.clippedLayer != nil && p.clippedLayer.layer != nil {
		// 这里需要清理C层面的资源
		// 注意：由于是MEM驱动创建的，需要销毁整个数据源
		p.clippedLayer = nil
	}
}

// ClipAndSerializeLayerByTilesOptimized 优化版本的主要接口函数
func ClipAndSerializeLayerByTilesOptimized(layer *GDALLayer, tiles []*TileInfo, uuid string, config *TileProcessingConfig, progressCallback TileProgressCallback) ([]*TileClipResultM, error) {
	if layer == nil || layer.layer == nil {
		return nil, errors.New("invalid GDAL layer")
	}

	if len(tiles) == 0 {
		return nil, errors.New("no tiles provided")
	}

	if uuid == "" {
		return nil, errors.New("UUID cannot be empty")
	}

	// 创建优化的处理器（不再需要预裁剪步骤）
	processor, err := NewOptimizedTileProcessor(layer, tiles, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor: %v", err)
	}
	defer processor.Cleanup()

	// 输出目录
	outputDir := uuid

	// 直接执行优化的处理流程（内部进行一次性裁剪和分组）
	results, err := processor.GroupAndExportByTiles(outputDir)
	if err != nil {
		return results, err
	}

	// 进度回调
	if config != nil && config.EnableProgress && progressCallback != nil {
		successCount := 0
		for _, result := range results {
			if result.Success {
				successCount++
			}
		}
		if len(tiles) > 0 {
			progressCallback(successCount, len(tiles), tiles[0])
		}
	}

	return results, nil
}

// BatchProcessTilesOptimized 批量优化处理多个图层的瓦片
func BatchProcessTilesOptimized(layers []*GDALLayer, tiles []*TileInfo, baseOutputDir string, config *TileProcessingConfig) (map[int][]*TileClipResultM, error) {
	if len(layers) == 0 {
		return nil, errors.New("no layers provided")
	}

	results := make(map[int][]*TileClipResultM)
	var mutex sync.Mutex
	var wg sync.WaitGroup

	// 并发处理每个图层
	semaphore := make(chan struct{}, config.MaxConcurrency)
	errChan := make(chan error, len(layers))

	for i, layer := range layers {
		wg.Add(1)
		go func(layerIndex int, gdalLayer *GDALLayer) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			outputDir := filepath.Join(baseOutputDir, fmt.Sprintf("layer_%d", layerIndex))
			layerResults, err := ClipAndSerializeLayerByTilesOptimized(
				gdalLayer,
				tiles,
				outputDir,
				config,
				nil,
			)

			if err != nil {
				errChan <- fmt.Errorf("layer %d processing failed: %v", layerIndex, err)
				return
			}

			mutex.Lock()
			results[layerIndex] = layerResults
			mutex.Unlock()
		}(i, layer)
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	var processingErrors []error
	for err := range errChan {
		processingErrors = append(processingErrors, err)
	}

	if len(processingErrors) > 0 {
		return results, fmt.Errorf("batch processing failed with %d errors: %v", len(processingErrors), processingErrors[0])
	}

	return results, nil
}

func GenerateTiles(inputLayer, methodLayer *GDALLayer, TileCount int, uuid string) {
	defer inputLayer.Close()
	defer methodLayer.Close()

	// 获取数据范围
	extent, err := getLayersExtent(inputLayer, methodLayer)
	if err != nil {
		fmt.Printf("获取图层范围失败: %v\n", err)
		return
	}

	// 创建瓦片裁剪信息
	tiles := createTileInfos(extent, TileCount)

	// 配置处理参数
	config := &TileProcessingConfig{
		MaxConcurrency: runtime.NumCPU() / 2, // 使用CPU核心数
		BufferSize:     1024 * 1024,          // 1MB缓冲区
		EnableProgress: true,                 // 启用进度回调
	}

	// 定义进度回调函数
	progressCallback := func(layerName string) func(processed, total int, currentTile *TileInfo) {
		return func(processed, total int, currentTile *TileInfo) {
			percentage := float64(processed) / float64(total) * 100
			fmt.Printf("[%s] 处理进度: %d/%d (%.1f%%) ",
				layerName, processed, total, percentage)
		}
	}

	// 使用 WaitGroup 来等待两个 goroutine 完成
	var wg sync.WaitGroup
	var err1, err2 error

	// 启动第一个图层的处理
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err1 = ClipAndSerializeLayerByTilesOptimized(
			inputLayer,
			tiles,
			uuid+"/layer1", // 输出目录名
			config,
			progressCallback("layer1"),
		)
		if err1 != nil {
			fmt.Printf("layer1 裁剪处理失败: %v\n", err1)
		}
	}()

	// 启动第二个图层的处理
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err2 = ClipAndSerializeLayerByTilesOptimized(
			methodLayer,
			tiles,
			uuid+"/layer2", // 输出目录名
			config,
			progressCallback("layer2"),
		)
		if err2 != nil {
			fmt.Printf("layer2 裁剪处理失败: %v\n", err2)
		}
	}()

	// 等待所有 goroutine 完成
	wg.Wait()

	// 检查是否有错误发生
	if err1 != nil || err2 != nil {
		fmt.Printf("分割处理完成，但存在错误 - layer1: %v, layer2: %v\n", err1, err2)
		return
	}

	fmt.Printf("所有图层分割处理完成\n")
}

type GroupTileFiles struct {
	Index int
	GPBin GroupBin
	Size  float64
}
type GroupBin struct {
	Layer1 string
	Layer2 string
}

// ReadAndGroupBinFiles 读取layer1和layer2文件夹中的bin文件并按文件名分组
func ReadAndGroupBinFiles(uuid string) ([]GroupTileFiles, error) {
	layer1Dir := uuid + "/layer1"
	layer2Dir := uuid + "/layer2"
	layer1Map := readBinFilesMap(layer1Dir)
	layer2Map := readBinFilesMap(layer2Dir)

	// 收集所有文件名索引
	indexMap := make(map[int]bool)
	for idx := range layer1Map {
		indexMap[idx] = true
	}
	for idx := range layer2Map {
		indexMap[idx] = true
	}

	var result []GroupTileFiles
	count := 0

	for index := range indexMap {

		result = append(result, GroupTileFiles{
			Index: index,
			GPBin: GroupBin{
				Layer1: layer1Map[index],
				Layer2: layer2Map[index],
			},
		})
		count++
	}

	return result, nil
}

// readBinFilesMap 读取目录下的bin文件并返回文件名索引到路径的映射
func readBinFilesMap(dir string) map[int]string {
	fileMap := make(map[int]string)

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.ToLower(filepath.Ext(path)) == ".bin" {
			fileName := strings.TrimSuffix(filepath.Base(path), ".bin")
			if index, err := strconv.Atoi(fileName); err == nil {
				fileMap[index] = path
			}
		}
		return nil
	})

	return fileMap
}

// PG
func GenerateTilesFromPG(db *gorm.DB, table1, table2 string, tileCount int, uuid string) error {
	// 1. 直接从PG计算合并的extent
	extent, err := getLayersExtentFromPG(db, table1, table2)

	if err != nil {
		return fmt.Errorf("获取图层范围失败: %v", err)
	}
	// 2. 创建瓦片信息
	tiles := createTileInfos(extent, tileCount)
	// 3. 配置处理参数
	config := &TileProcessingConfig{
		MaxConcurrency: runtime.NumCPU() / 2,
		BufferSize:     1024 * 1024,
		EnableProgress: true,
	}
	// 4. 并发处理两个图层的瓦片裁剪
	var wg sync.WaitGroup
	var err1, err2 error
	// 处理第一个图层
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = clipAndSerializeTilesFromPG(
			db,
			table1,
			tiles,
			filepath.Join(uuid, "layer1"),
			config,
		)
		if err1 != nil {
			log.Printf("layer1 裁剪处理失败: %v\n", err1)
		} else {
			log.Printf("layer1 处理完成\n")
		}
	}()
	// 处理第二个图层
	wg.Add(1)
	go func() {
		defer wg.Done()
		err2 = clipAndSerializeTilesFromPG(
			db,
			table2,
			tiles,
			filepath.Join(uuid, "layer2"),
			config,
		)
		if err2 != nil {
			log.Printf("layer2 裁剪处理失败: %v\n", err2)
		} else {
			log.Printf("layer2 处理完成\n")
		}
	}()
	wg.Wait()
	if err1 != nil || err2 != nil {
		return fmt.Errorf("分割处理失败 - layer1: %v, layer2: %v", err1, err2)
	}
	fmt.Printf("所有图层分割处理完成\n")
	return nil
}

// getLayersExtentFromPG 直接从PostgreSQL获取两个图层的合并范围
func getLayersExtentFromPG(db *gorm.DB, table1, table2 string) (*Extent, error) {
	// 使用UNION ALL合并两个表的extent，然后计算总体范围
	query := fmt.Sprintf(`
		WITH combined_extents AS (
			SELECT ST_Extent(geom) as extent FROM %s
			UNION ALL
			SELECT ST_Extent(geom) as extent FROM %s
		)
		SELECT 
			MIN(ST_XMin(extent)) as minx,
			MIN(ST_YMin(extent)) as miny,
			MAX(ST_XMax(extent)) as maxx,
			MAX(ST_YMax(extent)) as maxy
		FROM combined_extents
	`, table1, table2)
	var result struct {
		MinX float64 `gorm:"column:minx"`
		MinY float64 `gorm:"column:miny"`
		MaxX float64 `gorm:"column:maxx"`
		MaxY float64 `gorm:"column:maxy"`
	}
	err := db.Raw(query).Scan(&result).Error
	if err != nil {
		return nil, fmt.Errorf("查询extent失败: %v", err)
	}
	return &Extent{
		MinX: result.MinX,
		MinY: result.MinY,
		MaxX: result.MaxX,
		MaxY: result.MaxY,
	}, nil
}

// clipAndSerializeTilesFromPG 从PG查询并裁剪瓦片数据，然后序列化为bin文件
func clipAndSerializeTilesFromPG(db *gorm.DB, tableName string, tiles []*TileInfo, outputDir string, config *TileProcessingConfig) error {
	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}
	// 使用信号量控制并发数
	semaphore := make(chan struct{}, config.MaxConcurrency)
	var wg sync.WaitGroup
	errChan := make(chan error, len(tiles))
	successCount := 0
	var countMutex sync.Mutex
	for _, tile := range tiles {
		wg.Add(1)
		go func(t *TileInfo) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			err := processSingleTileFromPG(db, tableName, t, outputDir, config.BufferSize)
			if err != nil {
				errChan <- fmt.Errorf("处理瓦片 %d 失败: %v", t.Index, err)
			} else {
				countMutex.Lock()
				successCount++
				if successCount%10 == 0 {
					log.Printf("[%s] 已完成 %d/%d 个瓦片", tableName, successCount, len(tiles))
				}
				countMutex.Unlock()
			}
		}(tile)
	}
	wg.Wait()
	close(errChan)
	// 检查错误
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return fmt.Errorf("处理过程中出现 %d 个错误，首个错误: %v", len(errors), errors[0])
	}

	return nil
}

func processSingleTileFromPG(db *gorm.DB, tableName string, tile *TileInfo, outputDir string, bufferSize int) error {
	// 1. 构建瓦片的WKT表示
	tileWKT := fmt.Sprintf("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
		tile.MinX, tile.MinY,
		tile.MaxX, tile.MinY,
		tile.MaxX, tile.MaxY,
		tile.MinX, tile.MaxY,
		tile.MinX, tile.MinY,
	)

	// 2. 获取表的SRID
	var srid int
	err := db.Raw(fmt.Sprintf(`
		SELECT Find_SRID('public', '%s', 'geom') as srid
	`, tableName)).Scan(&srid).Error
	if err != nil {
		return fmt.Errorf("获取SRID失败: %v", err)
	}

	// 3. 先检查是否有数据
	var count int64
	err = db.Raw(fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s
		WHERE ST_Intersects(geom, ST_GeomFromText('%s', %d))
	`, tableName, tileWKT, srid)).Scan(&count).Error

	if err != nil {
		return fmt.Errorf("检查数据失败: %v", err)
	}

	outputPath := filepath.Join(outputDir, fmt.Sprintf("%d.bin", tile.Index))

	// 如果没有数据，创建空bin文件
	if count == 0 {
		log.Printf("瓦片 %d 无数据，创建空bin文件", tile.Index)
		return createEmptyBinFile(outputPath)
	}

	// 4. 获取字段列表（按顺序）
	var columns []struct {
		ColumnName string `gorm:"column:column_name"`
	}
	err = db.Raw(fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = '%s' 
		AND column_name != 'geom'
		ORDER BY ordinal_position
	`, tableName)).Scan(&columns).Error

	if err != nil {
		return fmt.Errorf("获取字段列表失败: %v", err)
	}

	// 构建明确的字段列表
	var fieldList []string
	for _, col := range columns {
		fieldList = append(fieldList, col.ColumnName)
	}
	fieldListStr := strings.Join(fieldList, ", ")

	// 5. 修改查询，强制转换所有几何为MultiPolygon
	query := fmt.Sprintf(`
		SELECT 
			ST_AsBinary(
				ST_Multi(
					ST_Intersection(geom, ST_GeomFromText('%s', %d))
				)
			) as geom,
			%s
		FROM %s
		WHERE ST_Intersects(geom, ST_GeomFromText('%s', %d))
		AND NOT ST_IsEmpty(ST_Intersection(geom, ST_GeomFromText('%s', %d)))
	`, tileWKT, srid, fieldListStr, tableName, tileWKT, srid, tileWKT, srid)

	// 6. 创建临时GDALLayer来存储查询结果
	tempLayer, err := createLayerFromPGQuery(db, query, tableName, srid)
	//err = WriteShapeFileLayer(tempLayer, fmt.Sprintf("C:\\Users\\Administrator\\Desktop\\新建文件夹 (2)\\%s_%d.shp", tableName, tile.Index), fmt.Sprintf("%d", tile.Index), true)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}
	if err != nil {
		return fmt.Errorf("创建临时图层失败: %v", err)
	}
	defer tempLayer.Close()

	// 7. 检查是否有数据
	featureCount := tempLayer.GetFeatureCount()
	if featureCount == 0 {
		// 创建空的bin文件
		return createEmptyBinFile(outputPath)
	}

	// 8. 序列化为bin文件
	err = serializeLayerToBinFile(tempLayer, outputPath, bufferSize)
	if err != nil {
		return fmt.Errorf("序列化失败: %v", err)
	}

	return nil
}
func getTableGeometryType(db *gorm.DB, tableName string) (C.OGRwkbGeometryType, error) {
	var result struct {
		Type string `gorm:"column:type"`
	}

	err := db.Raw(`
        SELECT type 
        FROM geometry_columns 
        WHERE f_table_name = ? AND f_geometry_column = 'geom'
    `, tableName).Scan(&result).Error

	if err != nil {
		return C.wkbUnknown, err
	}

	return mapGeometryColumnTypeToOGR(result.Type), nil
}

func mapGeometryColumnTypeToOGR(geomType string) C.OGRwkbGeometryType {
	switch strings.ToUpper(geomType) {
	case "POINT":
		return C.wkbPoint
	case "LINESTRING":
		return C.wkbLineString
	case "POLYGON":
		return C.wkbPolygon
	case "MULTIPOINT":
		return C.wkbMultiPoint
	case "MULTILINESTRING":
		return C.wkbMultiLineString
	case "MULTIPOLYGON":
		return C.wkbMultiPolygon
	case "GEOMETRY":
		return C.wkbUnknown
	default:
		return C.wkbUnknown
	}
}

func createLayerFromPGQuery(db *gorm.DB, query string, sourceTable string, srid int) (*GDALLayer, error) {
	// 创建内存数据源
	driver := C.OGRGetDriverByName(C.CString("Memory"))
	if driver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}
	ds := C.OGR_Dr_CreateDataSource(driver, C.CString(""), nil)
	if ds == nil {
		return nil, fmt.Errorf("创建内存数据源失败")
	}

	// 创建空间参考
	srs := C.OSRNewSpatialReference(nil)
	C.OSRImportFromEPSG(srs, C.int(srid))
	defer C.OSRDestroySpatialReference(srs)

	// 创建图层 - 强制使用MultiPolygon类型
	layerName := C.CString("temp_layer")
	defer C.free(unsafe.Pointer(layerName))

	// 获取原始几何类型并转换为Multi类型
	originalGeomType, err := getTableGeometryType(db, sourceTable)
	if err != nil {
		log.Printf("获取几何类型失败，使用默认类型: %v", err)
		originalGeomType = C.wkbPolygon // 默认为Polygon
	}

	// 转换为对应的Multi类型
	var multiGeomType C.OGRwkbGeometryType
	switch originalGeomType {
	case C.wkbPolygon:
		multiGeomType = C.wkbMultiPolygon
	case C.wkbLineString:
		multiGeomType = C.wkbMultiLineString
	case C.wkbPoint:
		multiGeomType = C.wkbMultiPoint
	default:
		multiGeomType = C.wkbMultiPolygon // 默认使用MultiPolygon
	}

	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, multiGeomType, nil)
	if layer == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建图层失败")
	}

	// 获取源表的字段定义（排除geom字段）- 按顺序
	var columns []struct {
		ColumnName      string `gorm:"column:column_name"`
		DataType        string `gorm:"column:data_type"`
		UdtName         string `gorm:"column:udt_name"`
		OrdinalPosition int    `gorm:"column:ordinal_position"`
	}
	err = db.Raw(fmt.Sprintf(`
		SELECT column_name, data_type, udt_name, ordinal_position
		FROM information_schema.columns
		WHERE table_name = '%s' 
		AND column_name != 'geom'
		ORDER BY ordinal_position
	`, sourceTable)).Scan(&columns).Error

	if err != nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("获取字段定义失败: %v", err)
	}

	// 添加字段定义到图层
	for _, col := range columns {
		fieldType := mapPostgreSQLTypeToOGR(col.DataType)
		fieldName := C.CString(col.ColumnName)
		fieldDefn := C.OGR_Fld_Create(fieldName, fieldType)
		C.OGR_L_CreateField(layer, fieldDefn, 1)
		C.OGR_Fld_Destroy(fieldDefn)
		C.free(unsafe.Pointer(fieldName))
	}

	// 执行查询并填充数据
	rows, err := db.Raw(query).Rows()
	if err != nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("执行查询失败: %v", err)
	}
	defer rows.Close()

	// 获取查询结果的列名
	columnNames, err := rows.Columns()
	if err != nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("获取列名失败: %v", err)
	}

	// 创建字段名到图层字段索引的映射
	fieldNameToLayerIndex := make(map[string]int)
	for i, col := range columns {
		fieldNameToLayerIndex[col.ColumnName] = i
	}

	// 创建查询列名到结果索引的映射
	columnMap := make(map[string]int)
	for i, name := range columnNames {
		columnMap[name] = i
	}

	// 获取图层字段定义
	layerFieldDefns := make([]C.OGRFieldDefnH, len(columns))
	featureDefn := C.OGR_L_GetLayerDefn(layer)
	featureCount := 0

	for i, _ := range columns {
		layerFieldDefns[i] = C.OGR_FD_GetFieldDefn(featureDefn, C.int(i))
	}

	for rows.Next() {
		// 准备扫描目标
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Printf("扫描行数据失败: %v", err)
			continue
		}

		// 创建要素
		feature := C.OGR_F_Create(featureDefn)

		// 设置几何字段 - 关键修复：确保几何类型匹配
		if geomIdx, ok := columnMap["geom"]; ok {
			if wkb, ok := values[geomIdx].([]byte); ok && len(wkb) > 0 {
				// 先创建几何对象
				geom := C.OGR_G_CreateGeometry(multiGeomType) // 使用Unknown让GDAL自动识别
				if geom != nil {
					cWkb := C.CBytes(wkb)
					result := C.OGR_G_ImportFromWkb(geom, cWkb, C.int(len(wkb)))
					C.free(cWkb)

					if result == C.OGRERR_NONE {
						// 检查几何类型并转换为Multi类型（如果需要）
						geomType := C.OGR_G_GetGeometryType(geom)

						var finalGeom C.OGRGeometryH

						// 如果已经是Multi类型，直接使用
						if isMultiGeometryType(geomType) {
							finalGeom = geom
						} else {
							// 转换为Multi类型
							finalGeom = convertToMultiGeometry(geom, multiGeomType)
							if finalGeom != geom {
								C.OGR_G_DestroyGeometry(geom) // 销毁原始几何
							}
						}

						if finalGeom != nil {
							setResult := C.OGR_F_SetGeometry(feature, finalGeom)
							if setResult != C.OGRERR_NONE {
								log.Printf("设置几何失败，错误码: %d", int(setResult))
							}

							if finalGeom != geom {
								C.OGR_G_DestroyGeometry(finalGeom)
							}
						}
					} else {
						log.Printf("导入WKB失败，错误码: %d", int(result))
					}

					if geom != nil {
						C.OGR_G_DestroyGeometry(geom)
					}
				}
			}
		}

		// 按字段定义顺序设置字段值
		for i, col := range columns {
			if queryColIdx, ok := columnMap[col.ColumnName]; ok {
				if queryColIdx < len(values) && values[queryColIdx] != nil {
					fieldDefn := layerFieldDefns[i]
					fieldType := C.OGR_Fld_GetType(fieldDefn)
					setFeatureFieldValueWithConversion(feature, C.int(i), values[queryColIdx], fieldType)
				}
			}
		}

		// 添加要素到图层
		if C.OGR_L_CreateFeature(layer, feature) == C.OGRERR_NONE {
			featureCount++
		} else {
			log.Printf("创建要素失败")
		}
		C.OGR_F_Destroy(feature)
	}

	return &GDALLayer{
		layer:   layer,
		dataset: ds,
		driver:  driver,
	}, nil
}

// 辅助函数：检查是否为Multi几何类型
func isMultiGeometryType(geomType C.OGRwkbGeometryType) bool {
	switch geomType {
	case C.wkbMultiPoint, C.wkbMultiLineString, C.wkbMultiPolygon:
		return true
	default:
		return false
	}
}

// 辅助函数：将几何转换为Multi类型
func convertToMultiGeometry(geom C.OGRGeometryH, targetMultiType C.OGRwkbGeometryType) C.OGRGeometryH {
	if geom == nil {
		return nil
	}

	// 创建Multi几何容器
	multiGeom := C.OGR_G_CreateGeometry(targetMultiType)
	if multiGeom == nil {
		return nil
	}

	// 克隆原始几何并添加到Multi容器中
	clonedGeom := C.OGR_G_Clone(geom)
	if clonedGeom != nil {
		result := C.OGR_G_AddGeometry(multiGeom, clonedGeom)
		C.OGR_G_DestroyGeometry(clonedGeom)

		if result == C.OGRERR_NONE {
			return multiGeom
		}
	}

	// 如果失败，清理并返回nil
	C.OGR_G_DestroyGeometry(multiGeom)
	return nil
}

// setFeatureFieldValueWithConversion 根据目标字段类型进行智能转换
func setFeatureFieldValueWithConversion(feature C.OGRFeatureH, fieldIndex C.int, value interface{}, targetType C.OGRFieldType) {
	if value == nil {
		return
	}

	switch targetType {
	case C.OFTInteger:
		// 目标是整数
		switch v := value.(type) {
		case int, int32, int64:
			C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(convertToInt64(v)))
		case float32, float64:
			C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(convertToFloat64(v)))
		case []byte:
			if intVal, err := strconv.ParseInt(string(v), 10, 32); err == nil {
				C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(intVal))
			}
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 32); err == nil {
				C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(intVal))
			}
		}

	case C.OFTInteger64:
		// 目标是64位整数
		switch v := value.(type) {
		case int, int32, int64:
			C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(convertToInt64(v)))
		case float32, float64:
			C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(convertToFloat64(v)))
		case []byte:
			if intVal, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(intVal))
			}
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(intVal))
			}
		}

	case C.OFTReal:
		// 目标是浮点数
		switch v := value.(type) {
		case float32, float64:
			C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(convertToFloat64(v)))
		case int, int32, int64:
			C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(convertToInt64(v)))
		case []byte:
			if floatVal, err := strconv.ParseFloat(string(v), 64); err == nil {
				C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(floatVal))
			}
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(floatVal))
			}
		}

	case C.OFTString:
		// 目标是字符串
		var strVal string
		switch v := value.(type) {
		case string:
			strVal = v
		case []byte:
			strVal = string(v)
		case int, int32, int64:
			strVal = fmt.Sprintf("%d", convertToInt64(v))
		case float32, float64:
			strVal = fmt.Sprintf("%f", convertToFloat64(v))
		default:
			strVal = fmt.Sprintf("%v", v)
		}
		cStr := C.CString(strVal)
		C.OGR_F_SetFieldString(feature, fieldIndex, cStr)
		C.free(unsafe.Pointer(cStr))

	default:
		// 其他类型，尝试转换为字符串
		var strVal string
		switch v := value.(type) {
		case string:
			strVal = v
		case []byte:
			strVal = string(v)
		default:
			strVal = fmt.Sprintf("%v", v)
		}
		cStr := C.CString(strVal)
		C.OGR_F_SetFieldString(feature, fieldIndex, cStr)
		C.free(unsafe.Pointer(cStr))
	}
}

// 辅助转换函数
func convertToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func convertToFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}

func mapPostgreSQLTypeToOGR(pgType string) C.OGRFieldType {
	switch strings.ToLower(pgType) {
	case "integer", "smallint", "int", "int2", "int4":
		return C.OFTInteger
	case "bigint", "int8":
		return C.OFTInteger64
	case "real", "float4":
		return C.OFTReal
	case "double precision", "float8":
		return C.OFTReal
	case "numeric", "decimal":
		return C.OFTReal // numeric 映射为 Real
	case "character varying", "varchar", "text", "character", "char", "bpchar":
		return C.OFTString
	case "date":
		return C.OFTDate
	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return C.OFTDateTime
	case "boolean", "bool":
		return C.OFTInteger
	default:
		log.Printf("未知的PostgreSQL类型: %s，使用String类型", pgType)
		return C.OFTString
	}
}

// setFeatureFieldValue 设置要素字段值
func setFeatureFieldValue(feature C.OGRFeatureH, fieldIndex C.int, value interface{}) {
	if value == nil {
		return
	}
	switch v := value.(type) {
	case int:
		C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(v))
	case int32:
		C.OGR_F_SetFieldInteger(feature, fieldIndex, C.int(v))
	case int64:
		C.OGR_F_SetFieldInteger64(feature, fieldIndex, C.longlong(v))
	case float32:
		C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(v))
	case float64:
		C.OGR_F_SetFieldDouble(feature, fieldIndex, C.double(v))
	case string:
		cStr := C.CString(v)
		C.OGR_F_SetFieldString(feature, fieldIndex, cStr)
		C.free(unsafe.Pointer(cStr))
	case []byte:
		str := string(v)
		cStr := C.CString(str)
		C.OGR_F_SetFieldString(feature, fieldIndex, cStr)
		C.free(unsafe.Pointer(cStr))
	case bool:
		var intVal C.int
		if v {
			intVal = 1
		} else {
			intVal = 0
		}
		C.OGR_F_SetFieldInteger(feature, fieldIndex, intVal)
	}
}

// serializeLayerToBinFile 将图层序列化到bin文件
func serializeLayerToBinFile(layer *GDALLayer, outputPath string, bufferSize int) error {
	// 调用C函数进行序列化
	result := C.serializeLayerToBinary(layer.layer, C.int(bufferSize))
	if result.success == 0 {
		return fmt.Errorf("序列化失败")
	}
	defer C.free(unsafe.Pointer(result.data))
	// 写入文件
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()
	data := C.GoBytes(unsafe.Pointer(result.data), C.int(result.size))
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}
	return nil
}

// createEmptyBinFile 创建空的bin文件（用于没有数据的瓦片）
func createEmptyBinFile(outputPath string) error {
	// 创建一个最小的有效bin文件结构
	// 包含：魔数(8) + 版本(4) + 几何类型(4) + SRS大小(4) + 字段数(4) + 要素数(4) = 28字节

	buffer := make([]byte, 28)
	offset := 0

	// 写入魔数 "GDALLYR2"
	copy(buffer[offset:], []byte("GDALLYR2"))
	offset += 8

	// 写入版本号 2
	binary.LittleEndian.PutUint32(buffer[offset:], 2)
	offset += 4

	// 写入几何类型 wkbUnknown (0)
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// 写入SRS大小 0
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// 写入字段数 0
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// 写入要素数 0
	binary.LittleEndian.PutUint32(buffer[offset:], 0)

	// 写入文件
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建空文件失败: %v", err)
	}
	defer file.Close()

	_, err = file.Write(buffer)
	return err
}

// IsValidBinFile 检查bin文件是否有效（存在且非空）
func IsValidBinFile(filePath string) bool {
	info, err := os.Stat(filePath)
	if err != nil {
		return false
	}

	// 检查文件大小，空文件只有1字节的标记
	return info.Size() > 1
}

// CleanupTileFiles 清理临时瓦片文件
func CleanupTileFiles(taskid string) error {
	return os.RemoveAll(taskid)
}
