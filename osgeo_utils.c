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
// osgeo_utils.c
#include "osgeo_utils.h"

// 声明外部函数，避免重复定义
extern int handleProgressUpdate(double, char*, void*);

OGRGeometryH normalizeGeometryType(OGRGeometryH geom, OGRwkbGeometryType expectedType);

// 创建内存图层用于存储相交结果
OGRLayerH createMemoryLayer(const char* layerName, OGRwkbGeometryType geomType, OGRSpatialReferenceH srs) {
    // 创建内存驱动
    OGRSFDriverH memDriver = OGRGetDriverByName("MEM");
    if (!memDriver) {
        return NULL;
    }

    // 创建内存数据源
    OGRDataSourceH memDS = OGR_Dr_CreateDataSource(memDriver, "", NULL);
    if (!memDS) {
        return NULL;
    }

    // 创建图层
    OGRLayerH layer = OGR_DS_CreateLayer(memDS, layerName, srs, geomType, NULL);
    return layer;
}

// 添加字段到图层
int addFieldToLayer(OGRLayerH layer, const char* fieldName, OGRFieldType fieldType) {
    OGRFieldDefnH fieldDefn = OGR_Fld_Create(fieldName, fieldType);
    if (!fieldDefn) {
        return 0;
    }

    OGRErr err = OGR_L_CreateField(layer, fieldDefn, 1); // 1表示强制创建
    OGR_Fld_Destroy(fieldDefn);

    return (err == OGRERR_NONE) ? 1 : 0;
}
inline int check_isnan(double x) {
    return x != x;
}
// 检查是否为无穷大
inline int check_isinf(double x) {
    return !isfinite(x) && !check_isnan(x);
}

// 复制字段值
void copyFieldValue(OGRFeatureH srcFeature, OGRFeatureH dstFeature, int srcFieldIndex, int dstFieldIndex) {
    if (OGR_F_IsFieldSet(srcFeature, srcFieldIndex)) {
        OGRFieldDefnH fieldDefn = OGR_F_GetFieldDefnRef(srcFeature, srcFieldIndex);
        OGRFieldType fieldType = OGR_Fld_GetType(fieldDefn);

        switch (fieldType) {
            case OFTInteger:
                OGR_F_SetFieldInteger(dstFeature, dstFieldIndex, OGR_F_GetFieldAsInteger(srcFeature, srcFieldIndex));
                break;
            case OFTReal:
                OGR_F_SetFieldDouble(dstFeature, dstFieldIndex, OGR_F_GetFieldAsDouble(srcFeature, srcFieldIndex));
                break;
            case OFTString:
                OGR_F_SetFieldString(dstFeature, dstFieldIndex, OGR_F_GetFieldAsString(srcFeature, srcFieldIndex));
                break;
            default:
                // 其他类型转为字符串
                OGR_F_SetFieldString(dstFeature, dstFieldIndex, OGR_F_GetFieldAsString(srcFeature, srcFieldIndex));
                break;
        }
    }
}

// 进度回调函数 - 这个函数会被GDAL调用
int progressCallback(double dfComplete, const char *pszMessage, void *pProgressArg) {
    // pProgressArg 包含Go回调函数的信息
    if (pProgressArg != NULL) {
        // 调用Go函数处理进度更新
        return handleProgressUpdate(dfComplete, (char*)pszMessage, pProgressArg);
    }
    return 1; // 继续执行
}


// 线程安全的图层克隆函数
OGRLayerH cloneLayerToMemory(OGRLayerH sourceLayer, const char* layerName) {
    if (!sourceLayer) return NULL;

    // 获取源图层信息
    OGRFeatureDefnH sourceDefn = OGR_L_GetLayerDefn(sourceLayer);
    OGRwkbGeometryType geomType = OGR_FD_GetGeomType(sourceDefn);
    OGRSpatialReferenceH srs = OGR_L_GetSpatialRef(sourceLayer);

    // 创建内存图层
    OGRLayerH memLayer = createMemoryLayer(layerName, geomType, srs);
    if (!memLayer) return NULL;

    // 复制字段定义
    int fieldCount = OGR_FD_GetFieldCount(sourceDefn);
    for (int i = 0; i < fieldCount; i++) {
        OGRFieldDefnH fieldDefn = OGR_FD_GetFieldDefn(sourceDefn, i);
        OGRFieldDefnH newFieldDefn = OGR_Fld_Create(
            OGR_Fld_GetNameRef(fieldDefn),
            OGR_Fld_GetType(fieldDefn)
        );
        OGR_Fld_SetWidth(newFieldDefn, OGR_Fld_GetWidth(fieldDefn));
        OGR_Fld_SetPrecision(newFieldDefn, OGR_Fld_GetPrecision(fieldDefn));
        OGR_L_CreateField(memLayer, newFieldDefn, 1);
        OGR_Fld_Destroy(newFieldDefn);
    }

    return memLayer;
}

// 修正的要素复制函数
int copyFeaturesWithSpatialFilter(OGRLayerH sourceLayer, OGRLayerH targetLayer, OGRGeometryH filterGeom) {
    if (!sourceLayer || !targetLayer) return 0;

    // 如果有空间过滤器，设置它
    if (filterGeom) {
        OGR_L_SetSpatialFilter(sourceLayer, filterGeom);
    } else {
        // 确保没有空间过滤器
        OGR_L_SetSpatialFilter(sourceLayer, NULL);
    }

    // 重置读取位置
    OGR_L_ResetReading(sourceLayer);

    int count = 0;
    OGRFeatureH feature;
    OGRFeatureDefnH targetDefn = OGR_L_GetLayerDefn(targetLayer);

    // 遍历所有要素
    while ((feature = OGR_L_GetNextFeature(sourceLayer)) != NULL) {
        // 创建新要素
        OGRFeatureH newFeature = OGR_F_Create(targetDefn);
        if (newFeature) {
            // 复制几何体
            OGRGeometryH geom = OGR_F_GetGeometryRef(feature);
            if (geom) {
                OGRGeometryH clonedGeom = OGR_G_Clone(geom);
                if (clonedGeom) {
                    OGR_F_SetGeometry(newFeature, clonedGeom);
                    OGR_G_DestroyGeometry(clonedGeom);
                }
            }

            // 复制所有字段
            int fieldCount = OGR_F_GetFieldCount(feature);
            for (int i = 0; i < fieldCount; i++) {
                if (OGR_F_IsFieldSet(feature, i)) {
                    // 获取字段类型并复制相应的值
                    OGRFieldDefnH fieldDefn = OGR_F_GetFieldDefnRef(feature, i);
                    OGRFieldType fieldType = OGR_Fld_GetType(fieldDefn);

                    switch (fieldType) {
                        case OFTInteger:
                            OGR_F_SetFieldInteger(newFeature, i, OGR_F_GetFieldAsInteger(feature, i));
                            break;
                        case OFTInteger64:
                            OGR_F_SetFieldInteger64(newFeature, i, OGR_F_GetFieldAsInteger64(feature, i));
                            break;
                        case OFTReal:
                            OGR_F_SetFieldDouble(newFeature, i, OGR_F_GetFieldAsDouble(feature, i));
                            break;
                        case OFTString:
                            OGR_F_SetFieldString(newFeature, i, OGR_F_GetFieldAsString(feature, i));
                            break;
                        case OFTDate:
                        case OFTTime:
                        case OFTDateTime: {
                            int year, month, day, hour, minute, second, tzflag;
                            OGR_F_GetFieldAsDateTime(feature, i, &year, &month, &day, &hour, &minute, &second, &tzflag);
                            OGR_F_SetFieldDateTime(newFeature, i, year, month, day, hour, minute, second, tzflag);
                            break;
                        }
                        default:
                            // 对于其他类型，尝试作为字符串复制
                            OGR_F_SetFieldString(newFeature, i, OGR_F_GetFieldAsString(feature, i));
                            break;
                    }
                }
            }

            // 添加要素到目标图层
            OGRErr err = OGR_L_CreateFeature(targetLayer, newFeature);
            if (err == OGRERR_NONE) {
                count++;
            }
            OGR_F_Destroy(newFeature);
        }
        OGR_F_Destroy(feature);
    }

    return count;
}

// 添加一个简单的复制所有要素的函数
int copyAllFeatures(OGRLayerH sourceLayer, OGRLayerH targetLayer) {
    return copyFeaturesWithSpatialFilter(sourceLayer, targetLayer, NULL);
}

// 检查要素几何体是否与分块边界相交（不包含完全在内部的情况）
int isFeatureOnBorder(OGRFeatureH feature, double minX, double minY, double maxX, double maxY, double buffer) {
    if (!feature) return 0;

    OGRGeometryH geom = OGR_F_GetGeometryRef(feature);
    if (!geom) return 0;

    // 创建分块的内部边界（去掉缓冲区）
    OGRGeometryH innerBounds = OGR_G_CreateGeometry(wkbPolygon);
    OGRGeometryH ring = OGR_G_CreateGeometry(wkbLinearRing);

    double innerMinX = minX + buffer;
    double innerMinY = minY + buffer;
    double innerMaxX = maxX - buffer;
    double innerMaxY = maxY - buffer;

    OGR_G_AddPoint_2D(ring, innerMinX, innerMinY);
    OGR_G_AddPoint_2D(ring, innerMaxX, innerMinY);
    OGR_G_AddPoint_2D(ring, innerMaxX, innerMaxY);
    OGR_G_AddPoint_2D(ring, innerMinX, innerMaxY);
    OGR_G_AddPoint_2D(ring, innerMinX, innerMinY);

    OGR_G_AddGeometry(innerBounds, ring);
    OGR_G_DestroyGeometry(ring);

    // 如果要素完全在内部边界内，则不是边界要素
    int isWithin = OGR_G_Within(geom, innerBounds);

    OGR_G_DestroyGeometry(innerBounds);

    // 返回1表示在边界上，0表示完全在内部
    return isWithin ? 0 : 1;
}

// 比较两个几何体的WKT是否完全相同
int geometryWKTEqual(OGRGeometryH geom1, OGRGeometryH geom2) {
    if (!geom1 || !geom2) {
        return geom1 == geom2 ? 1 : 0;
    }

    char *wkt1, *wkt2;
    OGR_G_ExportToWkt(geom1, &wkt1);
    OGR_G_ExportToWkt(geom2, &wkt2);

    int result = (strcmp(wkt1, wkt2) == 0) ? 1 : 0;

    CPLFree(wkt1);
    CPLFree(wkt2);
    return result;
}
OGRGeometryH setPrecisionIfNeeded(OGRGeometryH geom, double gridSize, int flags) {
    if (!geom || gridSize <= 0.0) {
        return geom;
    }

    // 记录原始几何类型
    OGRwkbGeometryType originalType = OGR_G_GetGeometryType(geom);
#if GDAL_VERSION_NUM >= 3110000
    // 设置精度
    OGRGeometryH preciseGeom = OGR_G_SetPrecision(geom, gridSize, flags);
    if (!preciseGeom) {
        return geom;
    }

    // 规范化几何类型
    OGRGeometryH normalizedGeom = normalizeGeometryType(preciseGeom, originalType);

    // 如果规范化成功且不是原几何体，清理精度设置后的几何体
    if (normalizedGeom && normalizedGeom != preciseGeom) {
        OGR_G_DestroyGeometry(preciseGeom);
        return normalizedGeom;
    }

    return preciseGeom;
#else
    // 对于较旧版本的 GDAL，使用替代方案
    // 可以使用 GEOS 库的精度模型或简单返回原几何体
    CPLError(CE_Warning, CPLE_AppDefined,
             "Geometry precision setting requires GDAL 3.11+, current version: %s",
             GDALVersionInfo("RELEASE_NAME"));
    return OGR_G_Clone(geom);
#endif
}


// 为图层中的所有要素设置几何精度
int setLayerGeometryPrecision(OGRLayerH layer, double gridSize, int flags) {
    if (!layer || gridSize <= 0.0) {
        return 0;
    }

    OGR_L_ResetReading(layer);
    OGRFeatureH feature;
    int processedCount = 0;
    int errorCount = 0;

    while ((feature = OGR_L_GetNextFeature(layer)) != NULL) {
        OGRGeometryH geom = OGR_F_GetGeometryRef(feature);
        if (geom) {
            OGRGeometryH preciseGeom = setPrecisionIfNeeded(geom, gridSize, flags);
            if (preciseGeom && preciseGeom != geom) {
                // 设置新的几何体到要素
                OGRErr setGeomErr = OGR_F_SetGeometry(feature, preciseGeom);
                if (setGeomErr == OGRERR_NONE) {
                    // 更新图层中的要素 - 检查返回值
                    OGRErr setFeatureErr = OGR_L_SetFeature(layer, feature);
                    if (setFeatureErr == OGRERR_NONE) {
                        processedCount++;
                    } else {
                        errorCount++;
                        // 可以选择记录错误信息
                        CPLError(CE_Warning, CPLE_AppDefined,
                                "Failed to update feature in layer, error code: %d", (int)setFeatureErr);
                    }
                } else {
                    errorCount++;
                    CPLError(CE_Warning, CPLE_AppDefined,
                            "Failed to set geometry precision for feature, error code: %d", (int)setGeomErr);
                }
                // 清理新创建的几何体
                OGR_G_DestroyGeometry(preciseGeom);
            }
        }
        OGR_F_Destroy(feature);
    }

    OGR_L_ResetReading(layer);

    // 如果有错误，可以通过CPLError报告
    if (errorCount > 0) {
        CPLError(CE_Warning, CPLE_AppDefined,
                "Geometry precision setting completed with %d errors out of %d attempts",
                errorCount, processedCount + errorCount);
    }

    return processedCount;
}

OGRFeatureH setFeatureGeometryPrecision(OGRFeatureH feature, double gridSize, int flags) {
    if (!feature || gridSize <= 0.0) {
        return feature;
    }

    OGRGeometryH geom = OGR_F_GetGeometryRef(feature);
    if (!geom) {
        return feature;
    }

    OGRGeometryH preciseGeom = setPrecisionIfNeeded(geom, gridSize, flags);
    if (preciseGeom && preciseGeom != geom) {
        // 克隆要素
        OGRFeatureH newFeature = OGR_F_Clone(feature);
        if (newFeature) {
            // 设置精确的几何体
            OGRErr err = OGR_F_SetGeometry(newFeature, preciseGeom);
            if (err == OGRERR_NONE) {
                OGR_G_DestroyGeometry(preciseGeom);
                return newFeature;
            } else {
                // 设置几何体失败，清理资源
                CPLError(CE_Warning, CPLE_AppDefined,
                        "Failed to set precision geometry to feature, error code: %d", (int)err);
                OGR_F_Destroy(newFeature);
            }
        }
        OGR_G_DestroyGeometry(preciseGeom);
    }

    return feature;
}
// 强制转换几何类型
OGRGeometryH forceGeometryType(OGRGeometryH geom, OGRwkbGeometryType targetType) {
    if (!geom) return NULL;

    OGRwkbGeometryType currentType = OGR_G_GetGeometryType(geom);

    // 尝试使用GDAL的强制转换功能
    OGRGeometryH convertedGeom = OGR_G_ForceTo(OGR_G_Clone(geom), targetType, NULL);

    if (convertedGeom && OGR_G_GetGeometryType(convertedGeom) == targetType) {
        return convertedGeom;
    }

    // 如果强制转换失败，清理并返回原几何体的克隆
    if (convertedGeom) {
        OGR_G_DestroyGeometry(convertedGeom);
    }

    return OGR_G_Clone(geom);
}
// 合并GeometryCollection中的同类型几何体
OGRGeometryH mergeGeometryCollection(OGRGeometryH geomCollection, OGRwkbGeometryType targetType) {
    if (!geomCollection) return NULL;

    int geomCount = OGR_G_GetGeometryCount(geomCollection);
    if (geomCount == 0) return NULL;

    // 根据目标类型创建相应的Multi几何体
    OGRGeometryH resultGeom = NULL;

    switch (targetType) {
        case wkbMultiPolygon:
        case wkbPolygon:
            resultGeom = OGR_G_CreateGeometry(wkbMultiPolygon);
            break;
        case wkbMultiLineString:
        case wkbLineString:
            resultGeom = OGR_G_CreateGeometry(wkbMultiLineString);
            break;
        case wkbMultiPoint:
        case wkbPoint:
            resultGeom = OGR_G_CreateGeometry(wkbMultiPoint);
            break;
        default:
            return OGR_G_Clone(geomCollection);
    }

    if (!resultGeom) return NULL;

    // 遍历集合中的几何体，添加到结果中
    for (int i = 0; i < geomCount; i++) {
        OGRGeometryH subGeom = OGR_G_GetGeometryRef(geomCollection, i);
        if (subGeom) {
            OGRwkbGeometryType subType = OGR_G_GetGeometryType(subGeom);

            // 检查子几何体类型是否兼容
            if ((targetType == wkbMultiPolygon && (subType == wkbPolygon || subType == wkbMultiPolygon)) ||
                (targetType == wkbMultiLineString && (subType == wkbLineString || subType == wkbMultiLineString)) ||
                (targetType == wkbMultiPoint && (subType == wkbPoint || subType == wkbMultiPoint))) {

                OGRGeometryH clonedSubGeom = OGR_G_Clone(subGeom);
                if (clonedSubGeom) {
                    OGR_G_AddGeometry(resultGeom, clonedSubGeom);
                    OGR_G_DestroyGeometry(clonedSubGeom);
                }
            }
        }
    }

    // 如果结果几何体为空，返回NULL
    if (OGR_G_GetGeometryCount(resultGeom) == 0) {
        OGR_G_DestroyGeometry(resultGeom);
        return NULL;
    }

    return resultGeom;
}
OGRGeometryH normalizeGeometryType(OGRGeometryH geom, OGRwkbGeometryType expectedType) {
    if (!geom) return NULL;

    OGRwkbGeometryType currentType = OGR_G_GetGeometryType(geom);


    if (currentType == wkbGeometryCollection) {
        int geomCount = OGR_G_GetGeometryCount(geom);


        for (int i = 0; i < geomCount; i++) {
            OGRGeometryH subGeom = OGR_G_GetGeometryRef(geom, i);

        }
    }
    // 如果类型已经匹配，直接返回
    if (currentType == expectedType) {
        return geom;
    }

    // 处理GeometryCollection转换为具体类型
    if (currentType == wkbGeometryCollection ||
        currentType == wkbGeometryCollection25D) {

        int geomCount = OGR_G_GetGeometryCount(geom);

        // 如果集合中只有一个几何体，提取它
        if (geomCount == 1) {
            OGRGeometryH subGeom = OGR_G_GetGeometryRef(geom, 0);
            if (subGeom) {
                OGRGeometryH clonedGeom = OGR_G_Clone(subGeom);
                OGRwkbGeometryType subType = OGR_G_GetGeometryType(clonedGeom);

                // 检查子几何体类型是否符合预期
                if (subType == expectedType ||
                    (expectedType == wkbMultiPolygon && subType == wkbPolygon) ||
                    (expectedType == wkbMultiLineString && subType == wkbLineString) ||
                    (expectedType == wkbMultiPoint && subType == wkbPoint)) {
                    return clonedGeom;
                }
                OGR_G_DestroyGeometry(clonedGeom);
            }
        }

        // 如果是多个同类型几何体，尝试合并
        if (geomCount > 1) {
            return mergeGeometryCollection(geom, expectedType);
        }
    }

    // 尝试强制转换类型
    return forceGeometryType(geom, expectedType);
}
// 创建瓦片裁剪几何体（矩形边界）
OGRGeometryH createTileClipGeometry(double minX, double minY, double maxX, double maxY) {
    OGRGeometryH ring = OGR_G_CreateGeometry(wkbLinearRing);
    OGR_G_AddPoint_2D(ring, minX, minY);
    OGR_G_AddPoint_2D(ring, maxX, minY);
    OGR_G_AddPoint_2D(ring, maxX, maxY);
    OGR_G_AddPoint_2D(ring, minX, maxY);
    OGR_G_AddPoint_2D(ring, minX, minY);

    OGRGeometryH polygon = OGR_G_CreateGeometry(wkbPolygon);
    OGR_G_AddGeometry(polygon, ring);
    OGR_G_DestroyGeometry(ring);

    return polygon;
}


// 计算瓦片边界（Web墨卡托坐标，符合Mapbox规范）
void getTileBounds(int x, int y, int zoom, double* minX, double* minY, double* maxX, double* maxY) {
    const double EARTH_RADIUS = 6378137.0;
    const double ORIGIN_SHIFT = M_PI * EARTH_RADIUS;  // 20037508.342789244

    // 🔥 修正：计算单个瓦片的世界尺寸（米）
    double numTiles = pow(2.0, zoom);
    double tileWorldSize = (2.0 * ORIGIN_SHIFT) / numTiles;

    // 计算瓦片边界（XYZ方案）
    *minX = (double)x * tileWorldSize - ORIGIN_SHIFT;
    *maxX = (double)(x + 1) * tileWorldSize - ORIGIN_SHIFT;

    // Y轴：XYZ方案，原点在左上角，Y轴向下
    *maxY = ORIGIN_SHIFT - (double)y * tileWorldSize;
    *minY = ORIGIN_SHIFT - (double)(y + 1) * tileWorldSize;
}





// 重投影数据集到Web墨卡托
GDALDatasetH reprojectToWebMercator(GDALDatasetH hSrcDS) {
    if (!hSrcDS) return NULL;

    // 创建Web墨卡托坐标系
    OGRSpatialReferenceH hDstSRS = OSRNewSpatialReference(NULL);
    OSRImportFromEPSG(hDstSRS, 3857);
    OSRSetAxisMappingStrategy(hDstSRS, OAMS_TRADITIONAL_GIS_ORDER);

    char *pszDstWKT = NULL;
    OSRExportToWkt(hDstSRS, &pszDstWKT);

    // 获取源坐标系
    const char *pszSrcWKT = GDALGetProjectionRef(hSrcDS);

    // 使用AutoCreateWarpedVRT进行重投影
    GDALDatasetH hWarpedDS = GDALAutoCreateWarpedVRT(
        hSrcDS, pszSrcWKT, pszDstWKT,
        GRIORA_Bilinear, 0.125, NULL
    );

    OSRDestroySpatialReference(hDstSRS);
    CPLFree(pszDstWKT);

    return hWarpedDS;
}

// 读取瓦片数据
int readTileData(GDALDatasetH hDS, double minX, double minY, double maxX, double maxY,
                 int tileSize, unsigned char* buffer) {
    if (!hDS || !buffer) return 0;

    double adfGeoTransform[6];
    if (GDALGetGeoTransform(hDS, adfGeoTransform) != CE_None) {
        return 0;
    }

    // 瓦片的世界坐标范围
    double tileWorldWidth = maxX - minX;
    double tileWorldHeight = maxY - minY;

    // 影像的世界坐标范围
    int rasterXSize = GDALGetRasterXSize(hDS);
    int rasterYSize = GDALGetRasterYSize(hDS);

    double imageMinX = adfGeoTransform[0];
    double imageMaxX = adfGeoTransform[0] + rasterXSize * adfGeoTransform[1];
    double imageMaxY = adfGeoTransform[3];
    double imageMinY = adfGeoTransform[3] + rasterYSize * adfGeoTransform[5];

    // 计算交集
    double intersectMinX = fmax(minX, imageMinX);
    double intersectMaxX = fmin(maxX, imageMaxX);
    double intersectMinY = fmax(minY, imageMinY);
    double intersectMaxY = fmin(maxY, imageMaxY);

    if (intersectMinX >= intersectMaxX || intersectMinY >= intersectMaxY) {
        return 0;
    }

    // 🔥 优化1：使用更精确的像素坐标计算
    // 计算交集在影像中的精确像素坐标（浮点数）
    double srcXOffFloat = (intersectMinX - adfGeoTransform[0]) / adfGeoTransform[1];
    double srcYOffFloat = (intersectMaxY - adfGeoTransform[3]) / adfGeoTransform[5];
    double srcXEndFloat = (intersectMaxX - adfGeoTransform[0]) / adfGeoTransform[1];
    double srcYEndFloat = (intersectMinY - adfGeoTransform[3]) / adfGeoTransform[5];

    // 🔥 优化2：使用 floor 和 ceil 确保边界完整
    int srcXOff = (int)floor(srcXOffFloat);
    int srcYOff = (int)floor(srcYOffFloat);
    int srcXEnd = (int)ceil(srcXEndFloat);
    int srcYEnd = (int)ceil(srcYEndFloat);

    int srcXSize = srcXEnd - srcXOff;
    int srcYSize = srcYEnd - srcYOff;

    // 边界检查
    if (srcXOff < 0) { srcXSize += srcXOff; srcXOff = 0; }
    if (srcYOff < 0) { srcYSize += srcYOff; srcYOff = 0; }
    if (srcXOff + srcXSize > rasterXSize) { srcXSize = rasterXSize - srcXOff; }
    if (srcYOff + srcYSize > rasterYSize) { srcYSize = rasterYSize - srcYOff; }

    if (srcXSize <= 0 || srcYSize <= 0) {
        return 0;
    }

    // 🔥 优化3：精确计算目标像素坐标（使用 round 而不是直接转换）
    // 计算交集在瓦片中的精确位置（浮点数）
    double dstXOffFloat = (intersectMinX - minX) / tileWorldWidth * tileSize;
    double dstYOffFloat = (maxY - intersectMaxY) / tileWorldHeight * tileSize;
    double dstXEndFloat = (intersectMaxX - minX) / tileWorldWidth * tileSize;
    double dstYEndFloat = (maxY - intersectMinY) / tileWorldHeight * tileSize;

    // 🔥 优化4：使用四舍五入确保像素对齐
    int dstXOff = (int)round(dstXOffFloat);
    int dstYOff = (int)round(dstYOffFloat);
    int dstXEnd = (int)round(dstXEndFloat);
    int dstYEnd = (int)round(dstYEndFloat);

    int dstXSize = dstXEnd - dstXOff;
    int dstYSize = dstYEnd - dstYOff;

    // 🔥 优化5：确保至少有1个像素
    if (dstXSize < 1) dstXSize = 1;
    if (dstYSize < 1) dstYSize = 1;

    // 边界裁剪
    if (dstXOff < 0) {
        dstXSize += dstXOff;
        dstXOff = 0;
    }
    if (dstYOff < 0) {
        dstYSize += dstYOff;
        dstYOff = 0;
    }
    if (dstXOff + dstXSize > tileSize) {
        dstXSize = tileSize - dstXOff;
    }
    if (dstYOff + dstYSize > tileSize) {
        dstYSize = tileSize - dstYOff;
    }

    if (dstXSize <= 0 || dstYSize <= 0) {
        return 0;
    }

    #ifdef DEBUG
    printf("Tile: [%.6f, %.6f, %.6f, %.6f]\n", minX, minY, maxX, maxY);
    printf("Intersect: [%.6f, %.6f, %.6f, %.6f]\n",
           intersectMinX, intersectMinY, intersectMaxX, intersectMaxY);
    printf("Src: offset=[%d, %d], size=[%d, %d]\n", srcXOff, srcYOff, srcXSize, srcYSize);
    printf("Dst: offset=[%d, %d], size=[%d, %d]\n", dstXOff, dstYOff, dstXSize, dstYSize);
    printf("Dst float: offset=[%.3f, %.3f], end=[%.3f, %.3f]\n",
           dstXOffFloat, dstYOffFloat, dstXEndFloat, dstYEndFloat);
    #endif

    int bandCount = GDALGetRasterCount(hDS);
    if (bandCount < 1) return 0;

    int bands = bandCount > 4 ? 4 : bandCount;

    // 临时缓冲区
    unsigned char* tempBuffer = (unsigned char*)malloc(dstXSize * dstYSize * bands);
    if (!tempBuffer) return 0;

    // 🔥 优化6：使用高质量重采样算法
    for (int i = 0; i < bands; i++) {
        GDALRasterBandH hBand = GDALGetRasterBand(hDS, i + 1);
        if (!hBand) {
            free(tempBuffer);
            return 0;
        }

        // 设置重采样算法为双线性或立方卷积
        GDALRasterIOExtraArg sExtraArg;
        INIT_RASTERIO_EXTRA_ARG(sExtraArg);
        sExtraArg.eResampleAlg = GRIORA_Bilinear;  // 或 GRIORA_Cubic

        CPLErr err = GDALRasterIOEx(
            hBand, GF_Read,
            srcXOff, srcYOff, srcXSize, srcYSize,
            tempBuffer + i * dstXSize * dstYSize,
            dstXSize, dstYSize,
            GDT_Byte,
            0, 0,
            &sExtraArg
        );

        if (err != CE_None) {
            // 如果 GDALRasterIOEx 失败，回退到普通方法
            err = GDALRasterIO(
                hBand, GF_Read,
                srcXOff, srcYOff, srcXSize, srcYSize,
                tempBuffer + i * dstXSize * dstYSize,
                dstXSize, dstYSize,
                GDT_Byte, 0, 0
            );

            if (err != CE_None) {
                free(tempBuffer);
                return 0;
            }
        }
    }

    // 清空buffer（透明背景）
    memset(buffer, 0, tileSize * tileSize * 4);

    // 复制数据到瓦片
    for (int i = 0; i < bands; i++) {
        for (int row = 0; row < dstYSize; row++) {
            int dstRow = dstYOff + row;
            if (dstRow >= tileSize) break;  // 安全检查

            for (int col = 0; col < dstXSize; col++) {
                int dstCol = dstXOff + col;
                if (dstCol >= tileSize) break;  // 安全检查

                int srcIdx = i * dstXSize * dstYSize + row * dstXSize + col;
                int dstIdx = i * tileSize * tileSize + dstRow * tileSize + dstCol;

                buffer[dstIdx] = tempBuffer[srcIdx];
            }
        }
    }

    // 处理灰度图
    if (bands == 1) {
        for (int row = dstYOff; row < dstYOff + dstYSize && row < tileSize; row++) {
            for (int col = dstXOff; col < dstXOff + dstXSize && col < tileSize; col++) {
                int idx = row * tileSize + col;
                unsigned char val = buffer[idx];
                buffer[idx] = val;
                buffer[tileSize * tileSize + idx] = val;
                buffer[2 * tileSize * tileSize + idx] = val;
            }
        }
        bands = 3;
    }

    // 设置Alpha通道
    if (bands == 3) {
        for (int row = dstYOff; row < dstYOff + dstYSize && row < tileSize; row++) {
            for (int col = dstXOff; col < dstXOff + dstXSize && col < tileSize; col++) {
                int idx = row * tileSize + col;
                buffer[3 * tileSize * tileSize + idx] = 255;
            }
        }
        bands = 4;
    }

    free(tempBuffer);
    return bands;
}



int getDatasetInfo(GDALDatasetH hDS, DatasetInfo* info) {
    if (!hDS || !info) return 0;

    info->width = GDALGetRasterXSize(hDS);
    info->height = GDALGetRasterYSize(hDS);
    info->bandCount = GDALGetRasterCount(hDS);

    if (GDALGetGeoTransform(hDS, info->geoTransform) != CE_None) {
        return 0;
    }

    const char* proj = GDALGetProjectionRef(hDS);
    if (proj) {
        strncpy(info->projection, proj, sizeof(info->projection) - 1);
        info->projection[sizeof(info->projection) - 1] = '\0';
    } else {
        info->projection[0] = '\0';
    }

    return 1;
}

// 辅助函数:使用矢量图层裁剪栅格
GDALDatasetH clipRasterByGeometry(GDALDatasetH srcDS, OGRGeometryH geom, double *bounds) {
    if (srcDS == NULL || geom == NULL) return NULL;

    // 获取几何体边界
    OGREnvelope envelope;
    OGR_G_GetEnvelope(geom, &envelope);

    bounds[0] = envelope.MinX;
    bounds[1] = envelope.MinY;
    bounds[2] = envelope.MaxX;
    bounds[3] = envelope.MaxY;

    // 创建临时矢量文件用于裁剪
    const char *pszCutlineFile = "/vsimem/cutline.geojson";
    OGRSFDriverH hDriver = OGRGetDriverByName("GeoJSON");
    if (hDriver == NULL) return NULL;

    OGRDataSourceH hCutlineDS = OGR_Dr_CreateDataSource(hDriver, pszCutlineFile, NULL);
    if (hCutlineDS == NULL) return NULL;

    OGRSpatialReferenceH hSRS = GDALGetSpatialRef(srcDS);
    OGRLayerH hLayer = OGR_DS_CreateLayer(hCutlineDS, "cutline", hSRS, wkbPolygon, NULL);
    if (hLayer == NULL) {
        OGR_DS_Destroy(hCutlineDS);
        VSIUnlink(pszCutlineFile);
        return NULL;
    }

    OGRFeatureDefnH hFDefn = OGR_L_GetLayerDefn(hLayer);
    OGRFeatureH hFeature = OGR_F_Create(hFDefn);
    OGR_F_SetGeometry(hFeature, geom);
    OGR_L_CreateFeature(hLayer, hFeature);
    OGR_F_Destroy(hFeature);
    OGR_DS_Destroy(hCutlineDS);

    // 构建 GDALWarp 选项
    char **papszOptions = NULL;
    papszOptions = CSLAddString(papszOptions, "-of");
    papszOptions = CSLAddString(papszOptions, "MEM");
    papszOptions = CSLAddString(papszOptions, "-cutline");
    papszOptions = CSLAddString(papszOptions, pszCutlineFile);
    papszOptions = CSLAddString(papszOptions, "-crop_to_cutline");
    papszOptions = CSLAddString(papszOptions, "-dstalpha");

    // 创建 GDALWarpAppOptions
    GDALWarpAppOptions *psWarpOptions = GDALWarpAppOptionsNew(papszOptions, NULL);
    CSLDestroy(papszOptions);

    if (psWarpOptions == NULL) {
        VSIUnlink(pszCutlineFile);
        return NULL;
    }

    // 执行裁剪
    GDALDatasetH ahSrcDS[1] = { srcDS };
    int bUsageError = 0;
    GDALDatasetH hDstDS = GDALWarp(
        "",           // 输出到内存
        NULL,         // 不使用已存在的目标数据集
        1,            // 源数据集数量
        ahSrcDS,      // 源数据集数组
        psWarpOptions,
        &bUsageError
    );

    // 清理
    GDALWarpAppOptionsFree(psWarpOptions);
    VSIUnlink(pszCutlineFile);

    return hDstDS;
}

int writeImage(GDALDatasetH ds, const char* filename, const char* format, int quality) {
    if (ds == NULL || filename == NULL || format == NULL) return 0;

    GDALDriverH driver = GDALGetDriverByName(format);
    if (driver == NULL) {
        fprintf(stderr, "Driver '%s' not found\n", format);
        return 0;
    }

    // 检查驱动是否支持 CreateCopy
    char **metadata = GDALGetMetadata(driver, NULL);
    int supportsCreate = CSLFetchBoolean(metadata, GDAL_DCAP_CREATECOPY, FALSE);
    if (!supportsCreate) {
        fprintf(stderr, "Driver '%s' does not support CreateCopy\n", format);
        return 0;
    }

    char **options = NULL;

    // 根据格式设置不同的选项
    if (strcmp(format, "JPEG") == 0 || strcmp(format, "JPG") == 0) {
        char qualityStr[32];
        snprintf(qualityStr, sizeof(qualityStr), "QUALITY=%d", quality);
        options = CSLAddString(options, qualityStr);
    }
    else if (strcmp(format, "PNG") == 0) {
        // PNG 压缩级别 (1-9)
        char compressionStr[32];
        int compression = (quality > 0 && quality <= 100) ? (9 - quality * 9 / 100) : 6;
        snprintf(compressionStr, sizeof(compressionStr), "ZLEVEL=%d", compression);
        options = CSLAddString(options, compressionStr);
    }
    else if (strcmp(format, "TIF") == 0 || strcmp(format, "TIFF") == 0) {
        options = CSLAddString(options, "COMPRESS=LZW");
        options = CSLAddString(options, "TILED=YES");
    }
    else if (strcmp(format, "WEBP") == 0) {
        char qualityStr[32];
        snprintf(qualityStr, sizeof(qualityStr), "QUALITY=%d", quality);
        options = CSLAddString(options, qualityStr);
    }
    else if (strcmp(format, "HFA") == 0) {
        // ERDAS IMAGINE (.img) 格式
        // HFA 支持压缩选项
        options = CSLAddString(options, "COMPRESSED=YES");

        // 可选：设置统计信息
        options = CSLAddString(options, "STATISTICS=YES");

        // 可选：设置金字塔
        // options = CSLAddString(options, "USE_RRD=YES");
    }

    GDALDatasetH outDS = GDALCreateCopy(driver, filename, ds, FALSE, options, NULL, NULL);

    CSLDestroy(options);

    if (outDS != NULL) {
        GDALClose(outDS);
        return 1;
    }
    return 0;
}

// 保留原函数以兼容旧代码
int writeJPEG(GDALDatasetH ds, const char* filename, int quality) {
    // 检查输入参数有效性，防止空指针解引用
    if (filename == NULL) {
        return -1;  // 返回错误码表示文件名为空
    }

    // 从文件名中提取扩展名，查找最后一个'.'的位置
    const char* ext = strrchr(filename, '.');
    char format[16] = "JPEG";  // 默认格式，初始化为JPEG

    // 检查是否找到了文件扩展名
    if (ext != NULL) {
        ext++;  // 跳过 '.' 字符，指向扩展名部分

        // 根据扩展名确定格式，使用strcasecmp进行不区分大小写的比较
        if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) {
            strncpy(format, "JPEG", sizeof(format) - 1);  // 安全复制JPEG格式字符串
            format[sizeof(format) - 1] = '\0';  // 确保字符串null终止
        } else if (strcasecmp(ext, "png") == 0) {
            strncpy(format, "PNG", sizeof(format) - 1);   // 安全复制PNG格式字符串
            format[sizeof(format) - 1] = '\0';   // 确保字符串null终止
        } else if (strcasecmp(ext, "tif") == 0 || strcasecmp(ext, "tiff") == 0) {
            strncpy(format, "GTiff", sizeof(format) - 1); // 安全复制GTiff格式字符串
            format[sizeof(format) - 1] = '\0';  // 确保字符串null终止
        } else if (strcasecmp(ext, "img") == 0) {
            strncpy(format, "HFA", sizeof(format) - 1);   // 安全复制HFA格式字符串
            format[sizeof(format) - 1] = '\0';   // 确保字符串null终止
        } else if (strcasecmp(ext, "webp") == 0) {
            strncpy(format, "WEBP", sizeof(format) - 1);  // 安全复制WEBP格式字符串，修复缩进
            format[sizeof(format) - 1] = '\0';  // 确保字符串null终止
        }
        // 可以根据需要添加更多格式支持
    }

    // 调用writeImage函数执行实际的图像写入操作
    return writeImage(ds, filename, format, quality);
}

// 将数据集写入内存缓冲区并返回二进制数据

/**
 * 将格式字符串标准化为GDAL驱动名称
 * @param format 输入的格式字符串（如"jpg", "tif"等）
 * @param standardFormat 输出的标准化格式字符串缓冲区
 * @param bufferSize 输出缓冲区大小
 */
static void standardizeFormat(const char* format, char* standardFormat, size_t bufferSize) {
    // 检查输入参数有效性
    if (format == NULL || standardFormat == NULL || bufferSize == 0) {
        return;
    }

    // 根据输入格式进行不区分大小写的匹配，转换为标准GDAL驱动名称
    if (strcasecmp(format, "jpg") == 0 || strcasecmp(format, "jpeg") == 0 || strcasecmp(format, "JPEG") == 0) {
        strncpy(standardFormat, "JPEG", bufferSize - 1);  // JPEG格式的标准驱动名
    } else if (strcasecmp(format, "png") == 0 || strcasecmp(format, "PNG") == 0) {
        strncpy(standardFormat, "PNG", bufferSize - 1);   // PNG格式的标准驱动名
    } else if (strcasecmp(format, "tif") == 0 || strcasecmp(format, "tiff") == 0 || strcasecmp(format, "GTiff") == 0) {
        strncpy(standardFormat, "GTiff", bufferSize - 1); // TIFF格式的标准驱动名
    } else if (strcasecmp(format, "img") == 0 || strcasecmp(format, "HFA") == 0) {
        strncpy(standardFormat, "HFA", bufferSize - 1);   // Erdas Imagine格式的标准驱动名
    } else if (strcasecmp(format, "webp") == 0 || strcasecmp(format, "WEBP") == 0) {
        strncpy(standardFormat, "WEBP", bufferSize - 1);  // WEBP格式的标准驱动名
    } else if (strcasecmp(format, "bmp") == 0 || strcasecmp(format, "BMP") == 0) {
        strncpy(standardFormat, "BMP", bufferSize - 1);   // BMP格式的标准驱动名
    } else if (strcasecmp(format, "gif") == 0 || strcasecmp(format, "GIF") == 0) {
        strncpy(standardFormat, "GIF", bufferSize - 1);   // GIF格式的标准驱动名
    } else {
        // 如果无法识别，直接复制原格式字符串
        strncpy(standardFormat, format, bufferSize - 1);
    }

    standardFormat[bufferSize - 1] = '\0';  // 确保字符串null终止
}

ImageBuffer* writeImageToMemory(GDALDatasetH ds, const char* format, int quality) {
    // 检查输入参数的有效性
    if (ds == NULL || format == NULL) return NULL;

    // 标准化格式名称，支持常见的文件扩展名识别
    char standardFormat[32];
    standardizeFormat(format, standardFormat, sizeof(standardFormat));

    // 使用标准化后的格式名称获取GDAL驱动
    GDALDriverH driver = GDALGetDriverByName(standardFormat);
    if (driver == NULL) {
        fprintf(stderr, "Driver '%s' not found\n", standardFormat);  // 输出标准化后的格式名
        return NULL;
    }

    // 获取驱动的元数据信息
    char **metadata = GDALGetMetadata(driver, NULL);
    // 检查驱动是否支持CreateCopy操作
    int supportsCreate = CSLFetchBoolean(metadata, GDAL_DCAP_CREATECOPY, FALSE);
    if (!supportsCreate) {
        fprintf(stderr, "Driver '%s' does not support CreateCopy\n", standardFormat);
        return NULL;
    }

    // 生成唯一的内存文件路径，使用数据集指针和格式名确保唯一性
    char memFilename[256];
    snprintf(memFilename, sizeof(memFilename), "/vsimem/temp_image_%p.%s",
             (void*)ds, standardFormat);

    // 初始化驱动选项列表
    char **options = NULL;

    // 根据标准化后的格式设置不同的选项
    if (strcmp(standardFormat, "JPEG") == 0) {
        char qualityStr[32];
        snprintf(qualityStr, sizeof(qualityStr), "QUALITY=%d", quality);  // 设置JPEG压缩质量
        options = CSLAddString(options, qualityStr);
    }
    else if (strcmp(standardFormat, "PNG") == 0) {
        char compressionStr[32];
        // 将质量参数转换为PNG的压缩级别（0-9），质量越高压缩级别越低
        int compression = (quality > 0 && quality <= 100) ? (9 - quality * 9 / 100) : 6;
        snprintf(compressionStr, sizeof(compressionStr), "ZLEVEL=%d", compression);
        options = CSLAddString(options, compressionStr);
    }
    else if (strcmp(standardFormat, "GTiff") == 0) {
        options = CSLAddString(options, "COMPRESS=LZW");  // 使用LZW压缩
        options = CSLAddString(options, "TILED=YES");     // 启用瓦片存储
    }
    else if (strcmp(standardFormat, "WEBP") == 0) {
        char qualityStr[32];
        snprintf(qualityStr, sizeof(qualityStr), "QUALITY=%d", quality);  // 设置WEBP压缩质量
        options = CSLAddString(options, qualityStr);
    }
    else if (strcmp(standardFormat, "BMP") == 0) {
        // BMP格式通常不需要特殊选项
        // 可以根据需要添加选项
    }

    // 创建到内存文件的数据集副本
    GDALDatasetH outDS = GDALCreateCopy(driver, memFilename, ds, FALSE, options, NULL, NULL);
    CSLDestroy(options);  // 释放选项列表内存

    // 检查数据集创建是否成功
    if (outDS == NULL) {
        return NULL;
    }

    // 关闭输出数据集，确保数据写入内存文件
    GDALClose(outDS);

    // 读取内存文件内容到缓冲区
    vsi_l_offset nDataLength;
    GByte *pabyData = VSIGetMemFileBuffer(memFilename, &nDataLength, FALSE);

    // 检查内存文件读取是否成功
    if (pabyData == NULL || nDataLength == 0) {
        VSIUnlink(memFilename);  // 清理内存文件
        return NULL;
    }

    // 分配返回结构体内存
    ImageBuffer *buffer = (ImageBuffer*)malloc(sizeof(ImageBuffer));
    if (buffer == NULL) {
        VSIUnlink(memFilename);  // 内存分配失败时清理
        return NULL;
    }

    // 复制数据（因为 VSI 内存在 Unlink 后会被释放）
    buffer->data = (unsigned char*)malloc(nDataLength);
    if (buffer->data == NULL) {
        free(buffer);            // 释放已分配的结构体内存
        VSIUnlink(memFilename);  // 清理内存文件
        return NULL;
    }

    // 将内存文件数据复制到返回缓冲区
    memcpy(buffer->data, pabyData, nDataLength);
    buffer->size = nDataLength;  // 设置缓冲区大小

    // 清理内存文件，释放VSI内存
    VSIUnlink(memFilename);

    return buffer;  // 返回包含图像数据的缓冲区
}


// 释放 ImageBuffer
void freeImageBuffer(ImageBuffer *buffer) {
    if (buffer != NULL) {
        if (buffer->data != NULL) {
            free(buffer->data);
        }
        free(buffer);
    }
}

// 使用掩膜方式裁剪像素坐标系栅格
GDALDatasetH clipPixelRasterByMask(GDALDatasetH srcDS, OGRGeometryH geom, double *bounds) {
    if (srcDS == NULL || geom == NULL) return NULL;

    // 获取源数据集信息
    int width = GDALGetRasterXSize(srcDS);
    int height = GDALGetRasterYSize(srcDS);
    int bandCount = GDALGetRasterCount(srcDS);

    if (bandCount == 0) {

        return NULL;
    }

    // 获取几何体边界(像素坐标)
    OGREnvelope envelope;
    OGR_G_GetEnvelope(geom, &envelope);


    // 计算裁剪区域(确保在图像范围内)
    int minX = (int)floor(envelope.MinX);
    int minY = (int)floor(envelope.MinY);
    int maxX = (int)ceil(envelope.MaxX);
    int maxY = (int)ceil(envelope.MaxY);

    // 边界检查
    if (minX < 0) minX = 0;
    if (minY < 0) minY = 0;
    if (maxX > width) maxX = width;
    if (maxY > height) maxY = height;

    int clipWidth = maxX - minX;
    int clipHeight = maxY - minY;


    if (clipWidth <= 0 || clipHeight <= 0) {
        fprintf(stderr, "Invalid clip dimensions: %dx%d\n", clipWidth, clipHeight);
        return NULL;
    }

    // 保存边界信息
    bounds[0] = minX;
    bounds[1] = minY;
    bounds[2] = maxX;
    bounds[3] = maxY;

    // 创建内存数据集用于掩膜
    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    if (memDriver == NULL) {
        fprintf(stderr, "MEM driver not available\n");
        return NULL;
    }

    // 创建掩膜数据集(单波段,字节类型)
    GDALDatasetH maskDS = GDALCreate(memDriver, "", clipWidth, clipHeight, 1, GDT_Byte, NULL);
    if (maskDS == NULL) {
        fprintf(stderr, "Failed to create mask dataset\n");
        return NULL;
    }

    // 设置掩膜的地理变换(像素坐标系)
    // 关键修改：使用正确的Y轴方向
    double maskGeoTransform[6] = {
        (double)minX,  // 左上角X
        1.0,           // X方向像素大小
        0.0,           // 旋转
        (double)minY,  // 左上角Y
        0.0,           // 旋转
        1.0            // Y方向像素大小 (正值，因为几何体坐标也是像素坐标)
    };
    GDALSetGeoTransform(maskDS, maskGeoTransform);

    // 获取掩膜波段
    GDALRasterBandH maskBand = GDALGetRasterBand(maskDS, 1);
    if (maskBand == NULL) {
        GDALClose(maskDS);
        fprintf(stderr, "Failed to get mask band\n");
        return NULL;
    }

    // 初始化掩膜为0
    unsigned char *zeroBuffer = (unsigned char*)calloc(clipWidth * clipHeight, sizeof(unsigned char));
    if (zeroBuffer == NULL) {
        GDALClose(maskDS);

        return NULL;
    }
    CPLErr err = GDALRasterIO(maskBand, GF_Write, 0, 0, clipWidth, clipHeight,
                              zeroBuffer, clipWidth, clipHeight, GDT_Byte, 0, 0);
    free(zeroBuffer);

    if (err != CE_None) {
        GDALClose(maskDS);
        fprintf(stderr, "Failed to initialize mask\n");
        return NULL;
    }

    // 创建临时矢量图层用于栅格化
    const char *pszTempVector = "/vsimem/temp_vector.geojson";
    OGRSFDriverH vecDriver = OGRGetDriverByName("Memory");  // 使用 Memory 驱动更快
    if (vecDriver == NULL) {
        vecDriver = OGRGetDriverByName("GeoJSON");  // 备用
        if (vecDriver == NULL) {
            GDALClose(maskDS);
            fprintf(stderr, "No vector driver available\n");
            return NULL;
        }
    }

    OGRDataSourceH vecDS = OGR_Dr_CreateDataSource(vecDriver, pszTempVector, NULL);
    if (vecDS == NULL) {
        GDALClose(maskDS);
        fprintf(stderr, "Failed to create temporary vector\n");
        return NULL;
    }

    // 创建图层(不设置空间参考,因为是像素坐标)
    OGRLayerH vecLayer = OGR_DS_CreateLayer(vecDS, "mask", NULL, wkbPolygon, NULL);
    if (vecLayer == NULL) {
        OGR_DS_Destroy(vecDS);
        GDALClose(maskDS);
        VSIUnlink(pszTempVector);
        fprintf(stderr, "Failed to create vector layer\n");
        return NULL;
    }

    // 创建要素并添加几何体
    OGRFeatureDefnH featureDefn = OGR_L_GetLayerDefn(vecLayer);
    OGRFeatureH vecFeature = OGR_F_Create(featureDefn);
    if (vecFeature == NULL) {
        OGR_DS_Destroy(vecDS);
        GDALClose(maskDS);
        VSIUnlink(pszTempVector);
        fprintf(stderr, "Failed to create feature\n");
        return NULL;
    }

    // 克隆几何体以避免修改原始几何体
    OGRGeometryH clonedGeom = OGR_G_Clone(geom);
    OGR_F_SetGeometry(vecFeature, clonedGeom);

    if (OGR_L_CreateFeature(vecLayer, vecFeature) != OGRERR_NONE) {
        OGR_G_DestroyGeometry(clonedGeom);
        OGR_F_Destroy(vecFeature);
        OGR_DS_Destroy(vecDS);
        GDALClose(maskDS);
        VSIUnlink(pszTempVector);
        fprintf(stderr, "Failed to add feature to layer\n");
        return NULL;
    }
    OGR_G_DestroyGeometry(clonedGeom);
    OGR_F_Destroy(vecFeature);

    // 栅格化矢量到掩膜(值为255)
    int bandList[1] = {1};
    double burnValue[1] = {255.0};
    char **rasterizeOptions = NULL;
    rasterizeOptions = CSLSetNameValue(rasterizeOptions, "ALL_TOUCHED", "TRUE");


    CPLErr rasterizeErr = GDALRasterizeLayers(maskDS, 1, bandList, 1, &vecLayer,
                                               NULL, NULL, burnValue, rasterizeOptions, NULL, NULL);
    CSLDestroy(rasterizeOptions);
    OGR_DS_Destroy(vecDS);
    VSIUnlink(pszTempVector);

    if (rasterizeErr != CE_None) {
        GDALClose(maskDS);
        fprintf(stderr, "Failed to rasterize geometry\n");
        return NULL;
    }

    // 验证掩膜是否有效（检查是否有非零像素）
    unsigned char *maskData = (unsigned char*)malloc(clipWidth * clipHeight * sizeof(unsigned char));
    if (maskData == NULL) {
        GDALClose(maskDS);
        fprintf(stderr, "Failed to allocate mask data buffer\n");
        return NULL;
    }

    err = GDALRasterIO(maskBand, GF_Read, 0, 0, clipWidth, clipHeight,
                       maskData, clipWidth, clipHeight, GDT_Byte, 0, 0);
    if (err != CE_None) {
        free(maskData);
        GDALClose(maskDS);
        fprintf(stderr, "Failed to read mask data\n");
        return NULL;
    }

    // 统计掩膜中的有效像素
    int validPixels = 0;
    for (int i = 0; i < clipWidth * clipHeight; i++) {
        if (maskData[i] > 0) {
            validPixels++;
        }
    }


    if (validPixels == 0) {
        free(maskData);
        GDALClose(maskDS);
        fprintf(stderr, "WARNING: Mask has no valid pixels!\n");
        return NULL;
    }

    // 创建输出数据集
    GDALDatasetH outputDS = GDALCreate(memDriver, "", clipWidth, clipHeight, bandCount, GDT_Byte, NULL);
    if (outputDS == NULL) {
        free(maskData);
        GDALClose(maskDS);
        fprintf(stderr, "Failed to create output dataset\n");
        return NULL;
    }

    // 设置输出数据集的地理变换
    GDALSetGeoTransform(outputDS, maskGeoTransform);

    // 对每个波段应用掩膜
    for (int b = 1; b <= bandCount; b++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(srcDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outputDS, b);

        if (srcBand == NULL || dstBand == NULL) {
            fprintf(stderr, "Failed to get band %d\n", b);
            continue;
        }

        // 读取源数据
        unsigned char *srcData = (unsigned char*)malloc(clipWidth * clipHeight * sizeof(unsigned char));
        if (srcData == NULL) {
            fprintf(stderr, "Failed to allocate source data buffer for band %d\n", b);
            continue;
        }

        err = GDALRasterIO(srcBand, GF_Read, minX, minY, clipWidth, clipHeight,
                           srcData, clipWidth, clipHeight, GDT_Byte, 0, 0);
        if (err != CE_None) {
            free(srcData);
            fprintf(stderr, "Failed to read source data for band %d\n", b);
            continue;
        }

        // 应用掩膜(掩膜为0的地方设为0,掩膜>0的地方保留原值)
        for (int i = 0; i < clipWidth * clipHeight; i++) {
            if (maskData[i] == 0) {
                srcData[i] = 0;
            }
        }

        // 写入输出数据
        err = GDALRasterIO(dstBand, GF_Write, 0, 0, clipWidth, clipHeight,
                           srcData, clipWidth, clipHeight, GDT_Byte, 0, 0);
        free(srcData);

        if (err != CE_None) {
            fprintf(stderr, "Failed to write output data for band %d\n", b);
        }
    }

    free(maskData);
    GDALClose(maskDS);


    return outputDS;
}