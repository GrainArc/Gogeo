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

    // 🔥 第一步：计算瓦片的世界坐标范围
    double tileWorldWidth = maxX - minX;
    double tileWorldHeight = maxY - minY;

    // 🔥 第二步：获取影像的世界坐标范围
    int rasterXSize = GDALGetRasterXSize(hDS);
    int rasterYSize = GDALGetRasterYSize(hDS);

    double imageMinX = adfGeoTransform[0];
    double imageMaxX = adfGeoTransform[0] + rasterXSize * adfGeoTransform[1];
    double imageMaxY = adfGeoTransform[3];
    double imageMinY = adfGeoTransform[3] + rasterYSize * adfGeoTransform[5];  // 注意：[5]是负数

    // 🔥 第三步：计算瓦片和影像的交集
    double intersectMinX = fmax(minX, imageMinX);
    double intersectMaxX = fmin(maxX, imageMaxX);
    double intersectMinY = fmax(minY, imageMinY);
    double intersectMaxY = fmin(maxY, imageMaxY);

    // 检查是否有交集
    if (intersectMinX >= intersectMaxX || intersectMinY >= intersectMaxY) {
        return 0;  // 无交集，返回空瓦片
    }

    // 🔥 第四步：计算交集在影像中的像素坐标
    int srcXOff = (int)((intersectMinX - adfGeoTransform[0]) / adfGeoTransform[1]);
    int srcYOff = (int)((intersectMaxY - adfGeoTransform[3]) / adfGeoTransform[5]);
    int srcXSize = (int)((intersectMaxX - intersectMinX) / adfGeoTransform[1]);
    int srcYSize = (int)((intersectMinY - intersectMaxY) / adfGeoTransform[5]);

    // 边界检查和裁剪
    if (srcXOff < 0) { srcXSize += srcXOff; srcXOff = 0; }
    if (srcYOff < 0) { srcYSize += srcYOff; srcYOff = 0; }
    if (srcXOff + srcXSize > rasterXSize) { srcXSize = rasterXSize - srcXOff; }
    if (srcYOff + srcYSize > rasterYSize) { srcYSize = rasterYSize - srcYOff; }

    if (srcXSize <= 0 || srcYSize <= 0) {
        return 0;
    }

    // 🔥 第五步：计算交集在瓦片中的像素坐标
    int dstXOff = (int)((intersectMinX - minX) / tileWorldWidth * tileSize);
    int dstYOff = (int)((maxY - intersectMaxY) / tileWorldHeight * tileSize);  // 注意Y轴方向
    int dstXSize = (int)((intersectMaxX - intersectMinX) / tileWorldWidth * tileSize);
    int dstYSize = (int)((intersectMaxY - intersectMinY) / tileWorldHeight * tileSize);

    // 确保目标尺寸在瓦片范围内
    if (dstXOff < 0) { dstXOff = 0; }
    if (dstYOff < 0) { dstYOff = 0; }
    if (dstXOff + dstXSize > tileSize) { dstXSize = tileSize - dstXOff; }
    if (dstYOff + dstYSize > tileSize) { dstYSize = tileSize - dstYOff; }

    if (dstXSize <= 0 || dstYSize <= 0) {
        return 0;
    }

    #ifdef DEBUG
    printf("Tile world bounds: [%.2f, %.2f, %.2f, %.2f]\n", minX, minY, maxX, maxY);
    printf("Intersect bounds: [%.2f, %.2f, %.2f, %.2f]\n",
           intersectMinX, intersectMinY, intersectMaxX, intersectMaxY);
    printf("Source: offset=[%d, %d], size=[%d, %d]\n", srcXOff, srcYOff, srcXSize, srcYSize);
    printf("Dest: offset=[%d, %d], size=[%d, %d]\n", dstXOff, dstYOff, dstXSize, dstYSize);
    #endif

    // 🔥 第六步：创建临时缓冲区读取影像数据
    int bandCount = GDALGetRasterCount(hDS);
    if (bandCount < 1) return 0;

    int bands = bandCount > 4 ? 4 : bandCount;

    // 为重采样创建临时缓冲区
    unsigned char* tempBuffer = (unsigned char*)malloc(dstXSize * dstYSize * bands);
    if (!tempBuffer) return 0;

    // 🔥 第七步：从影像读取数据并重采样到目标尺寸
    for (int i = 0; i < bands; i++) {
        GDALRasterBandH hBand = GDALGetRasterBand(hDS, i + 1);
        if (!hBand) {
            free(tempBuffer);
            return 0;
        }

        CPLErr err = GDALRasterIO(
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

    // 🔥 第八步：将重采样后的数据复制到瓦片的正确位置
    // 先清空buffer（设为透明）
    memset(buffer, 0, tileSize * tileSize * 4);

    for (int i = 0; i < bands; i++) {
        for (int row = 0; row < dstYSize; row++) {
            for (int col = 0; col < dstXSize; col++) {
                int srcIdx = i * dstXSize * dstYSize + row * dstXSize + col;
                int dstRow = dstYOff + row;
                int dstCol = dstXOff + col;
                int dstIdx = i * tileSize * tileSize + dstRow * tileSize + dstCol;

                buffer[dstIdx] = tempBuffer[srcIdx];
            }
        }
    }

    // 🔥 第九步：处理单波段（灰度）转RGB
    if (bands == 1) {
        for (int row = dstYOff; row < dstYOff + dstYSize; row++) {
            for (int col = dstXOff; col < dstXOff + dstXSize; col++) {
                int idx = row * tileSize + col;
                unsigned char val = buffer[idx];
                buffer[idx] = val;                          // R
                buffer[tileSize * tileSize + idx] = val;    // G
                buffer[2 * tileSize * tileSize + idx] = val; // B
            }
        }
        bands = 3;
    }

    // 🔥 第十步：设置Alpha通道
    if (bands == 3) {
        for (int row = dstYOff; row < dstYOff + dstYSize; row++) {
            for (int col = dstXOff; col < dstXOff + dstXSize; col++) {
                int idx = row * tileSize + col;
                buffer[3 * tileSize * tileSize + idx] = 255;  // 不透明
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