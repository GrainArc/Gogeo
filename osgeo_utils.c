/*
Copyright (C) 2025 [fmecool]

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

