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
#include <gdal_alg.h>
#include <gdalwarper.h> 
#include <ogr_api.h>
#include <ogr_srs_api.h>
#include <cpl_error.h>
#include <cpl_conv.h>
#include <cpl_string.h>
#include <stdlib.h>
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

#ifdef __cplusplus
}
#endif

#endif // OSGEO_UTILS_H
