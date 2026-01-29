#ifndef OSGEO_RASTERIZE_H
#define OSGEO_RASTERIZE_H

#include "gdal.h"
#include "ogr_api.h"
#include "cpl_conv.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int r;
    int g;
    int b;
    int a;
} RGBA;

typedef struct {
    double minLon;
    double minLat;
    double maxLon;
    double maxLat;
} VectorTileBounds;

// 单色栅格化
CPLErr RasterizeSingleColorC(
    GDALDatasetH rasterDS,
    OGRLayerH layer,
    RGBA color,
    int tileSize
);

// 按属性栅格化
CPLErr RasterizeByAttributeC(
    GDALDatasetH rasterDS,
    OGRLayerH layer,
    const char *attName,
    int colorCount,
    const char **attrValues,
    RGBA *colors,
    int tileSize
);

// 创建栅格数据集
GDALDatasetH CreateRasterDatasetC(
    GDALDriverH driver,
    int tileSize,
    VectorTileBounds bounds,
    double *pixelWidth,
    double *pixelHeight
);

// 初始化栅格波段为透明
void InitializeRasterBandsC(GDALDatasetH rasterDS, int tileSize);

// 设置地理变换和投影
void SetGeoTransformAndProjectionC(
    GDALDatasetH rasterDS,
    VectorTileBounds bounds,
    double pixelWidth,
    double pixelHeight
);

// 栅格转PNG
void *RasterToPNGC(GDALDatasetH rasterDS, int tileSize, int *pngSize);

#ifdef __cplusplus
}
#endif

#endif // OSGEO_RASTERIZE_H
