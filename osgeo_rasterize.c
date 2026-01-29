#include "osgeo_rasterize.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <png.h>

CPLErr RasterizeSingleColorC(
    GDALDatasetH rasterDS,
    OGRLayerH layer,
    RGBA color,
    int tileSize)
{
    if (rasterDS == NULL || layer == NULL) {
        return CE_Failure;
    }

    // 重置图层读取
    OGR_L_ResetReading(layer);

    double burnValues[4] = {
        (double)color.r,
        (double)color.g,
        (double)color.b,
        (double)color.a
    };

    int bands[4] = {1, 2, 3, 4};

    // 创建选项列表
    char **options = NULL;
    options = CSLSetNameValue(options, "ALL_TOUCHED", "TRUE");

    // 执行栅格化
    CPLErr err = GDALRasterizeLayers(
        rasterDS,
        4,
        bands,
        1,
        &layer,
        NULL,
        NULL,
        burnValues,
        options,
        NULL,
        NULL
    );

    CSLDestroy(options);
    OGR_L_ResetReading(layer);

    return err;
}

CPLErr RasterizeByAttributeC(
    GDALDatasetH rasterDS,
    OGRLayerH layer,
    const char *attName,
    int colorCount,
    const char **attrValues,
    RGBA *colors,
    int tileSize)
{
    if (rasterDS == NULL || layer == NULL || attName == NULL) {
        return CE_Failure;
    }

    for (int i = 0; i < colorCount; i++) {
        // 重置读取游标
        OGR_L_ResetReading(layer);

        // 构建WHERE子句
        char whereClause[1024];
        snprintf(whereClause, sizeof(whereClause), "%s = '%s'", attName, attrValues[i]);

        // 设置属性过滤器
        OGR_L_SetAttributeFilter(layer, whereClause);

        double burnValues[4] = {
            (double)colors[i].r,
            (double)colors[i].g,
            (double)colors[i].b,
            (double)colors[i].a
        };

        int bands[4] = {1, 2, 3, 4};

        char **options = NULL;
        options = CSLSetNameValue(options, "ALL_TOUCHED", "TRUE");

        CPLErr err = GDALRasterizeLayers(
            rasterDS,
            4,
            bands,
            1,
            &layer,
            NULL,
            NULL,
            burnValues,
            options,
            NULL,
            NULL
        );

        CSLDestroy(options);

        if (err != CE_None) {
            OGR_L_SetAttributeFilter(layer, NULL);
            OGR_L_ResetReading(layer);
            return err;
        }
    }

    // 清除过滤器并重置读取
    OGR_L_SetAttributeFilter(layer, NULL);
    OGR_L_ResetReading(layer);

    return CE_None;
}

GDALDatasetH CreateRasterDatasetC(
    GDALDriverH driver,
    int tileSize,
    VectorTileBounds bounds,
    double *pixelWidth,
    double *pixelHeight)
{
    if (driver == NULL) {
        return NULL;
    }

    *pixelWidth = (bounds.maxLon - bounds.minLon) / tileSize;
    *pixelHeight = (bounds.maxLat - bounds.minLat) / tileSize;

    GDALDatasetH rasterDS = GDALCreate(
        driver,
        "",
        tileSize,
        tileSize,
        4,
        GDT_Byte,
        NULL
    );

    return rasterDS;
}

void InitializeRasterBandsC(GDALDatasetH rasterDS, int tileSize)
{
    if (rasterDS == NULL) {
        return;
    }

    for (int band = 1; band <= 4; band++) {
        GDALRasterBandH rasterBand = GDALGetRasterBand(rasterDS, band);
        if (rasterBand != NULL) {
            unsigned char *buffer = (unsigned char *)CPLMalloc(tileSize * tileSize);
            memset(buffer, 0, tileSize * tileSize);

            GDALRasterIO(
                rasterBand,
                GF_Write,
                0, 0,
                tileSize, tileSize,
                buffer,
                tileSize, tileSize,
                GDT_Byte,
                0, 0
            );

            CPLFree(buffer);
        }
    }
}

void SetGeoTransformAndProjectionC(
    GDALDatasetH rasterDS,
    VectorTileBounds bounds,
    double pixelWidth,
    double pixelHeight)
{
    if (rasterDS == NULL) {
        return;
    }

    double geoTransform[6] = {
        bounds.minLon,
        pixelWidth,
        0,
        bounds.maxLat,
        0,
        -pixelHeight
    };

    GDALSetGeoTransform(rasterDS, geoTransform);

    // 设置投影
    OGRSpatialReferenceH srs = OSRNewSpatialReference(NULL);
    if (srs != NULL) {
        OSRImportFromEPSG(srs, 4326);
        char *wkt = NULL;
        if (OSRExportToWkt(srs, &wkt) == OGRERR_NONE && wkt != NULL) {
            GDALSetProjection(rasterDS, wkt);
            CPLFree(wkt);
        }
        OSRDestroySpatialReference(srs);
    }
}

// PNG编码辅助结构
typedef struct {
    unsigned char *data;
    int size;
    int capacity;
} PNGBuffer;

static void png_write_callback(png_structp png_ptr, png_bytep data, png_size_t length)
{
    PNGBuffer *buf = (PNGBuffer *)png_get_io_ptr(png_ptr);
    if (buf->size + length > buf->capacity) {
        buf->capacity = (buf->size + length) * 2;
        buf->data = (unsigned char *)realloc(buf->data, buf->capacity);
    }
    memcpy(buf->data + buf->size, data, length);
    buf->size += length;
}

void *RasterToPNGC(GDALDatasetH rasterDS, int tileSize, int *pngSize)
{
    if (rasterDS == NULL || pngSize == NULL) {
        return NULL;
    }

    // 读取栅格数据
    unsigned char *imageData = (unsigned char *)CPLMalloc(tileSize * tileSize * 4);
    if (imageData == NULL) {
        return NULL;
    }

    // 逐波段读取
    for (int band = 1; band <= 4; band++) {
        GDALRasterBandH rasterBand = GDALGetRasterBand(rasterDS, band);
        if (rasterBand == NULL) {
            CPLFree(imageData);
            return NULL;
        }

        unsigned char *bandData = (unsigned char *)CPLMalloc(tileSize * tileSize);
        if (bandData == NULL) {
            CPLFree(imageData);
            return NULL;
        }

        CPLErr err = GDALRasterIO(
            rasterBand,
            GF_Read,
            0, 0,
            tileSize, tileSize,
            bandData,
            tileSize, tileSize,
            GDT_Byte,
            0, 0
        );

        if (err != CE_None) {
            CPLFree(bandData);
            CPLFree(imageData);
            return NULL;
        }

        // 填充到RGBA图像
        for (int i = 0; i < tileSize * tileSize; i++) {
            imageData[i * 4 + band - 1] = bandData[i];
        }

        CPLFree(bandData);
    }

    // 使用libpng编码为PNG
    PNGBuffer pngBuffer = {
        .data = (unsigned char *)CPLMalloc(1024 * 1024),
        .size = 0,
        .capacity = 1024 * 1024
    };

    png_structp png = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if (!png) {
        CPLFree(imageData);
        CPLFree(pngBuffer.data);
        return NULL;
    }

    png_infop info = png_create_info_struct(png);
    if (!info) {
        png_destroy_write_struct(&png, NULL);
        CPLFree(imageData);
        CPLFree(pngBuffer.data);
        return NULL;
    }

    if (setjmp(png_jmpbuf(png))) {
        png_destroy_write_struct(&png, &info);
        CPLFree(imageData);
        CPLFree(pngBuffer.data);
        return NULL;
    }

    png_set_write_fn(png, &pngBuffer, png_write_callback, NULL);

    png_set_IHDR(
        png,
        info,
        tileSize,
        tileSize,
        8,
        PNG_COLOR_TYPE_RGBA,
        PNG_INTERLACE_NONE,
        PNG_COMPRESSION_TYPE_DEFAULT,
        PNG_FILTER_TYPE_DEFAULT
    );

    png_write_info(png, info);

    png_bytep *row_pointers = (png_bytep *)CPLMalloc(sizeof(png_bytep) * tileSize);
    for (int y = 0; y < tileSize; y++) {
        row_pointers[y] = imageData + y * tileSize * 4;
    }

    png_write_image(png, row_pointers);
    png_write_end(png, NULL);

    png_destroy_write_struct(&png, &info);
    CPLFree(row_pointers);
    CPLFree(imageData);

    *pngSize = pngBuffer.size;
    return pngBuffer.data;
}
