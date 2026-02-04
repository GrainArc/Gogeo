// osgeo_color.c
#include "osgeo_color.h"
#include "osgeo_utils.h"
#include <stdio.h>
#include <float.h>

// ==================== 辅助函数实现 ====================

static inline double clamp(double value, double min, double max) {
    if (value < min) return min;
    if (value > max) return max;
    return value;
}

static inline int clampInt(int value, int min, int max) {
    if (value < min) return min;
    if (value > max) return max;
    return value;
}

// RGB转HSL
void rgbToHsl(double r, double g, double b, double* h, double* s, double* l) {
    r /= 255.0;
    g /= 255.0;
    b /= 255.0;

    double max = fmax(fmax(r, g), b);
    double min = fmin(fmin(r, g), b);
    double delta = max - min;

    *l = (max + min) / 2.0;

    if (delta == 0) {
        *h = 0;
        *s = 0;
    } else {
        *s = *l > 0.5 ? delta / (2.0 - max - min) : delta / (max + min);

        if (max == r) {
            *h = fmod((g - b) / delta + (g < b ? 6 : 0), 6.0);
        } else if (max == g) {
            *h = (b - r) / delta + 2.0;
        } else {
            *h = (r - g) / delta + 4.0;
        }
        *h *= 60.0;
    }
}

// HSL转RGB
void hslToRgb(double h, double s, double l, double* r, double* g, double* b) {
    if (s == 0) {
        *r = *g = *b = l * 255.0;
        return;
    }

    double c = (1.0 - fabs(2.0 * l - 1.0)) * s;
    double x = c * (1.0 - fabs(fmod(h / 60.0, 2.0) - 1.0));
    double m = l - c / 2.0;

    double r1, g1, b1;

    if (h < 60) {
        r1 = c; g1 = x; b1 = 0;
    } else if (h < 120) {
        r1 = x; g1 = c; b1 = 0;
    } else if (h < 180) {
        r1 = 0; g1 = c; b1 = x;
    } else if (h < 240) {
        r1 = 0; g1 = x; b1 = c;
    } else if (h < 300) {
        r1 = x; g1 = 0; b1 = c;
    } else {
        r1 = c; g1 = 0; b1 = x;
    }

    *r = (r1 + m) * 255.0;
    *g = (g1 + m) * 255.0;
    *b = (b1 + m) * 255.0;
}

// RGB转HSV
void rgbToHsv(double r, double g, double b, double* h, double* s, double* v) {
    r /= 255.0;
    g /= 255.0;
    b /= 255.0;

    double max = fmax(fmax(r, g), b);
    double min = fmin(fmin(r, g), b);
    double delta = max - min;

    *v = max;
    *s = max == 0 ? 0 : delta / max;

    if (delta == 0) {
        *h = 0;
    } else if (max == r) {
        *h = 60.0 * fmod((g - b) / delta + 6.0, 6.0);
    } else if (max == g) {
        *h = 60.0 * ((b - r) / delta + 2.0);
    } else {
        *h = 60.0 * ((r - g) / delta + 4.0);
    }
}

// HSV转RGB
void hsvToRgb(double h, double s, double v, double* r, double* g, double* b) {
    if (s == 0) {
        *r = *g = *b = v * 255.0;
        return;
    }

    double c = v * s;
    double x = c * (1.0 - fabs(fmod(h / 60.0, 2.0) - 1.0));
    double m = v - c;

    double r1, g1, b1;

    if (h < 60) {
        r1 = c; g1 = x; b1 = 0;
    } else if (h < 120) {
        r1 = x; g1 = c; b1 = 0;
    } else if (h < 180) {
        r1 = 0; g1 = c; b1 = x;
    } else if (h < 240) {
        r1 = 0; g1 = x; b1 = c;
    } else if (h < 300) {
        r1 = x; g1 = 0; b1 = c;
    } else {
        r1 = c; g1 = 0; b1 = x;
    }

    *r = (r1 + m) * 255.0;
    *g = (g1 + m) * 255.0;
    *b = (b1 + m) * 255.0;
}

// 创建Gamma查找表
unsigned char* createGammaLUT(double gamma) {
    unsigned char* lut = (unsigned char*)malloc(256);
    if (!lut) return NULL;

    double invGamma = 1.0 / gamma;
    for (int i = 0; i < 256; i++) {
        lut[i] = (unsigned char)clamp(pow(i / 255.0, invGamma) * 255.0, 0, 255);
    }
    return lut;
}

// 创建色阶查找表
unsigned char* createLUT(LevelsParams* params) {
    unsigned char* lut = (unsigned char*)malloc(256);
    if (!lut) return NULL;

    double inputRange = params->inputMax - params->inputMin;
    double outputRange = params->outputMax - params->outputMin;

    for (int i = 0; i < 256; i++) {
        double normalized;
        if (i <= params->inputMin) {
            normalized = 0.0;
        } else if (i >= params->inputMax) {
            normalized = 1.0;
        } else {
            normalized = (i - params->inputMin) / inputRange;
        }

        // 应用中间调调整
        normalized = pow(normalized, 1.0 / params->midtone);

        // 映射到输出范围
        double output = params->outputMin + normalized * outputRange;
        lut[i] = (unsigned char)clamp(output, 0, 255);
    }
    return lut;
}

// 三次样条插值
static double cubicInterpolate(double p0, double p1, double p2, double p3, double t) {
    double a = -0.5 * p0 + 1.5 * p1 - 1.5 * p2 + 0.5 * p3;
    double b = p0 - 2.5 * p1 + 2.0 * p2 - 0.5 * p3;
    double c = -0.5 * p0 + 0.5 * p2;
    double d = p1;
    return a * t * t * t + b * t * t + c * t + d;
}

// 创建曲线查找表
unsigned char* createCurveLUT(CurveParams* params) {
    unsigned char* lut = (unsigned char*)malloc(256);
    if (!lut) return NULL;

    if (params->pointCount < 2) {
        for (int i = 0; i < 256; i++) lut[i] = i;
        return lut;
    }

    // 对控制点排序
    for (int i = 0; i < params->pointCount - 1; i++) {
        for (int j = i + 1; j < params->pointCount; j++) {
            if (params->points[j].input < params->points[i].input) {
                CurvePoint temp = params->points[i];
                params->points[i] = params->points[j];
                params->points[j] = temp;
            }
        }
    }

    // 使用样条插值生成LUT
    for (int i = 0; i < 256; i++) {
        double x = (double)i;

        // 找到x所在的区间
        int idx = 0;
        for (int j = 0; j < params->pointCount - 1; j++) {
            if (x >= params->points[j].input && x <= params->points[j + 1].input) {
                idx = j;
                break;
            }
        }

        // 边界处理
        if (x <= params->points[0].input) {
            lut[i] = (unsigned char)clamp(params->points[0].output, 0, 255);
            continue;
        }
        if (x >= params->points[params->pointCount - 1].input) {
            lut[i] = (unsigned char)clamp(params->points[params->pointCount - 1].output, 0, 255);
            continue;
        }

        // 获取插值所需的4个点
        int i0 = (idx > 0) ? idx - 1 : idx;
        int i1 = idx;
        int i2 = idx + 1;
        int i3 = (idx + 2 < params->pointCount) ? idx + 2 : idx + 1;

        double t = (x - params->points[i1].input) /
                   (params->points[i2].input - params->points[i1].input);

        double y = cubicInterpolate(
            params->points[i0].output,
            params->points[i1].output,
            params->points[i2].output,
            params->points[i3].output,
            t
        );

        lut[i] = (unsigned char)clamp(y, 0, 255);
    }

    return lut;
}

// 计算直方图
int* calculateHistogram(GDALDatasetH hDS, int bandIndex, ReferenceRegion* region) {
    int* histogram = (int*)calloc(256, sizeof(int));
    if (!histogram) return NULL;

    GDALRasterBandH band = GDALGetRasterBand(hDS, bandIndex);
    if (!band) {
        free(histogram);
        return NULL;
    }

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);

    int startX = 0, startY = 0, regionWidth = width, regionHeight = height;
    if (region) {
        startX = region->x;
        startY = region->y;
        regionWidth = region->width;
        regionHeight = region->height;
    }

    unsigned char* buffer = (unsigned char*)malloc(regionWidth);
    if (!buffer) {
        free(histogram);
        return NULL;
    }

    for (int y = 0; y < regionHeight; y++) {
        if (GDALRasterIO(band, GF_Read, startX, startY + y, regionWidth, 1,
                         buffer, regionWidth, 1, GDT_Byte, 0, 0) != CE_None) {
            continue;
        }
        for (int x = 0; x < regionWidth; x++) {
            histogram[buffer[x]]++;
        }
    }

    free(buffer);
    return histogram;
}

// 计算累积直方图
double* calculateCumulativeHistogram(int* histogram, int size) {
    double* cumHist = (double*)malloc(256 * sizeof(double));
    if (!cumHist) return NULL;

    long long total = 0;
    for (int i = 0; i < 256; i++) {
        total += histogram[i];
    }

    long long sum = 0;
    for (int i = 0; i < 256; i++) {
        sum += histogram[i];
        cumHist[i] = (double)sum / total;
    }

    return cumHist;
}

// ==================== 调色函数实现 ====================

// 基础调色
GDALDatasetH adjustColors(GDALDatasetH hDS, ColorAdjustParams* params) {
    if (!hDS || !params) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    // 创建输出数据集
    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
// osgeo_color.c (续)

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    // 处理需要至少3个波段的RGB操作
    int hasRGB = (bandCount >= 3);

    // 分配缓冲区
    unsigned char* rBuf = (unsigned char*)malloc(width * height);
    unsigned char* gBuf = (unsigned char*)malloc(width * height);
    unsigned char* bBuf = (unsigned char*)malloc(width * height);
    unsigned char* aBuf = NULL;

    if (!rBuf || !gBuf || !bBuf) {
        if (rBuf) free(rBuf);
        if (gBuf) free(gBuf);
        if (bBuf) free(bBuf);
        GDALClose(outDS);
        return NULL;
    }

    if (bandCount >= 4) {
        aBuf = (unsigned char*)malloc(width * height);
    }

    // 读取数据
    GDALRasterBandH band1 = GDALGetRasterBand(hDS, 1);
    GDALRasterBandH band2 = bandCount >= 2 ? GDALGetRasterBand(hDS, 2) : NULL;
    GDALRasterBandH band3 = bandCount >= 3 ? GDALGetRasterBand(hDS, 3) : NULL;
    GDALRasterBandH band4 = bandCount >= 4 ? GDALGetRasterBand(hDS, 4) : NULL;

    GDALRasterIO(band1, GF_Read, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    if (band2) GDALRasterIO(band2, GF_Read, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    if (band3) GDALRasterIO(band3, GF_Read, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);
    if (band4 && aBuf) GDALRasterIO(band4, GF_Read, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);

    // 如果只有单波段，复制到其他通道
    if (!hasRGB) {
        memcpy(gBuf, rBuf, width * height);
        memcpy(bBuf, rBuf, width * height);
    }

    // 预计算亮度和对比度参数
    double brightnessOffset = params->brightness * 255.0;
    double contrastFactor = (params->contrast >= 0) ?
                            (1.0 + params->contrast * 2.0) :
                            (1.0 + params->contrast);

    // 处理每个像素
    for (int i = 0; i < width * height; i++) {
        double r = rBuf[i];
        double g = gBuf[i];
        double b = bBuf[i];

        // 1. 亮度调整
        r += brightnessOffset;
        g += brightnessOffset;
        b += brightnessOffset;

        // 2. 对比度调整 (以128为中心)
        r = (r - 128.0) * contrastFactor + 128.0;
        g = (g - 128.0) * contrastFactor + 128.0;
        b = (b - 128.0) * contrastFactor + 128.0;

        // 3. Gamma校正
        if (params->gamma != 1.0 && params->gamma > 0) {
            double invGamma = 1.0 / params->gamma;
            r = pow(clamp(r, 0, 255) / 255.0, invGamma) * 255.0;
            g = pow(clamp(g, 0, 255) / 255.0, invGamma) * 255.0;
            b = pow(clamp(b, 0, 255) / 255.0, invGamma) * 255.0;
        }

        // 4. 饱和度和色相调整 (转换到HSL空间)
        if (params->saturation != 0 || params->hue != 0) {
            double h, s, l;
            rgbToHsl(clamp(r, 0, 255), clamp(g, 0, 255), clamp(b, 0, 255), &h, &s, &l);

            // 饱和度调整
            if (params->saturation >= 0) {
                s = s + (1.0 - s) * params->saturation;
            } else {
                s = s * (1.0 + params->saturation);
            }
            s = clamp(s, 0, 1);

            // 色相调整
            h = fmod(h + params->hue + 360.0, 360.0);

            hslToRgb(h, s, l, &r, &g, &b);
        }

        // 写回结果
        rBuf[i] = (unsigned char)clamp(r, 0, 255);
        gBuf[i] = (unsigned char)clamp(g, 0, 255);
        bBuf[i] = (unsigned char)clamp(b, 0, 255);
    }

    // 写入输出数据集
    GDALRasterBandH outBand1 = GDALGetRasterBand(outDS, 1);
    GDALRasterBandH outBand2 = bandCount >= 2 ? GDALGetRasterBand(outDS, 2) : NULL;
    GDALRasterBandH outBand3 = bandCount >= 3 ? GDALGetRasterBand(outDS, 3) : NULL;
    GDALRasterBandH outBand4 = bandCount >= 4 ? GDALGetRasterBand(outDS, 4) : NULL;

    GDALRasterIO(outBand1, GF_Write, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    if (outBand2) GDALRasterIO(outBand2, GF_Write, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    if (outBand3) GDALRasterIO(outBand3, GF_Write, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);
    if (outBand4 && aBuf) GDALRasterIO(outBand4, GF_Write, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);

    // 清理
    free(rBuf);
    free(gBuf);
    free(bBuf);
    if (aBuf) free(aBuf);

    return outDS;
}

// 单独调整亮度
GDALDatasetH adjustBrightness(GDALDatasetH hDS, double brightness) {
    ColorAdjustParams params = {
        .brightness = brightness,
        .contrast = 0,
        .saturation = 0,
        .gamma = 1.0,
        .hue = 0
    };
    return adjustColors(hDS, &params);
}

// 单独调整对比度
GDALDatasetH adjustContrast(GDALDatasetH hDS, double contrast) {
    ColorAdjustParams params = {
        .brightness = 0,
        .contrast = contrast,
        .saturation = 0,
        .gamma = 1.0,
        .hue = 0
    };
    return adjustColors(hDS, &params);
}

// 单独调整饱和度
GDALDatasetH adjustSaturation(GDALDatasetH hDS, double saturation) {
    ColorAdjustParams params = {
        .brightness = 0,
        .contrast = 0,
        .saturation = saturation,
        .gamma = 1.0,
        .hue = 0
    };
    return adjustColors(hDS, &params);
}

// Gamma校正
GDALDatasetH adjustGamma(GDALDatasetH hDS, double gamma) {
    ColorAdjustParams params = {
        .brightness = 0,
        .contrast = 0,
        .saturation = 0,
        .gamma = gamma,
        .hue = 0
    };
    return adjustColors(hDS, &params);
}

// 色相调整
GDALDatasetH adjustHue(GDALDatasetH hDS, double hue) {
    ColorAdjustParams params = {
        .brightness = 0,
        .contrast = 0,
        .saturation = 0,
        .gamma = 1.0,
        .hue = hue
    };
    return adjustColors(hDS, &params);
}

// 色阶调整
GDALDatasetH adjustLevels(GDALDatasetH hDS, LevelsParams* params, int bandIndex) {
    if (!hDS || !params) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    // 创建输出数据集
    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);

    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    // 创建查找表
    unsigned char* lut = createLUT(params);
    if (!lut) {
        GDALClose(outDS);
        return NULL;
    }

    // 分配缓冲区
    unsigned char* buffer = (unsigned char*)malloc(width * height);
    if (!buffer) {
        free(lut);
        GDALClose(outDS);
        return NULL;
// osgeo_color.c (续3)

    }

    // 处理每个波段
    for (int b = 1; b <= bandCount; b++) {
        // 如果指定了波段，只处理该波段
        if (bandIndex > 0 && b != bandIndex) {
            // 直接复制未处理的波段
            GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
            GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);
            GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
            GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
            continue;
        }

        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        // 读取数据
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);

        // 应用查找表
        for (int i = 0; i < width * height; i++) {
            buffer[i] = lut[buffer[i]];
        }

        // 写入数据
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
    }

    free(buffer);
    free(lut);

    return outDS;
}

// 曲线调整
GDALDatasetH adjustCurves(GDALDatasetH hDS, CurveParams* params) {
    if (!hDS || !params) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    // 创建曲线查找表
    unsigned char* lut = createCurveLUT(params);
    if (!lut) {
        GDALClose(outDS);
        return NULL;
    }

    unsigned char* buffer = (unsigned char*)malloc(width * height);
    if (!buffer) {
        free(lut);
        GDALClose(outDS);
        return NULL;
    }

    for (int b = 1; b <= bandCount; b++) {
        // channel: 0=全部, 1=R, 2=G, 3=B
        int shouldProcess = (params->channel == 0) || (params->channel == b);

        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);

        if (shouldProcess) {
            for (int i = 0; i < width * height; i++) {
                buffer[i] = lut[buffer[i]];
            }
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
    }

    free(buffer);
    free(lut);

    return outDS;
}

// 自动色阶
GDALDatasetH autoLevels(GDALDatasetH hDS, double clipPercent) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    unsigned char* buffer = (unsigned char*)malloc(width * height);
    if (!buffer) {
        GDALClose(outDS);
        return NULL;
    }

    long long totalPixels = (long long)width * height;
    long long clipCount = (long long)(totalPixels * clipPercent / 100.0);

    for (int b = 1; b <= bandCount; b++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);

        // 计算直方图
        int histogram[256] = {0};
        for (int i = 0; i < width * height; i++) {
            histogram[buffer[i]]++;
        }

        // 找到裁剪后的最小值
        long long count = 0;
        int minVal = 0;
        for (int i = 0; i < 256; i++) {
            count += histogram[i];
            if (count > clipCount) {
                minVal = i;
                break;
            }
        }

        // 找到裁剪后的最大值
        count = 0;
        int maxVal = 255;
        for (int i = 255; i >= 0; i--) {
            count += histogram[i];
            if (count > clipCount) {
                maxVal = i;
                break;
            }
        }

        // 避免除零
        if (maxVal <= minVal) {
            maxVal = minVal + 1;
        }

        // 创建并应用查找表
        double scale = 255.0 / (maxVal - minVal);
        for (int i = 0; i < width * height; i++) {
            int val = buffer[i];
            if (val <= minVal) {
                buffer[i] = 0;
            } else if (val >= maxVal) {
                buffer[i] = 255;
            } else {
                buffer[i] = (unsigned char)((val - minVal) * scale);
            }
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
    }

    free(buffer);
    return outDS;
}

// 自动对比度
GDALDatasetH autoContrast(GDALDatasetH hDS) {
    return autoLevels(hDS, 0.5); // 使用0.5%的裁剪
}

// 自动白平衡 (灰度世界算法)
GDALDatasetH autoWhiteBalance(GDALDatasetH hDS) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    if (bandCount < 3) {
        // 非RGB图像，直接返回副本
        return adjustBrightness(hDS, 0);
    }

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* rBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* gBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* bBuf = (unsigned char*)malloc(pixelCount);

    if (!rBuf || !gBuf || !bBuf) {
        if (rBuf) free(rBuf);
        if (gBuf) free(gBuf);
        if (bBuf) free(bBuf);
        GDALClose(outDS);
        return NULL;
    }

    // 读取RGB数据
    GDALRasterIO(GDALGetRasterBand(hDS, 1), GF_Read, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 2), GF_Read, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 3), GF_Read, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);

    // 计算各通道均值
    double sumR = 0, sumG = 0, sumB = 0;
    for (int i = 0; i < pixelCount; i++) {
        sumR += rBuf[i];
        sumG += gBuf[i];
        sumB += bBuf[i];
    }

    double avgR = sumR / pixelCount;
    double avgG = sumG / pixelCount;
    double avgB = sumB / pixelCount;

    // 计算灰度均值
    double avgGray = (avgR + avgG + avgB) / 3.0;

    // 计算缩放因子
    double scaleR = (avgR > 0) ? avgGray / avgR : 1.0;
    double scaleG = (avgG > 0) ? avgGray / avgG : 1.0;
    double scaleB = (avgB > 0) ? avgGray / avgB : 1.0;

    // 应用白平衡
    for (int i = 0; i < pixelCount; i++) {
        rBuf[i] = (unsigned char)clamp(rBuf[i] * scaleR, 0, 255);
        gBuf[i] = (unsigned char)clamp(gBuf[i] * scaleG, 0, 255);
        bBuf[i] = (unsigned char)clamp(bBuf[i] * scaleB, 0, 255);
    }

    // 写入结果
    GDALRasterIO(GDALGetRasterBand(outDS, 1), GF_Write, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(outDS, 2), GF_Write, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(outDS, 3), GF_Write, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);

    // 复制Alpha通道（如果有）
    if (bandCount >= 4) {
        unsigned char* aBuf = (unsigned char*)malloc(pixelCount);
        if (aBuf) {
            GDALRasterIO(GDALGetRasterBand(hDS, 4), GF_Read, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);
            GDALRasterIO(GDALGetRasterBand(outDS, 4), GF_Write, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);
            free(aBuf);
        }
    }

    free(rBuf);
    free(gBuf);
    free(bBuf);

    return outDS;
}

// 直方图均衡化
GDALDatasetH histogramEqualization(GDALDatasetH hDS, int bandIndex) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* buffer = (unsigned char*)malloc(pixelCount);
    if (!buffer) {
        GDALClose(outDS);
        return NULL;
    }

    for (int b = 1; b <= bandCount; b++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);

        // 如果指定了波段，只处理该波段
        if (bandIndex == 0 || bandIndex == b) {
            // 计算直方图
            int histogram[256] = {0};
            for (int i = 0; i < pixelCount; i++) {
                histogram[buffer[i]]++;
            }

            // 计算累积分布函数
            int cdf[256];
            cdf[0] = histogram[0];
            for (int i = 1; i < 256; i++) {
                cdf[i] = cdf[i - 1] + histogram[i];
            }

            // 找到最小非零CDF值
            int cdfMin = 0;
            for (int i = 0; i < 256; i++) {
                if (cdf[i] > 0) {
                    cdfMin = cdf[i];
                    break;
                }
            }

            // 创建均衡化查找表
            unsigned char lut[256];
            double scale = 255.0 / (pixelCount - cdfMin);
            for (int i = 0; i < 256; i++) {
                if (cdf[i] <= cdfMin) {
                    lut[i] = 0;
                } else {
                    lut[i] = (unsigned char)clamp((cdf[i] - cdfMin) * scale, 0, 255);
                }
            }

            // 应用查找表
            for (int i = 0; i < pixelCount; i++) {
                buffer[i] = lut[buffer[i]];
            }
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
    }

    free(buffer);
    return outDS;
}

// CLAHE (对比度受限自适应直方图均衡化)
GDALDatasetH claheEqualization(GDALDatasetH hDS, int tileSize, double clipLimit) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    if (tileSize <= 0) tileSize = 64;
    if (clipLimit <= 0) clipLimit = 2.0;

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* srcBuffer = (unsigned char*)malloc(pixelCount);
    unsigned char* dstBuffer = (unsigned char*)malloc(pixelCount);

    if (!srcBuffer || !dstBuffer) {
        if (srcBuffer) free(srcBuffer);
        if (dstBuffer) free(dstBuffer);
        GDALClose(outDS);
        return NULL;
    }

    // 计算瓦片数量
    int tilesX = (width + tileSize - 1) / tileSize;
    int tilesY = (height + tileSize - 1) / tileSize;

    // 为每个瓦片预计算查找表
    unsigned char** tileLUTs = (unsigned char**)malloc(tilesX * tilesY * sizeof(unsigned char*));
    if (!tileLUTs) {
        free(srcBuffer);
        free(dstBuffer);
        GDALClose(outDS);
        return NULL;
    }

    for (int b = 1; b <= bandCount; b++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, srcBuffer, width, height, GDT_Byte, 0, 0);

        // 为每个瓦片计算CLAHE查找表
        for (int ty = 0; ty < tilesY; ty++) {
            for (int tx = 0; tx < tilesX; tx++) {
                int tileIdx = ty * tilesX + tx;
                tileLUTs[tileIdx] = (unsigned char*)malloc(256);

                int startX = tx * tileSize;
                int startY = ty * tileSize;
                int endX = (startX + tileSize < width) ? startX + tileSize : width;
                int endY = (startY + tileSize < height) ? startY + tileSize : height;
                int tilePixels = (endX - startX) * (endY - startY);

                // 计算瓦片直方图
                int histogram[256] = {0};
                for (int y = startY; y < endY; y++) {
                    for (int x = startX; x < endX; x++) {
                        histogram[srcBuffer[y * width + x]]++;
                    }
                }

                // 裁剪直方图
                int clipThreshold = (int)(clipLimit * tilePixels / 256);
                int excess = 0;
                for (int i = 0; i < 256; i++) {
                    if (histogram[i] > clipThreshold) {
                        excess += histogram[i] - clipThreshold;
                        histogram[i] = clipThreshold;
                    }
                }

                // 重新分配超出部分
                int avgIncrease = excess / 256;
                for (int i = 0; i < 256; i++) {
                    histogram[i] += avgIncrease;
                }

                // 计算CDF并创建查找表
                int cdf[256];
                cdf[0] = histogram[0];
                for (int i = 1; i < 256; i++) {
                    cdf[i] = cdf[i - 1] + histogram[i];
                }

                int cdfMin = 0;
                for (int i = 0; i < 256; i++) {
                    if (cdf[i] > 0) {
                        cdfMin = cdf[i];
                        break;
                    }
                }

                double scale = 255.0 / (tilePixels - cdfMin + 1);
                for (int i = 0; i < 256; i++) {
                    tileLUTs[tileIdx][i] = (unsigned char)clamp((cdf[i] - cdfMin) * scale, 0, 255);
                }
            }
        }

        // 使用双线性插值应用CLAHE
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                // 计算当前像素所在的瓦片位置
                double tileXf = (double)x / tileSize - 0.5;
                double tileYf = (double)y / tileSize - 0.5;

                int tx0 = (int)floor(tileXf);
                int ty0 = (int)floor(tileYf);
                int tx1 = tx0 + 1;
                int ty1 = ty0 + 1;

                // 边界处理
                tx0 = clampInt(tx0, 0, tilesX - 1);
                ty0 = clampInt(ty0, 0, tilesY - 1);
                tx1 = clampInt(tx1, 0, tilesX - 1);
                ty1 = clampInt(ty1, 0, tilesY - 1);

                // 计算插值权重
                double wx = tileXf - floor(tileXf);
                double wy = tileYf - floor(tileYf);

                if (tileXf < 0) wx = 0;
                if (tileYf < 0) wy = 0;

                unsigned char val = srcBuffer[y * width + x];

                // 双线性插值
                double v00 = tileLUTs[ty0 * tilesX + tx0][val];
                double v01 = tileLUTs[ty0 * tilesX + tx1][val];
                double v10 = tileLUTs[ty1 * tilesX + tx0][val];
                double v11 = tileLUTs[ty1 * tilesX + tx1][val];

                double v0 = v00 * (1 - wx) + v01 * wx;
                double v1 = v10 * (1 - wx) + v11 * wx;
                double result = v0 * (1 - wy) + v1 * wy;

                dstBuffer[y * width + x] = (unsigned char)clamp(result, 0, 255);
            }
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, dstBuffer, width, height, GDT_Byte, 0, 0);

        // 释放瓦片查找表
        for (int i = 0; i < tilesX * tilesY; i++) {
            free(tileLUTs[i]);
        }
    }

    free(tileLUTs);
    free(srcBuffer);
    free(dstBuffer);

    return outDS;
}

// ==================== 匀色函数实现 ====================

// 获取颜色统计信息
ColorStatistics* getColorStatistics(GDALDatasetH hDS, ReferenceRegion* region) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    if (bandCount < 3) return NULL;

    ColorStatistics* stats = (ColorStatistics*)calloc(1, sizeof(ColorStatistics));
    if (!stats) return NULL;

    int startX = 0, startY = 0, regionWidth = width, regionHeight = height;
    if (region) {
        startX = region->x;
        startY = region->y;
        regionWidth = region->width;
        regionHeight = region->height;
    }

    int pixelCount = regionWidth * regionHeight;
    unsigned char* rBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* gBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* bBuf = (unsigned char*)malloc(pixelCount);

    if (!rBuf || !gBuf || !bBuf) {
        if (rBuf) free(rBuf);
        if (gBuf) free(gBuf);
        if (bBuf) free(bBuf);
        free(stats);
        return NULL;
    }

    // 读取数据
    GDALRasterIO(GDALGetRasterBand(hDS, 1), GF_Read, startX, startY, regionWidth, regionHeight,
                 rBuf, regionWidth, regionHeight, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 2), GF_Read, startX, startY, regionWidth, regionHeight,
                 gBuf, regionWidth, regionHeight, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(hDS, 3), GF_Read, startX, startY, regionWidth, regionHeight,
                 bBuf, regionWidth, regionHeight, GDT_Byte, 0, 0);

    // 计算统计信息
    double sumR = 0, sumG = 0, sumB = 0;
    double sumR2 = 0, sumG2 = 0, sumB2 = 0;
    stats->minR = stats->minG = stats->minB = 255;
    stats->maxR = stats->maxG = stats->maxB = 0;

    for (int i = 0; i < pixelCount; i++) {
        double r = rBuf[i], g = gBuf[i], b = bBuf[i];

        sumR += r; sumG += g; sumB += b;
        sumR2 += r * r; sumG2 += g * g; sumB2 += b * b;

        if (r < stats->minR) stats->minR = r;
        if (g < stats->minG) stats->minG = g;
        if (b < stats->minB) stats->minB = b;
        if (r > stats->maxR) stats->maxR = r;
        if (g > stats->maxG) stats->maxG = g;
        if (b > stats->maxB) stats->maxB = b;
    }

    stats->meanR = sumR / pixelCount;
    stats->meanG = sumG / pixelCount;
    stats->meanB = sumB / pixelCount;

    stats->stdR = sqrt(sumR2 / pixelCount - stats->meanR * stats->meanR);
    stats->stdG = sqrt(sumG2 / pixelCount - stats->meanG * stats->meanG);
    stats->stdB = sqrt(sumB2 / pixelCount - stats->meanB * stats->meanB);

    free(rBuf);
    free(gBuf);
    free(bBuf);

    return stats;
}

// 获取波段统计信息
BandStatistics* getBandStatistics(GDALDatasetH hDS, int bandIndex, ReferenceRegion* region) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);

    BandStatistics* stats = (BandStatistics*)calloc(1, sizeof(BandStatistics));
    if (!stats) return NULL;

    stats->histogram = (int*)calloc(256, sizeof(int));
    stats->histogramSize = 256;

    if (!stats->histogram) {
        free(stats);
        return NULL;
    }

    int startX = 0, startY = 0, regionWidth = width, regionHeight = height;
    if (region) {
        startX = region->x;
        startY = region->y;
        regionWidth = region->width;
// osgeo_color.c (续4)

        regionHeight = region->height;
    }

    int pixelCount = regionWidth * regionHeight;
    unsigned char* buffer = (unsigned char*)malloc(pixelCount);

    if (!buffer) {
        free(stats->histogram);
        free(stats);
        return NULL;
    }

    GDALRasterBandH band = GDALGetRasterBand(hDS, bandIndex);
    GDALRasterIO(band, GF_Read, startX, startY, regionWidth, regionHeight,
                 buffer, regionWidth, regionHeight, GDT_Byte, 0, 0);

    // 计算统计信息
    double sum = 0, sum2 = 0;
    stats->min = 255;
    stats->max = 0;

    for (int i = 0; i < pixelCount; i++) {
        double val = buffer[i];
        sum += val;
        sum2 += val * val;
        stats->histogram[buffer[i]]++;

        if (val < stats->min) stats->min = val;
        if (val > stats->max) stats->max = val;
    }

    stats->mean = sum / pixelCount;
    stats->stddev = sqrt(sum2 / pixelCount - stats->mean * stats->mean);

    free(buffer);
    return stats;
}

// 释放统计信息
void freeColorStatistics(ColorStatistics* stats) {
    if (stats) free(stats);
}

void freeBandStatistics(BandStatistics* stats) {
    if (stats) {
        if (stats->histogram) free(stats->histogram);
        free(stats);
    }
}

// 直方图匹配
GDALDatasetH histogramMatch(GDALDatasetH srcDS, GDALDatasetH refDS,
                            ReferenceRegion* srcRegion, ReferenceRegion* refRegion) {
    if (!srcDS || !refDS) return NULL;

    int width = GDALGetRasterXSize(srcDS);
    int height = GDALGetRasterYSize(srcDS);
    int bandCount = GDALGetRasterCount(srcDS);
    int refBandCount = GDALGetRasterCount(refDS);

    if (bandCount != refBandCount) return NULL;

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(srcDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(srcDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* srcBuffer = (unsigned char*)malloc(pixelCount);

    if (!srcBuffer) {
        GDALClose(outDS);
        return NULL;
    }

    for (int b = 1; b <= bandCount; b++) {
        // 计算源图像直方图和CDF
        int* srcHist = calculateHistogram(srcDS, b, srcRegion);
        double* srcCDF = calculateCumulativeHistogram(srcHist, 256);

        // 计算参考图像直方图和CDF
        int* refHist = calculateHistogram(refDS, b, refRegion);
        double* refCDF = calculateCumulativeHistogram(refHist, 256);

        if (!srcHist || !srcCDF || !refHist || !refCDF) {
            if (srcHist) free(srcHist);
            if (srcCDF) free(srcCDF);
            if (refHist) free(refHist);
            if (refCDF) free(refCDF);
            continue;
        }

        // 创建映射查找表
        unsigned char lut[256];
        for (int i = 0; i < 256; i++) {
            double srcVal = srcCDF[i];
            int bestMatch = 0;
            double minDiff = fabs(srcVal - refCDF[0]);

            for (int j = 1; j < 256; j++) {
                double diff = fabs(srcVal - refCDF[j]);
                if (diff < minDiff) {
                    minDiff = diff;
                    bestMatch = j;
                }
            }
            lut[i] = (unsigned char)bestMatch;
        }

        // 读取源数据
        GDALRasterBandH srcBand = GDALGetRasterBand(srcDS, b);
        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, srcBuffer, width, height, GDT_Byte, 0, 0);

        // 应用查找表
        for (int i = 0; i < pixelCount; i++) {
            srcBuffer[i] = lut[srcBuffer[i]];
        }

        // 写入结果
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);
        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, srcBuffer, width, height, GDT_Byte, 0, 0);

        free(srcHist);
        free(srcCDF);
        free(refHist);
        free(refCDF);
    }

    free(srcBuffer);
    return outDS;
}

// 均值-标准差匹配
GDALDatasetH meanStdMatch(GDALDatasetH srcDS, ColorStatistics* targetStats,
                          ReferenceRegion* region, double strength) {
    if (!srcDS || !targetStats) return NULL;

    int width = GDALGetRasterXSize(srcDS);
    int height = GDALGetRasterYSize(srcDS);
    int bandCount = GDALGetRasterCount(srcDS);

    if (bandCount < 3) return NULL;

    strength = clamp(strength, 0, 1);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(srcDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(srcDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    // 获取源图像统计信息
    ColorStatistics* srcStats = getColorStatistics(srcDS, region);
    if (!srcStats) {
        GDALClose(outDS);
        return NULL;
    }

    int pixelCount = width * height;
    unsigned char* rBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* gBuf = (unsigned char*)malloc(pixelCount);
    unsigned char* bBuf = (unsigned char*)malloc(pixelCount);

    if (!rBuf || !gBuf || !bBuf) {
        if (rBuf) free(rBuf);
        if (gBuf) free(gBuf);
        if (bBuf) free(bBuf);
        freeColorStatistics(srcStats);
        GDALClose(outDS);
        return NULL;
    }

    // 读取数据
    GDALRasterIO(GDALGetRasterBand(srcDS, 1), GF_Read, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(srcDS, 2), GF_Read, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(srcDS, 3), GF_Read, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);

    // 计算变换参数
    double scaleR = (srcStats->stdR > 0) ? targetStats->stdR / srcStats->stdR : 1.0;
    double scaleG = (srcStats->stdG > 0) ? targetStats->stdG / srcStats->stdG : 1.0;
    double scaleB = (srcStats->stdB > 0) ? targetStats->stdB / srcStats->stdB : 1.0;

    // 应用强度混合
    scaleR = 1.0 + (scaleR - 1.0) * strength;
    scaleG = 1.0 + (scaleG - 1.0) * strength;
    scaleB = 1.0 + (scaleB - 1.0) * strength;

    double offsetR = targetStats->meanR - srcStats->meanR * scaleR;
    double offsetG = targetStats->meanG - srcStats->meanG * scaleG;
    double offsetB = targetStats->meanB - srcStats->meanB * scaleB;

    offsetR *= strength;
    offsetG *= strength;
    offsetB *= strength;

    // 应用变换
    for (int i = 0; i < pixelCount; i++) {
        double r = rBuf[i] * scaleR + offsetR;
        double g = gBuf[i] * scaleG + offsetG;
        double b = bBuf[i] * scaleB + offsetB;

        rBuf[i] = (unsigned char)clamp(r, 0, 255);
        gBuf[i] = (unsigned char)clamp(g, 0, 255);
        bBuf[i] = (unsigned char)clamp(b, 0, 255);
    }

    // 写入结果
    GDALRasterIO(GDALGetRasterBand(outDS, 1), GF_Write, 0, 0, width, height, rBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(outDS, 2), GF_Write, 0, 0, width, height, gBuf, width, height, GDT_Byte, 0, 0);
    GDALRasterIO(GDALGetRasterBand(outDS, 3), GF_Write, 0, 0, width, height, bBuf, width, height, GDT_Byte, 0, 0);

    // 复制Alpha通道
    if (bandCount >= 4) {
        unsigned char* aBuf = (unsigned char*)malloc(pixelCount);
        if (aBuf) {
            GDALRasterIO(GDALGetRasterBand(srcDS, 4), GF_Read, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);
            GDALRasterIO(GDALGetRasterBand(outDS, 4), GF_Write, 0, 0, width, height, aBuf, width, height, GDT_Byte, 0, 0);
            free(aBuf);
        }
    }

    free(rBuf);
    free(gBuf);
    free(bBuf);
    freeColorStatistics(srcStats);

    return outDS;
}

// Wallis滤波匀色
GDALDatasetH wallisFilter(GDALDatasetH hDS, double targetMean, double targetStd,
                          double c, double b, int windowSize) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    if (windowSize <= 0) windowSize = 31;
    if (windowSize % 2 == 0) windowSize++;

    c = clamp(c, 0, 1);
    b = clamp(b, 0, 1);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* srcBuffer = (unsigned char*)malloc(pixelCount);
    unsigned char* dstBuffer = (unsigned char*)malloc(pixelCount);
    double* meanBuffer = (double*)malloc(pixelCount * sizeof(double));
    double* stdBuffer = (double*)malloc(pixelCount * sizeof(double));

    if (!srcBuffer || !dstBuffer || !meanBuffer || !stdBuffer) {
        if (srcBuffer) free(srcBuffer);
        if (dstBuffer) free(dstBuffer);
        if (meanBuffer) free(meanBuffer);
        if (stdBuffer) free(stdBuffer);
        GDALClose(outDS);
        return NULL;
    }

    int halfWindow = windowSize / 2;

    for (int band = 1; band <= bandCount; band++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, band);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, band);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, srcBuffer, width, height, GDT_Byte, 0, 0);

        // 计算局部均值和标准差（使用积分图加速）
        double* integralSum = (double*)calloc((width + 1) * (height + 1), sizeof(double));
        double* integralSum2 = (double*)calloc((width + 1) * (height + 1), sizeof(double));

        if (!integralSum || !integralSum2) {
            if (integralSum) free(integralSum);
            if (integralSum2) free(integralSum2);
            continue;
        }

        // 构建积分图
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                double val = srcBuffer[y * width + x];
                int idx = (y + 1) * (width + 1) + (x + 1);
                int idxLeft = (y + 1) * (width + 1) + x;
                int idxUp = y * (width + 1) + (x + 1);
                int idxDiag = y * (width + 1) + x;

                integralSum[idx] = val + integralSum[idxLeft] + integralSum[idxUp] - integralSum[idxDiag];
                integralSum2[idx] = val * val + integralSum2[idxLeft] + integralSum2[idxUp] - integralSum2[idxDiag];
            }
        }

        // 计算局部统计量
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int x0 = (x - halfWindow < 0) ? 0 : x - halfWindow;
                int y0 = (y - halfWindow < 0) ? 0 : y - halfWindow;
                int x1 = (x + halfWindow >= width) ? width - 1 : x + halfWindow;
                int y1 = (y + halfWindow >= height) ? height - 1 : y + halfWindow;

                int count = (x1 - x0 + 1) * (y1 - y0 + 1);

                // 使用积分图计算区域和
                int idx00 = y0 * (width + 1) + x0;
                int idx01 = y0 * (width + 1) + (x1 + 1);
                int idx10 = (y1 + 1) * (width + 1) + x0;
                int idx11 = (y1 + 1) * (width + 1) + (x1 + 1);

                double sum = integralSum[idx11] - integralSum[idx01] - integralSum[idx10] + integralSum[idx00];
                double sum2 = integralSum2[idx11] - integralSum2[idx01] - integralSum2[idx10] + integralSum2[idx00];

                double localMean = sum / count;
                double localVar = sum2 / count - localMean * localMean;
                double localStd = (localVar > 0) ? sqrt(localVar) : 1.0;

                meanBuffer[y * width + x] = localMean;
                stdBuffer[y * width + x] = localStd;
            }
        }

        free(integralSum);
        free(integralSum2);

        // 应用Wallis滤波
        // 公式: g = (f - m_f) * (c * s_g / s_f) + b * m_g + (1 - b) * m_f
        // 其中 f=输入, g=输出, m_f=局部均值, s_f=局部标准差, m_g=目标均值, s_g=目标标准差
        for (int i = 0; i < pixelCount; i++) {
            double f = srcBuffer[i];
            double mf = meanBuffer[i];
            double sf = stdBuffer[i];

            // 避免除零
            if (sf < 1.0) sf = 1.0;

            double r1 = c * targetStd / sf;
            double r0 = b * targetMean + (1.0 - b) * mf;

            double g = (f - mf) * r1 + r0;

            dstBuffer[i] = (unsigned char)clamp(g, 0, 255);
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, dstBuffer, width, height, GDT_Byte, 0, 0);
    }

    free(srcBuffer);
    free(dstBuffer);
    free(meanBuffer);
    free(stdBuffer);

    return outDS;
}

// 矩匹配
GDALDatasetH momentMatch(GDALDatasetH srcDS, GDALDatasetH refDS,
                         ReferenceRegion* srcRegion, ReferenceRegion* refRegion) {
    if (!srcDS || !refDS) return NULL;

    // 获取参考图像的统计信息
    ColorStatistics* refStats = getColorStatistics(refDS, refRegion);
    if (!refStats) return NULL;

    // 使用均值-标准差匹配
    GDALDatasetH result = meanStdMatch(srcDS, refStats, srcRegion, 1.0);

    freeColorStatistics(refStats);
    return result;
}

// 线性回归匀色
GDALDatasetH linearRegressionBalance(GDALDatasetH srcDS, GDALDatasetH refDS,
                                      ReferenceRegion* overlapRegion) {
    if (!srcDS || !refDS || !overlapRegion) return NULL;

    int width = GDALGetRasterXSize(srcDS);
    int height = GDALGetRasterYSize(srcDS);
    int bandCount = GDALGetRasterCount(srcDS);
    int refBandCount = GDALGetRasterCount(refDS);

    if (bandCount != refBandCount || bandCount < 3) return NULL;

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(srcDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(srcDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int overlapPixels = overlapRegion->width * overlapRegion->height;
    int pixelCount = width * height;

    // 为每个波段计算线性回归参数
    for (int b = 1; b <= bandCount; b++) {
        unsigned char* srcOverlap = (unsigned char*)malloc(overlapPixels);
        unsigned char* refOverlap = (unsigned char*)malloc(overlapPixels);
        unsigned char* srcFull = (unsigned char*)malloc(pixelCount);

        if (!srcOverlap || !refOverlap || !srcFull) {
            if (srcOverlap) free(srcOverlap);
            if (refOverlap) free(refOverlap);
            if (srcFull) free(srcFull);
            continue;
        }

        // 读取重叠区域数据
        GDALRasterIO(GDALGetRasterBand(srcDS, b), GF_Read,
                     overlapRegion->x, overlapRegion->y,
                     overlapRegion->width, overlapRegion->height,
                     srcOverlap, overlapRegion->width, overlapRegion->height, GDT_Byte, 0, 0);

        GDALRasterIO(GDALGetRasterBand(refDS, b), GF_Read,
                     overlapRegion->x, overlapRegion->y,
                     overlapRegion->width, overlapRegion->height,
                     refOverlap, overlapRegion->width, overlapRegion->height, GDT_Byte, 0, 0);

        // 计算线性回归参数 y = ax + b
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        int validCount = 0;

        for (int i = 0; i < overlapPixels; i++) {
            // 跳过黑色像素（可能是无效区域）
            if (srcOverlap[i] == 0 || refOverlap[i] == 0) continue;

            double x = srcOverlap[i];
            double y = refOverlap[i];

            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
            validCount++;
        }

        double a = 1.0, bCoef = 0.0;
        if (validCount > 10) {
            double denom = validCount * sumX2 - sumX * sumX;
            if (fabs(denom) > 1e-10) {
                a = (validCount * sumXY - sumX * sumY) / denom;
                bCoef = (sumY - a * sumX) / validCount;
            }
        }

        // 限制参数范围，避免过度调整
        a = clamp(a, 0.5, 2.0);
        bCoef = clamp(bCoef, -50, 50);

        // 读取完整源数据并应用变换
        GDALRasterIO(GDALGetRasterBand(srcDS, b), GF_Read, 0, 0, width, height,
                     srcFull, width, height, GDT_Byte, 0, 0);

        for (int i = 0; i < pixelCount; i++) {
            double val = srcFull[i] * a + bCoef;
            srcFull[i] = (unsigned char)clamp(val, 0, 255);
        }

        GDALRasterIO(GDALGetRasterBand(outDS, b), GF_Write, 0, 0, width, height,
                     srcFull, width, height, GDT_Byte, 0, 0);

        free(srcOverlap);
        free(refOverlap);
        free(srcFull);
    }

    return outDS;
}

// Dodging匀光
GDALDatasetH dodgingBalance(GDALDatasetH hDS, int blockSize, double strength) {
    if (!hDS) return NULL;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    if (blockSize <= 0) blockSize = 128;
    strength = clamp(strength, 0, 1);

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", width, height, bandCount, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(hDS, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(hDS);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    int pixelCount = width * height;
    unsigned char* srcBuffer = (unsigned char*)malloc(pixelCount);
    unsigned char* dstBuffer = (unsigned char*)malloc(pixelCount);

    if (!srcBuffer || !dstBuffer) {
        if (srcBuffer) free(srcBuffer);
        if (dstBuffer) free(dstBuffer);
        GDALClose(outDS);
        return NULL;
    }

    // 计算块数量
    int blocksX = (width + blockSize - 1) / blockSize;
    int blocksY = (height + blockSize - 1) / blockSize;

    // 存储每个块的均值
    double* blockMeans = (double*)malloc(blocksX * blocksY * sizeof(double));
    if (!blockMeans) {
        free(srcBuffer);
        free(dstBuffer);
        GDALClose(outDS);
        return NULL;
    }

    for (int b = 1; b <= bandCount; b++) {
        GDALRasterBandH srcBand = GDALGetRasterBand(hDS, b);
        GDALRasterBandH dstBand = GDALGetRasterBand(outDS, b);

        GDALRasterIO(srcBand, GF_Read, 0, 0, width, height, srcBuffer, width, height, GDT_Byte, 0, 0);

        // 计算全局均值
        double globalSum = 0;
        for (int i = 0; i < pixelCount; i++) {
            globalSum += srcBuffer[i];
        }
        double globalMean = globalSum / pixelCount;

        // 计算每个块的均值
        for (int by = 0; by < blocksY; by++) {
            for (int bx = 0; bx < blocksX; bx++) {
                int startX = bx * blockSize;
                int startY = by * blockSize;
                int endX = (startX + blockSize < width) ? startX + blockSize : width;
                int endY = (startY + blockSize < height) ? startY + blockSize : height;

                double sum = 0;
                int count = 0;

                for (int y = startY; y < endY; y++) {
                    for (int x = startX; x < endX; x++) {
                        sum += srcBuffer[y * width + x];
                        count++;
                    }
                }

                blockMeans[by * blocksX + bx] = (count > 0) ? sum / count : globalMean;
            }
        }

        // 使用双线性插值计算每个像素的局部均值并进行匀光
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                // 计算块坐标
                double blockXf = (double)x / blockSize - 0.5;
                double blockYf = (double)y / blockSize - 0.5;

                int bx0 = (int)floor(blockXf);
                int by0 = (int)floor(blockYf);
                int bx1 = bx0 + 1;
                int by1 = by0 + 1;

                // 边界处理
                bx0 = clampInt(bx0, 0, blocksX - 1);
                by0 = clampInt(by0, 0, blocksY - 1);
                bx1 = clampInt(bx1, 0, blocksX - 1);
                by1 = clampInt(by1, 0, blocksY - 1);

                // 计算插值权重
                double wx = blockXf - floor(blockXf);
                double wy = blockYf - floor(blockYf);

                if (blockXf < 0) wx = 0;
                if (blockYf < 0) wy = 0;

                // 双线性插值计算局部均值
                double m00 = blockMeans[by0 * blocksX + bx0];
                double m01 = blockMeans[by0 * blocksX + bx1];
                double m10 = blockMeans[by1 * blocksX + bx0];
// osgeo_color.c (续5)

                double m11 = blockMeans[by1 * blocksX + bx1];

                double m0 = m00 * (1 - wx) + m01 * wx;
                double m1 = m10 * (1 - wx) + m11 * wx;
                double localMean = m0 * (1 - wy) + m1 * wy;

                // 计算匀光校正
                double correction = globalMean - localMean;
                double val = srcBuffer[y * width + x] + correction * strength;

                dstBuffer[y * width + x] = (unsigned char)clamp(val, 0, 255);
            }
        }

        GDALRasterIO(dstBand, GF_Write, 0, 0, width, height, dstBuffer, width, height, GDT_Byte, 0, 0);
    }

    free(srcBuffer);
    free(dstBuffer);
    free(blockMeans);

    return outDS;
}

// 多影像批量匀色
GDALDatasetH* batchColorBalance(GDALDatasetH* datasets, int count,
                                 GDALDatasetH refDS, ColorBalanceParams* params) {
    if (!datasets || count <= 0 || !refDS || !params) return NULL;

    GDALDatasetH* results = (GDALDatasetH*)calloc(count, sizeof(GDALDatasetH));
    if (!results) return NULL;

    // 获取参考图像统计信息
    ColorStatistics* refStats = getColorStatistics(refDS, NULL);
    if (!refStats) {
        free(results);
        return NULL;
    }

    for (int i = 0; i < count; i++) {
        if (!datasets[i]) {
            results[i] = NULL;
            continue;
        }

        switch (params->method) {
            case BALANCE_HISTOGRAM_MATCH:
                results[i] = histogramMatch(datasets[i], refDS,
                                           params->overlapRegion, params->overlapRegion);
                break;

            case BALANCE_MEAN_STD:
                results[i] = meanStdMatch(datasets[i], refStats,
                                         params->overlapRegion, params->strength);
                break;

            case BALANCE_WALLIS:
                results[i] = wallisFilter(datasets[i], params->targetMean, params->targetStd,
                                         params->wallisC, params->wallisB, 31);
                break;

            case BALANCE_MOMENT_MATCH:
                results[i] = momentMatch(datasets[i], refDS,
                                        params->overlapRegion, params->overlapRegion);
                break;

            case BALANCE_LINEAR_REGRESSION:
                if (params->overlapRegion) {
                    results[i] = linearRegressionBalance(datasets[i], refDS, params->overlapRegion);
                } else {
                    results[i] = meanStdMatch(datasets[i], refStats, NULL, params->strength);
                }
                break;

            case BALANCE_DODGING:
                results[i] = dodgingBalance(datasets[i], 128, params->strength);
                break;

            default:
                results[i] = meanStdMatch(datasets[i], refStats, NULL, params->strength);
                break;
        }
    }

    freeColorStatistics(refStats);
    return results;
}

// 渐变融合
GDALDatasetH gradientBlend(GDALDatasetH ds1, GDALDatasetH ds2,
                           ReferenceRegion* overlapRegion, int blendWidth) {
    if (!ds1 || !ds2 || !overlapRegion) return NULL;

    int width1 = GDALGetRasterXSize(ds1);
    int height1 = GDALGetRasterYSize(ds1);
    int bandCount1 = GDALGetRasterCount(ds1);

    int width2 = GDALGetRasterXSize(ds2);
    int height2 = GDALGetRasterYSize(ds2);
    int bandCount2 = GDALGetRasterCount(ds2);

    if (bandCount1 != bandCount2) return NULL;

    // 计算输出尺寸（假设ds2在ds1右侧）
    int outWidth = width1 + width2 - overlapRegion->width;
    int outHeight = (height1 > height2) ? height1 : height2;

    if (blendWidth <= 0) blendWidth = overlapRegion->width;
    if (blendWidth > overlapRegion->width) blendWidth = overlapRegion->width;

    GDALDriverH memDriver = GDALGetDriverByName("MEM");
    GDALDatasetH outDS = GDALCreate(memDriver, "", outWidth, outHeight, bandCount1, GDT_Byte, NULL);
    if (!outDS) return NULL;

    // 复制地理信息
    double geoTransform[6];
    if (GDALGetGeoTransform(ds1, geoTransform) == CE_None) {
        GDALSetGeoTransform(outDS, geoTransform);
    }
    const char* proj = GDALGetProjectionRef(ds1);
    if (proj && strlen(proj) > 0) {
        GDALSetProjection(outDS, proj);
    }

    unsigned char* buf1 = (unsigned char*)malloc(width1 * height1);
    unsigned char* buf2 = (unsigned char*)malloc(width2 * height2);
    unsigned char* outBuf = (unsigned char*)calloc(outWidth * outHeight, 1);

    if (!buf1 || !buf2 || !outBuf) {
        if (buf1) free(buf1);
        if (buf2) free(buf2);
        if (outBuf) free(outBuf);
        GDALClose(outDS);
        return NULL;
    }

    // 计算融合起始位置
    int blendStartX = overlapRegion->x;
    int blendEndX = blendStartX + blendWidth;
    int ds2OffsetX = width1 - overlapRegion->width;

    for (int b = 1; b <= bandCount1; b++) {
        // 读取两个数据集的数据
        GDALRasterIO(GDALGetRasterBand(ds1, b), GF_Read, 0, 0, width1, height1,
                     buf1, width1, height1, GDT_Byte, 0, 0);
        GDALRasterIO(GDALGetRasterBand(ds2, b), GF_Read, 0, 0, width2, height2,
                     buf2, width2, height2, GDT_Byte, 0, 0);

        // 清空输出缓冲区
        memset(outBuf, 0, outWidth * outHeight);

        for (int y = 0; y < outHeight; y++) {
            for (int x = 0; x < outWidth; x++) {
                double val = 0;

                if (x < blendStartX) {
                    // 完全来自ds1
                    if (y < height1 && x < width1) {
                        val = buf1[y * width1 + x];
                    }
                } else if (x >= blendEndX) {
                    // 完全来自ds2
                    int x2 = x - ds2OffsetX;
                    if (y < height2 && x2 >= 0 && x2 < width2) {
                        val = buf2[y * width2 + x2];
                    }
                } else {
                    // 融合区域
                    double alpha = (double)(x - blendStartX) / blendWidth;

                    // 使用平滑的S曲线
                    alpha = alpha * alpha * (3 - 2 * alpha);

                    double val1 = 0, val2 = 0;

                    if (y < height1 && x < width1) {
                        val1 = buf1[y * width1 + x];
                    }

                    int x2 = x - ds2OffsetX;
                    if (y < height2 && x2 >= 0 && x2 < width2) {
                        val2 = buf2[y * width2 + x2];
                    }

                    val = val1 * (1 - alpha) + val2 * alpha;
                }

                outBuf[y * outWidth + x] = (unsigned char)clamp(val, 0, 255);
            }
        }

        GDALRasterIO(GDALGetRasterBand(outDS, b), GF_Write, 0, 0, outWidth, outHeight,
                     outBuf, outWidth, outHeight, GDT_Byte, 0, 0);
    }

    free(buf1);
    free(buf2);
    free(outBuf);

    return outDS;
}

// 应用查找表到数据集
int applyLUT(GDALDatasetH hDS, unsigned char* lut, int bandIndex) {
    if (!hDS || !lut) return 0;

    int width = GDALGetRasterXSize(hDS);
    int height = GDALGetRasterYSize(hDS);
    int bandCount = GDALGetRasterCount(hDS);

    int pixelCount = width * height;
    unsigned char* buffer = (unsigned char*)malloc(pixelCount);
    if (!buffer) return 0;

    int startBand = (bandIndex > 0) ? bandIndex : 1;
    int endBand = (bandIndex > 0) ? bandIndex : bandCount;

    for (int b = startBand; b <= endBand; b++) {
        GDALRasterBandH band = GDALGetRasterBand(hDS, b);

        GDALRasterIO(band, GF_Read, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);

        for (int i = 0; i < pixelCount; i++) {
            buffer[i] = lut[buffer[i]];
        }

        GDALRasterIO(band, GF_Write, 0, 0, width, height, buffer, width, height, GDT_Byte, 0, 0);
    }

    free(buffer);
    return 1;
}



