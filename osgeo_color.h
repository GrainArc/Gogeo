// osgeo_color.h
#ifndef OSGEO_COLOR_H
#define OSGEO_COLOR_H

#include <gdal.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 调色参数结构 ====================

// 基础调色参数
typedef struct {
    double brightness;      // 亮度调整 [-1.0, 1.0]
    double contrast;        // 对比度调整 [-1.0, 1.0]
    double saturation;      // 饱和度调整 [-1.0, 1.0]
    double gamma;           // Gamma校正 [0.1, 10.0]
    double hue;             // 色相调整 [-180, 180]
} ColorAdjustParams;

// 色阶调整参数
typedef struct {
    double inputMin;        // 输入最小值
    double inputMax;        // 输入最大值
    double outputMin;       // 输出最小值
    double outputMax;       // 输出最大值
    double midtone;         // 中间调 [0.1, 9.9], 1.0为不变
} LevelsParams;

// 曲线调整点
typedef struct {
    double input;           // 输入值 [0, 255]
    double output;          // 输出值 [0, 255]
} CurvePoint;

// 曲线调整参数
typedef struct {
    CurvePoint* points;     // 控制点数组
    int pointCount;         // 控制点数量
    int channel;            // 通道: 0=全部, 1=R, 2=G, 3=B
} CurveParams;

// 直方图统计
typedef struct {
    double min;
    double max;
    double mean;
    double stddev;
    int* histogram;         // 256个bin
    int histogramSize;
} BandStatistics;

// ==================== 匀色参数结构 ====================

// 参考区域
typedef struct {
    int x, y;               // 左上角坐标
    int width, height;      // 区域尺寸
} ReferenceRegion;

// 匀色统计信息
typedef struct {
    double meanR, meanG, meanB;
    double stdR, stdG, stdB;
    double minR, minG, minB;
    double maxR, maxG, maxB;
} ColorStatistics;

// 匀色方法枚举
typedef enum {
    BALANCE_HISTOGRAM_MATCH = 0,    // 直方图匹配
    BALANCE_MEAN_STD,               // 均值-标准差匹配
    BALANCE_WALLIS,                 // Wallis滤波
    BALANCE_MOMENT_MATCH,           // 矩匹配
    BALANCE_LINEAR_REGRESSION,      // 线性回归
    BALANCE_DODGING                 // Dodging匀光
} ColorBalanceMethod;

// 匀色参数
typedef struct {
    ColorBalanceMethod method;
    double strength;                // 匀色强度 [0, 1]
    int useOverlapRegion;           // 是否使用重叠区域
    ReferenceRegion* overlapRegion; // 重叠区域
    double wallisC;                 // Wallis对比度参数 [0, 1]
    double wallisB;                 // Wallis亮度参数 [0, 1]
    double targetMean;              // 目标均值
    double targetStd;               // 目标标准差
} ColorBalanceParams;

// ==================== 调色函数 ====================

// 基础调色
GDALDatasetH adjustColors(GDALDatasetH hDS, ColorAdjustParams* params);

// 单独调整亮度
GDALDatasetH adjustBrightness(GDALDatasetH hDS, double brightness);

// 单独调整对比度
GDALDatasetH adjustContrast(GDALDatasetH hDS, double contrast);

// 单独调整饱和度
GDALDatasetH adjustSaturation(GDALDatasetH hDS, double saturation);

// Gamma校正
GDALDatasetH adjustGamma(GDALDatasetH hDS, double gamma);

// 色相调整
GDALDatasetH adjustHue(GDALDatasetH hDS, double hue);

// 色阶调整
GDALDatasetH adjustLevels(GDALDatasetH hDS, LevelsParams* params, int bandIndex);

// 曲线调整
GDALDatasetH adjustCurves(GDALDatasetH hDS, CurveParams* params);

// 自动色阶
GDALDatasetH autoLevels(GDALDatasetH hDS, double clipPercent);

// 自动对比度
GDALDatasetH autoContrast(GDALDatasetH hDS);

// 自动白平衡
GDALDatasetH autoWhiteBalance(GDALDatasetH hDS);

// 直方图均衡化
GDALDatasetH histogramEqualization(GDALDatasetH hDS, int bandIndex);

// CLAHE (对比度受限自适应直方图均衡化)
GDALDatasetH claheEqualization(GDALDatasetH hDS, int tileSize, double clipLimit);

// ==================== 匀色函数 ====================

// 获取颜色统计信息
ColorStatistics* getColorStatistics(GDALDatasetH hDS, ReferenceRegion* region);

// 获取波段统计信息
BandStatistics* getBandStatistics(GDALDatasetH hDS, int bandIndex, ReferenceRegion* region);

// 直方图匹配
GDALDatasetH histogramMatch(GDALDatasetH srcDS, GDALDatasetH refDS,
                            ReferenceRegion* srcRegion, ReferenceRegion* refRegion);

// 均值-标准差匹配
GDALDatasetH meanStdMatch(GDALDatasetH srcDS, ColorStatistics* targetStats,
                          ReferenceRegion* region, double strength);

// Wallis滤波匀色
GDALDatasetH wallisFilter(GDALDatasetH hDS, double targetMean, double targetStd,
                          double c, double b, int windowSize);

// 矩匹配
GDALDatasetH momentMatch(GDALDatasetH srcDS, GDALDatasetH refDS,
                         ReferenceRegion* srcRegion, ReferenceRegion* refRegion);

// 线性回归匀色
GDALDatasetH linearRegressionBalance(GDALDatasetH srcDS, GDALDatasetH refDS,
                                      ReferenceRegion* overlapRegion);

// Dodging匀光
GDALDatasetH dodgingBalance(GDALDatasetH hDS, int blockSize, double strength);

// 多影像匀色
GDALDatasetH* batchColorBalance(GDALDatasetH* datasets, int count,
                                 GDALDatasetH refDS, ColorBalanceParams* params);

// 渐变融合
GDALDatasetH gradientBlend(GDALDatasetH ds1, GDALDatasetH ds2,
                           ReferenceRegion* overlapRegion, int blendWidth);

// ==================== 辅助函数 ====================

// 释放统计信息
void freeColorStatistics(ColorStatistics* stats);
void freeBandStatistics(BandStatistics* stats);

// 创建查找表
unsigned char* createLUT(LevelsParams* params);
unsigned char* createCurveLUT(CurveParams* params);
unsigned char* createGammaLUT(double gamma);

// 应用查找表
int applyLUT(GDALDatasetH hDS, unsigned char* lut, int bandIndex);

// RGB <-> HSL 转换
void rgbToHsl(double r, double g, double b, double* h, double* s, double* l);
void hslToRgb(double h, double s, double l, double* r, double* g, double* b);

// RGB <-> HSV 转换
void rgbToHsv(double r, double g, double b, double* h, double* s, double* v);
void hsvToRgb(double h, double s, double v, double* r, double* g, double* b);

// 计算直方图
int* calculateHistogram(GDALDatasetH hDS, int bandIndex, ReferenceRegion* region);

// 计算累积直方图
double* calculateCumulativeHistogram(int* histogram, int size);

#ifdef __cplusplus
}
#endif

#endif // OSGEO_COLOR_H
