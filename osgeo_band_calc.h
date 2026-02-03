// osgeo_band_calc.h
#ifndef OSGEO_BAND_CALC_H
#define OSGEO_BAND_CALC_H

#include "gdal.h"

#ifdef __cplusplus
extern "C" {
#endif

// 编译后的表达式（不透明类型）
typedef struct CompiledExpression CompiledExpression;
typedef struct BlockCalculator BlockCalculator;

// 表达式编译与释放
CompiledExpression* compileExpression(const char* expression);
void freeCompiledExpression(CompiledExpression* ce);

// 波段表达式计算
double* calculateBandExpression(GDALDatasetH hDS, const char* expression, int* outSize);

// 带条件的计算
double* calculateBandExpressionWithCondition(GDALDatasetH hDS,
                                              const char* expression,
                                              const char* condition,
                                              double noDataValue,
                                              int* outSize);

// 条件替换
double* conditionalReplace(GDALDatasetH hDS, int bandIndex,
                           double* minValues, double* maxValues,
                           double* newValues, int* includeMin, int* includeMax,
                           int conditionCount, int* outSize);

// 分块计算器（用于超大影像）
BlockCalculator* createBlockCalculator(GDALDatasetH hDS, const char* expression,
                                        int blockWidth, int blockHeight);
void freeBlockCalculator(BlockCalculator* bc);
double* calculateBlock(BlockCalculator* bc, int blockX, int blockY,
                       int* outWidth, int* outHeight);

// 预定义指数计算
double* calculateNDVI(GDALDatasetH hDS, int nirBand, int redBand, int* outSize);
double* calculateNDWI(GDALDatasetH hDS, int greenBand, int nirBand, int* outSize);
double* calculateEVI(GDALDatasetH hDS, int nirBand, int redBand, int blueBand, int* outSize);
void freeBandCalcResult(double* ptr);
#ifdef __cplusplus
}
#endif

#endif // OSGEO_BAND_CALC_H
