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

package Gogeo

/*
#cgo windows CFLAGS: -IC:/OSGeo4W/include -IC:/OSGeo4W/include/gdal -Wno-unused-result
#cgo windows LDFLAGS: -LC:/OSGeo4W/lib -lgdal_i -lstdc++ -static-libgcc -static-libstdc++
#cgo linux CFLAGS: -I/usr/include/gdal -Wno-unused-result
#cgo linux LDFLAGS: -L/usr/lib -lgdal -lstdc++
#cgo android CFLAGS: -I/data/data/com.termux/files/usr/include -Wno-unused-result
#cgo android LDFLAGS: -L/data/data/com.termux/files/usr/lib -lgdal
#include "osgeo_utils.h"

// 初始化GDAL并修复PROJ配置问题
void initializeGDALWithProjFix(const char* projDataPath, const char* shapeEncoding) {
    // 动态设置PROJ数据路径，支持自定义路径
    if (projDataPath && strlen(projDataPath) > 0) {
        CPLSetConfigOption("PROJ_DATA", projDataPath);      // 设置PROJ数据目录
        CPLSetConfigOption("PROJ_LIB", projDataPath);       // 设置PROJ库路径（兼容性）
    }

    // 强制使用传统的GIS轴顺序（经度，纬度），避免坐标轴混乱
    CPLSetConfigOption("OSR_DEFAULT_AXIS_MAPPING_STRATEGY", "TRADITIONAL_GIS_ORDER");


    // 动态设置Shapefile编码，支持不同字符集
    if (shapeEncoding && strlen(shapeEncoding) > 0) {
        CPLSetConfigOption("SHAPE_ENCODING", shapeEncoding); // 设置Shapefile文件编码
    }
	CPLSetErrorHandler(CPLQuietErrorHandler);
    // 注册所有GDAL驱动程序，启用栅格数据支持
    GDALAllRegister();
    // 注册所有OGR驱动程序，启用矢量数据支持
    OGRRegisterAll();

}

// 创建EPSG:4490坐标系并设置正确的轴顺序
OGRSpatialReferenceH createEPSG4490WithCorrectAxis() {
    // 创建新的空间参考系统对象
    OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
    if (!hSRS) {
        return NULL;  // 内存分配失败时返回NULL
    }

    // 导入EPSG:4490坐标系定义（中国大地坐标系2000）
    if (OSRImportFromEPSG(hSRS, 4490) != OGRERR_NONE) {
        OSRDestroySpatialReference(hSRS);  // 失败时清理资源
        return NULL;                        // 返回NULL表示创建失败
    }

    // 设置传统的GIS轴顺序（经度在前，纬度在后）
    OSRSetAxisMappingStrategy(hSRS, OAMS_TRADITIONAL_GIS_ORDER);

    return hSRS;  // 返回成功创建的空间参考系统
}

// 从十六进制WKB字符串创建几何对象
OGRGeometryH createGeometryFromWKBHex(const char* wkbHex) {
    // 验证输入参数有效性
    if (!wkbHex || strlen(wkbHex) == 0) {
        return NULL;  // 空输入时返回NULL
    }

    int hexLen = strlen(wkbHex);  // 获取十六进制字符串长度
    // 验证十六进制字符串长度必须为偶数
    if (hexLen % 2 != 0) {
        return NULL;  // 奇数长度无效
    }

    int wkbLen = hexLen / 2;  // 计算WKB二进制数据长度
    // 分配内存存储WKB二进制数据
    unsigned char* wkbData = (unsigned char*)malloc(wkbLen);
    if (!wkbData) {
        return NULL;  // 内存分配失败
    }

    // 将十六进制字符串转换为字节数组
    for (int i = 0; i < wkbLen; i++) {
        char hex[3] = {wkbHex[i*2], wkbHex[i*2+1], '\0'};  // 提取两个十六进制字符
        wkbData[i] = (unsigned char)strtol(hex, NULL, 16);  // 转换为字节值
    }

    OGRGeometryH hGeometry = NULL;  // 初始化几何对象指针
    OGRErr err;                     // 错误代码变量

    // 检查是否是EWKB格式（至少需要9字节：1字节序+4字节类型+4字节SRID）
    if (wkbLen >= 9) {
        uint32_t geomType;  // 几何类型变量
        // 根据字节序读取几何类型
        if (wkbData[0] == 1) { // 小端序（LSB）
            geomType = wkbData[1] | (wkbData[2] << 8) | (wkbData[3] << 16) | (wkbData[4] << 24);
        } else { // 大端序（MSB）
            geomType = (wkbData[1] << 24) | (wkbData[2] << 16) | (wkbData[3] << 8) | wkbData[4];
        }

        // 检查是否包含SRID信息（EWKB格式标志位）
        if (geomType & 0x20000000) {  // 包含SRID的EWKB格式
            // 创建不包含SRID的标准WKB数据
            unsigned char* standardWkb = (unsigned char*)malloc(wkbLen - 4);
            if (standardWkb) {
                // 复制字节序标识
                standardWkb[0] = wkbData[0];

                // 复制几何类型（移除SRID标志位）
                uint32_t cleanGeomType = geomType & ~0x20000000;
                if (wkbData[0] == 1) { // 小端序写入
                    standardWkb[1] = cleanGeomType & 0xFF;
                    standardWkb[2] = (cleanGeomType >> 8) & 0xFF;
                    standardWkb[3] = (cleanGeomType >> 16) & 0xFF;
                    standardWkb[4] = (cleanGeomType >> 24) & 0xFF;
                } else { // 大端序写入
                    standardWkb[1] = (cleanGeomType >> 24) & 0xFF;
                    standardWkb[2] = (cleanGeomType >> 16) & 0xFF;
                    standardWkb[3] = (cleanGeomType >> 8) & 0xFF;
                    standardWkb[4] = cleanGeomType & 0xFF;
                }

                // 跳过SRID（4字节），复制剩余几何数据
                memcpy(standardWkb + 5, wkbData + 9, wkbLen - 9);

                // 从标准WKB创建几何对象
                err = OGR_G_CreateFromWkb(standardWkb, NULL, &hGeometry, wkbLen - 4);
                free(standardWkb);  // 释放临时缓冲区
            }
        } else {
            // 标准WKB格式，直接解析
            err = OGR_G_CreateFromWkb(wkbData, NULL, &hGeometry, wkbLen);
        }
    } else {
        // 数据长度不足，尝试直接解析（可能是简单几何）
        err = OGR_G_CreateFromWkb(wkbData, NULL, &hGeometry, wkbLen);
    }

    free(wkbData);  // 释放WKB数据缓冲区

    // 检查几何对象创建是否成功
    if (err != OGRERR_NONE) {
        if (hGeometry) {
            OGR_G_DestroyGeometry(hGeometry);  // 失败时清理已分配的几何对象
        }
        return NULL;  // 返回NULL表示创建失败
    }

    return hGeometry;  // 返回成功创建的几何对象
}

// 从WKB数据中提取几何类型信息
uint32_t getGeometryTypeFromWKBData(const unsigned char* wkbData, int wkbLen) {
    // 验证输入参数（至少需要5字节：1字节序+4字节类型）
    if (!wkbData || wkbLen < 5) {
        return 0;  // 无效输入返回0
    }

    // 读取字节序标识（第一个字节）
    unsigned char byteOrder = wkbData[0];

    // 根据字节序读取几何类型（接下来4个字节）
    uint32_t geomType;
    if (byteOrder == 1) { // 小端序（LSB first）
        geomType = wkbData[1] | (wkbData[2] << 8) | (wkbData[3] << 16) | (wkbData[4] << 24);
    } else { // 大端序（MSB first）
        geomType = (wkbData[1] << 24) | (wkbData[2] << 16) | (wkbData[3] << 8) | wkbData[4];
    }

    // 移除SRID标志位，返回纯几何类型
    return geomType & ~0x20000000;
}

// 清理GDAL资源的辅助函数
void cleanupGDAL() {
    GDALDestroyDriverManager();  // 清理GDAL驱动管理器
    OGRCleanupAll();            // 清理所有OGR资源
}

void goErrorHandler(CPLErr eErrClass, int err_no, const char *msg) {
    // 可以在这里处理GDAL错误
}
*/
import "C"

import (
	"errors" // 用于创建错误对象
	"log"
	"os"      // 用于环境变量操作
	"runtime" // 用于运行时信息获取
	"unsafe"  // 用于C指针操作
)

type GeometryPrecisionConfig struct {
	GridSize      float64 // 精度网格大小，0表示浮点精度
	PreserveTopo  bool    // 是否保持拓扑结构
	KeepCollapsed bool    // 是否保留折叠的元素
	Enabled       bool    // 是否启用精度设置
}

// SpatialReference 表示空间参考系统的Go包装器
type SpatialReference struct {
	cPtr C.OGRSpatialReferenceH // C语言空间参考系统指针
}

// Geometry 表示几何对象的Go包装器
type Geometry struct {
	cPtr C.OGRGeometryH // C语言几何对象指针
}

// InitializeGDAL 初始化GDAL库，支持自定义配置

// shapeEncoding: Shapefile编码，为空时使用默认编码
func InitializeGDAL() error {
	// 如果未指定PROJ路径，根据操作系统设置默认路径
	projDataPath := ""
	if runtime.GOOS == "windows" {
		// Windows下优先使用环境变量，否则使用默认路径
		if envPath := os.Getenv("PROJ_DATA"); envPath != "" {
			projDataPath = envPath
		} else {
			projDataPath = "C:/OSGeo4W/share/proj" // Windows默认路径
		}
	} else if runtime.GOOS == "linux" {
		// Linux下优先使用环境变量，否则使用系统路径
		if envPath := os.Getenv("PROJ_DATA"); envPath != "" {
			projDataPath = envPath
		} else {
			projDataPath = "/usr/share/proj" // Linux默认路径
		}
	} else if runtime.GOOS == "android" {
		if envPath := os.Getenv("PROJ_DATA"); envPath != "" {
			projDataPath = envPath
		} else {
			projDataPath = "/data/data/com.termux/files/usr/share/proj" // Linux默认路径
		}
	}

	// 如果未指定编码，使用默认编码
	shapeEncoding := ""

	// 转换Go字符串为C字符串
	cProjPath := C.CString(projDataPath)
	cEncoding := C.CString(shapeEncoding)

	// 确保C字符串资源被释放
	defer C.free(unsafe.Pointer(cProjPath))
	defer C.free(unsafe.Pointer(cEncoding))

	// 调用C函数初始化GDAL
	C.initializeGDALWithProjFix(cProjPath, cEncoding)

	return nil // 初始化成功
}
func CreateEPSG4490WithCorrectAxis() (*SpatialReference, error) {
	// 调用C函数创建空间参考系统
	cPtr := C.createEPSG4490WithCorrectAxis()
	if cPtr == nil {
		return nil, errors.New("failed to create EPSG:4490 spatial reference system") // 创建失败
	}

	// 创建Go包装器对象
	srs := &SpatialReference{cPtr: cPtr}

	// 设置终结器，确保C资源被正确释放
	runtime.SetFinalizer(srs, (*SpatialReference).destroy)

	return srs, nil // 返回成功创建的对象
}

// CreateGeometryFromWKBHex 从十六进制WKB字符串创建几何对象
// wkbHex: 十六进制格式的WKB字符串
// 返回几何对象和可能的错误
func CreateGeometryFromWKBHex(wkbHex string) (*Geometry, error) {
	// 验证输入参数
	if wkbHex == "" {
		return nil, errors.New("WKB hex string cannot be empty") // 空字符串错误
	}

	// 转换Go字符串为C字符串
	cWkbHex := C.CString(wkbHex)
	defer C.free(unsafe.Pointer(cWkbHex)) // 确保C字符串被释放

	// 调用C函数创建几何对象
	cPtr := C.createGeometryFromWKBHex(cWkbHex)
	if cPtr == nil {
		return nil, errors.New("failed to create geometry from WKB hex string") // 创建失败
	}
	// 创建Go包装器对象
	geom := &Geometry{cPtr: cPtr}
	return geom, nil // 返回成功创建的对象
}

// GetGeometryTypeFromWKBData 从WKB数据中提取几何类型
// wkbData: WKB二进制数据
// 返回几何类型代码
func GetGeometryTypeFromWKBData(wkbData []byte) uint32 {
	// 验证输入数据长度
	if len(wkbData) < 5 {
		return 0 // 数据长度不足
	}

	// 调用C函数提取几何类型
	return uint32(C.getGeometryTypeFromWKBData((*C.uchar)(&wkbData[0]), C.int(len(wkbData))))
}

// CleanupGDAL 清理GDAL资源，程序退出前调用
func CleanupGDAL() {
	C.cleanupGDAL() // 调用C函数清理资源
}

// destroy 销毁空间参考系统的C资源（终结器函数）
func (srs *SpatialReference) destroy() {
	if srs.cPtr != nil {
		C.OSRDestroySpatialReference(srs.cPtr) // 释放C资源
		srs.cPtr = nil                         // 避免重复释放
	}
}

// destroy 销毁几何对象的C资源（终结器函数）
func (geom *Geometry) destroy() {
	if geom.cPtr != nil {
		// 添加错误恢复机制
		defer func() {
			if r := recover(); r != nil {
				log.Printf("警告: 销毁几何对象时发生错误: %v", r)
			}
			geom.cPtr = nil
		}()

		C.OGR_G_DestroyGeometry(geom.cPtr)
		geom.cPtr = nil
	}
}

// Destroy 手动销毁空间参考系统资源
func (srs *SpatialReference) Destroy() {
	runtime.SetFinalizer(srs, nil) // 移除终结器
	srs.destroy()                  // 立即释放资源
}

// Destroy 手动销毁几何对象资源
func (geom *Geometry) Destroy() {
	runtime.SetFinalizer(geom, nil) // 移除终结器
	geom.destroy()                  // 立即释放资源
}
