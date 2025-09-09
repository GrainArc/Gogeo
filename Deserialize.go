package Gogeo

/*
#include "osgeo_utils.h"
#include <stdlib.h>
#include <string.h>

// 反序列化结果结构
typedef struct {
    OGRLayerH layer;
    OGRDataSourceH dataSource;
    int success;
    char* errorMessage;
} DeserializeResult;

// 安全的内存读取函数
int safeMemcpy(void* dest, const void* src, size_t n, size_t remaining) {
    if (remaining < n) return 0;
    memcpy(dest, src, n);
    return 1;
}

// 从WKB数据创建几何对象
OGRGeometryH createGeometryFromWKB(unsigned char* wkbData, int wkbSize) {
    if (!wkbData || wkbSize <= 0) return NULL;

    OGRGeometryH hGeom = NULL;
    OGRErr err = OGR_G_CreateFromWkb(wkbData, NULL, &hGeom, wkbSize);

    if (err != OGRERR_NONE) {
        if (hGeom) OGR_G_DestroyGeometry(hGeom);
        return NULL;
    }

    return hGeom;
}

// 反序列化单个要素
OGRFeatureH deserializeFeature(unsigned char* buffer, size_t bufferSize,
                              OGRFeatureDefnH hDefn, size_t* bytesRead) {
    if (!buffer || !hDefn || !bytesRead || bufferSize == 0) return NULL;

    unsigned char* ptr = buffer;
    size_t remainingSize = bufferSize;
    *bytesRead = 0;

    // 读取要素ID
    GIntBig fid;
    if (!safeMemcpy(&fid, ptr, sizeof(GIntBig), remainingSize)) return NULL;
    ptr += sizeof(GIntBig);
    remainingSize -= sizeof(GIntBig);
    *bytesRead += sizeof(GIntBig);

    // 读取字段数量
    int fieldCount;
    if (!safeMemcpy(&fieldCount, ptr, sizeof(int), remainingSize)) return NULL;
    ptr += sizeof(int);
    remainingSize -= sizeof(int);
    *bytesRead += sizeof(int);

    // 验证字段数量的合理性
    if (fieldCount < 0 || fieldCount > 10000) return NULL;

    // 创建要素
    OGRFeatureH hFeature = OGR_F_Create(hDefn);
    if (!hFeature) return NULL;

    OGR_F_SetFID(hFeature, fid);

    int layerFieldCount = OGR_FD_GetFieldCount(hDefn);

    // 读取字段数据
    for (int i = 0; i < fieldCount; i++) {
        // 读取字段类型
        int fieldType;
        if (!safeMemcpy(&fieldType, ptr, sizeof(int), remainingSize)) {
            OGR_F_Destroy(hFeature);
            return NULL;
        }
        ptr += sizeof(int);
        remainingSize -= sizeof(int);
        *bytesRead += sizeof(int);

        // 读取数据长度
        int dataSize;
        if (!safeMemcpy(&dataSize, ptr, sizeof(int), remainingSize)) {
            OGR_F_Destroy(hFeature);
            return NULL;
        }
        ptr += sizeof(int);
        remainingSize -= sizeof(int);
        *bytesRead += sizeof(int);

        // 验证数据长度的合理性
        if (dataSize < 0 || dataSize > remainingSize) {
            OGR_F_Destroy(hFeature);
            return NULL;
        }

        // 只处理在图层定义范围内的字段
        if (i < layerFieldCount) {
            // 根据字段类型读取数据
            switch (fieldType) {
                case OFTInteger: {
                    if (dataSize == sizeof(int)) {
                        int value;
                        if (safeMemcpy(&value, ptr, sizeof(int), remainingSize)) {
                            OGR_F_SetFieldInteger(hFeature, i, value);
                        }
                    }
                    break;
                }
                case OFTInteger64: {
                    if (dataSize == sizeof(GIntBig)) {
                        GIntBig value;
                        if (safeMemcpy(&value, ptr, sizeof(GIntBig), remainingSize)) {
                            OGR_F_SetFieldInteger64(hFeature, i, value);
                        }
                    }
                    break;
                }
                case OFTReal: {
                    if (dataSize == sizeof(double)) {
                        double value;
                        if (safeMemcpy(&value, ptr, sizeof(double), remainingSize)) {
                            OGR_F_SetFieldDouble(hFeature, i, value);
                        }
                    }
                    break;
                }
                case OFTString: {
                    if (dataSize > 0 && dataSize <= remainingSize) {
                        char* str = (char*)malloc(dataSize + 1);
                        if (str) {
                            memset(str, 0, dataSize + 1);
                            memcpy(str, ptr, dataSize);
                            OGR_F_SetFieldString(hFeature, i, str);
                            free(str);
                        }
                    }
                    break;
                }
                case OFTBinary: {
                    if (dataSize > 0 && dataSize <= remainingSize) {
                        OGR_F_SetFieldBinary(hFeature, i, dataSize, ptr);
                    }
                    break;
                }
                default:
                    // 跳过未支持的字段类型
                    break;
            }
        }

        // 移动指针，即使没有处理数据也要跳过
        ptr += dataSize;
        remainingSize -= dataSize;
        *bytesRead += dataSize;
    }

    // 读取几何数据
    int wkbSize;
    if (!safeMemcpy(&wkbSize, ptr, sizeof(int), remainingSize)) {
        OGR_F_Destroy(hFeature);
        return NULL;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);
    *bytesRead += sizeof(int);

    if (wkbSize > 0) {
        if (wkbSize <= remainingSize && wkbSize < 100000000) { // 100MB上限
            OGRGeometryH hGeom = createGeometryFromWKB(ptr, wkbSize);
            if (hGeom) {
                OGR_F_SetGeometry(hFeature, hGeom);
                OGR_G_DestroyGeometry(hGeom);
            }

            ptr += wkbSize;
            remainingSize -= wkbSize;
            *bytesRead += wkbSize;
        } else {
            OGR_F_Destroy(hFeature);
            return NULL;
        }
    }

    return hFeature;
}

// 从二进制数据反序列化图层
DeserializeResult deserializeLayerFromBinary(unsigned char* buffer, size_t bufferSize) {
    DeserializeResult result = {NULL, NULL, 0, NULL};

    if (!buffer || bufferSize < 24) {
        result.errorMessage = (char*)malloc(50);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Invalid buffer or insufficient size");
        }
        return result;
    }

    unsigned char* ptr = buffer;
    size_t remainingSize = bufferSize;

    // 验证魔数
    if (memcmp(ptr, "GDALLYR2", 8) != 0) {
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Invalid magic number");
        }
        return result;
    }
    ptr += 8;
    remainingSize -= 8;

    // 读取版本号
    int version;
    if (!safeMemcpy(&version, ptr, sizeof(int), remainingSize)) {
        result.errorMessage = (char*)malloc(40);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Cannot read version");
        }
        return result;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);

    if (version != 2) {
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Unsupported version");
        }
        return result;
    }

    // 读取几何类型
    OGRwkbGeometryType geomType;
    if (!safeMemcpy(&geomType, ptr, sizeof(int), remainingSize)) {
        result.errorMessage = (char*)malloc(40);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Cannot read geometry type");
        }
        return result;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);

    // 读取空间参考系统
    int srsWKTSize;
    if (!safeMemcpy(&srsWKTSize, ptr, sizeof(int), remainingSize)) {
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Cannot read SRS size");
        }
        return result;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);

    // 验证SRS大小的合理性
    if (srsWKTSize < 0 || srsWKTSize > remainingSize) {
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Invalid SRS size");
        }
        return result;
    }

    OGRSpatialReferenceH hSRS = NULL;
    if (srsWKTSize > 0) {
        char* srsWKT = (char*)malloc(srsWKTSize + 1);
        if (srsWKT) {
            memset(srsWKT, 0, srsWKTSize + 1);
            memcpy(srsWKT, ptr, srsWKTSize);

            hSRS = OSRNewSpatialReference(NULL);
            if (hSRS) {
                char* srsWKTPtr = srsWKT;
                if (OSRImportFromWkt(hSRS, &srsWKTPtr) != OGRERR_NONE) {
                    OSRDestroySpatialReference(hSRS);
                    hSRS = NULL;
                }
            }
            free(srsWKT);
        }

        ptr += srsWKTSize;
        remainingSize -= srsWKTSize;
    }

    // 创建内存数据源和图层
    OGRSFDriverH hDriver = OGRGetDriverByName("MEM");
    if (!hDriver) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Failed to get MEM driver");
        }
        return result;
    }

    OGRDataSourceH hDS = OGR_Dr_CreateDataSource(hDriver, "", NULL);
    if (!hDS) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        result.errorMessage = (char*)malloc(40);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Failed to create data source");
        }
        return result;
    }

    OGRLayerH hLayer = OGR_DS_CreateLayer(hDS, "deserialized_layer", hSRS, geomType, NULL);
    if (!hLayer) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        OGR_DS_Destroy(hDS);
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Failed to create layer");
        }
        return result;
    }

    // 读取字段定义
    int fieldCount;
    if (!safeMemcpy(&fieldCount, ptr, sizeof(int), remainingSize)) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        OGR_DS_Destroy(hDS);
        result.errorMessage = (char*)malloc(40);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Cannot read field count");
        }
        return result;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);

    // 验证字段数量的合理性
    if (fieldCount < 0 || fieldCount > 1000) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        OGR_DS_Destroy(hDS);
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Invalid field count");
        }
        return result;
    }

    // 创建字段定义
    for (int i = 0; i < fieldCount; i++) {
        // 读取字段类型
        int fieldType;
        if (!safeMemcpy(&fieldType, ptr, sizeof(int), remainingSize)) break;
        ptr += sizeof(int);
        remainingSize -= sizeof(int);

        // 读取字段名长度
        int nameLen;
        if (!safeMemcpy(&nameLen, ptr, sizeof(int), remainingSize)) break;
        ptr += sizeof(int);
        remainingSize -= sizeof(int);

        // 验证名称长度
        if (nameLen <= 0 || nameLen > remainingSize || nameLen > 1000) break;

        // 读取字段名
        char* fieldName = (char*)malloc(nameLen + 1);
        if (!fieldName) break;

        memset(fieldName, 0, nameLen + 1);
        memcpy(fieldName, ptr, nameLen);
        ptr += nameLen;
        remainingSize -= nameLen;

        // 读取字段宽度和精度
        int fieldWidth, fieldPrecision;
        if (!safeMemcpy(&fieldWidth, ptr, sizeof(int), remainingSize) ||
            !safeMemcpy(&fieldPrecision, ptr + sizeof(int), sizeof(int), remainingSize - sizeof(int))) {
            free(fieldName);
            break;
        }
        ptr += sizeof(int) * 2;
        remainingSize -= sizeof(int) * 2;

        // 创建字段定义
        OGRFieldDefnH hFieldDefn = OGR_Fld_Create(fieldName, fieldType);
        if (hFieldDefn) {
            OGR_Fld_SetWidth(hFieldDefn, fieldWidth);
            OGR_Fld_SetPrecision(hFieldDefn, fieldPrecision);
            OGR_L_CreateField(hLayer, hFieldDefn, 1);
            OGR_Fld_Destroy(hFieldDefn);
        }

        free(fieldName);
    }

    // 读取要素数量
    int featureCount;
    if (!safeMemcpy(&featureCount, ptr, sizeof(int), remainingSize)) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        OGR_DS_Destroy(hDS);
        result.errorMessage = (char*)malloc(40);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Cannot read feature count");
        }
        return result;
    }
    ptr += sizeof(int);
    remainingSize -= sizeof(int);

    // 验证要素数量
    if (featureCount < 0 || featureCount > 1000000) {
        if (hSRS) OSRDestroySpatialReference(hSRS);
        OGR_DS_Destroy(hDS);
        result.errorMessage = (char*)malloc(30);
        if (result.errorMessage) {
            strcpy(result.errorMessage, "Invalid feature count");
        }
        return result;
    }

    // 获取图层定义
    OGRFeatureDefnH hDefn = OGR_L_GetLayerDefn(hLayer);

    // 读取要素数据
    for (int i = 0; i < featureCount && remainingSize > sizeof(size_t); i++) {
        // 读取要素数据大小
        size_t featureSize;
        if (!safeMemcpy(&featureSize, ptr, sizeof(size_t), remainingSize)) break;
        ptr += sizeof(size_t);
        remainingSize -= sizeof(size_t);

        // 验证要素大小
        if (featureSize == 0 || featureSize > remainingSize || featureSize > 10000000) break;

        // 反序列化要素
        size_t bytesRead;
        OGRFeatureH hFeature = deserializeFeature(ptr, featureSize, hDefn, &bytesRead);

        if (hFeature) {
            OGR_L_CreateFeature(hLayer, hFeature);
            OGR_F_Destroy(hFeature);
        }

        ptr += featureSize;
        remainingSize -= featureSize;
    }

    if (hSRS) OSRDestroySpatialReference(hSRS);

    result.layer = hLayer;
    result.dataSource = hDS;
    result.success = 1;

    return result;
}

*/
import "C"

import (
	"errors"
	"fmt"
	"io/ioutil"
	"unsafe"
)

// DeserializeResult 反序列化结果
type DeserializeResult struct {
	Layer        *GDALLayer
	Success      bool
	ErrorMessage string
}

// DeserializeLayerFromBinary 从二进制数据反序列化图层（修复版本）
func DeserializeLayerFromBinary(data []byte) (*DeserializeResult, error) {
	if len(data) == 0 {
		return &DeserializeResult{
			Success:      false,
			ErrorMessage: "empty data buffer",
		}, errors.New("empty data buffer")
	}

	// 直接使用原始数据，不创建副本
	result := C.deserializeLayerFromBinary(
		(*C.uchar)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	goResult := &DeserializeResult{
		Success: result.success == 1,
	}

	if result.success == 1 {
		goResult.Layer = &GDALLayer{
			layer:   result.layer,
			dataset: result.dataSource,
			driver:  nil,
		}
	} else {
		if result.errorMessage != nil {
			goResult.ErrorMessage = C.GoString(result.errorMessage)
			C.free(unsafe.Pointer(result.errorMessage))
		} else {
			goResult.ErrorMessage = "unknown deserialization error"
		}
	}

	if !goResult.Success {
		return goResult, fmt.Errorf("deserialization failed: %s", goResult.ErrorMessage)
	}

	return goResult, nil
}


// DeserializeLayerFromFile 从bin文件反序列化图层（修复版本）
func DeserializeLayerFromFile(filePath string) (*GDALLayer, error) {
	// 读取文件内容
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", filePath, err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty file: %s", filePath)
	}
	InitializeGDAL()
	// 反序列化
	result, err := DeserializeLayerFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize from file %s: %v", filePath, err)
	}

	if !result.Success {
		return nil, fmt.Errorf("deserialization failed for file %s: %s", filePath, result.ErrorMessage)
	}

	return result.Layer, nil
}

// SafeDeserializeLayerFromFile 安全版本的文件反序列化
func SafeDeserializeLayerFromFile(filePath string) (*GDALLayer, error) {
	// 首先验证文件格式
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", filePath, err)
	}

	if err := ValidateBinaryFormat(data); err != nil {
		return nil, fmt.Errorf("invalid file format %s: %v", filePath, err)
	}

	// 然后进行反序列化
	return DeserializeLayerFromFile(filePath)
}

// BatchDeserializeFromFiles 批量从bin文件反序列化图层（修复版本）
func BatchDeserializeFromFiles(filePaths []string) (map[string]*GDALLayer, []error) {
	layers := make(map[string]*GDALLayer)
	var errors []error

	for _, filePath := range filePaths {
		layer, err := SafeDeserializeLayerFromFile(filePath)
		if err != nil {
			errors = append(errors, fmt.Errorf("file %s: %v", filePath, err))
			continue
		}

		layers[filePath] = layer
	}

	return layers, errors
}

// ValidateBinaryFormat 验证二进制文件格式是否正确（修复版本）
func ValidateBinaryFormat(data []byte) error {
	if len(data) < 12 { // 魔数(8) + 版本(4)
		return errors.New("insufficient data size")
	}

	// 检查魔数
	magic := string(data[:8])
	if magic != "GDALLYR2" {
		return fmt.Errorf("invalid magic number: expected 'GDALLYR2', got '%s'", magic)
	}

	// 安全地读取版本号
	if len(data) < 12 {
		return errors.New("insufficient data for version")
	}

	version := int32(data[8]) | int32(data[9])<<8 | int32(data[10])<<16 | int32(data[11])<<24
	if version != 2 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	return nil
}

// GetBinaryMetadata 获取二进制文件的元数据信息（修复版本）
func GetBinaryMetadata(data []byte) (*LayerMetadata, error) {
	if err := ValidateBinaryFormat(data); err != nil {
		return nil, err
	}

	metadata := &LayerMetadata{}
	ptr := 12 // 跳过魔数和版本

	if len(data) < ptr+4 {
		return nil, errors.New("insufficient data for geometry type")
	}

	// 安全地读取几何类型
	geomType := int32(data[ptr]) | int32(data[ptr+1])<<8 | int32(data[ptr+2])<<16 | int32(data[ptr+3])<<24
	metadata.GeometryType = int(geomType)
	ptr += 4

	if len(data) < ptr+4 {
		return nil, errors.New("insufficient data for SRS size")
	}

	// 安全地读取SRS信息
	srsSize := int32(data[ptr]) | int32(data[ptr+1])<<8 | int32(data[ptr+2])<<16 | int32(data[ptr+3])<<24
	metadata.SRSSize = int(srsSize)
	ptr += 4

	if srsSize > 0 {
		if srsSize < 0 || len(data) < ptr+int(srsSize) {
			return nil, errors.New("insufficient data for SRS or invalid SRS size")
		}

		if srsSize > 1 {
			metadata.SRSWKT = string(data[ptr : ptr+int(srsSize)-1]) // 去掉null终止符
		}
		ptr += int(srsSize)
	}

	if len(data) < ptr+4 {
		return nil, errors.New("insufficient data for field count")
	}

	// 安全地读取字段数量
	fieldCount := int32(data[ptr]) | int32(data[ptr+1])<<8 | int32(data[ptr+2])<<16 | int32(data[ptr+3])<<24
	metadata.FieldCount = int(fieldCount)

	return metadata, nil
}

// LayerMetadata 图层元数据
type LayerMetadata struct {
	GeometryType int
	SRSWKT       string
	SRSSize      int
	FieldCount   int
}

// CleanupDeserializedLayer 清理反序列化的图层资源（修复版本）
func (layer *GDALLayer) CleanupDeserializedLayer() {
	if layer != nil && layer.dataset != nil {
		C.OGR_DS_Destroy(layer.dataset)
		layer.dataset = nil
		layer.layer = nil
		layer.driver = nil
	}
}
