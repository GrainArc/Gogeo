package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type GeomType int

const (
	GeomUnknown         GeomType = 0
	GeomPoint           GeomType = 1
	GeomLineString      GeomType = 2
	GeomPolygon         GeomType = 3
	GeomMultiPoint      GeomType = 4
	GeomMultiLineString GeomType = 5
	GeomMultiPolygon    GeomType = 6
	GeomCollection      GeomType = 7
)

// ==================== 图层操作函数 ====================

// CreateMemoryLayer 创建内存图层
func CreateMemoryLayer(layerName string, geomType GeomType) (*GDALLayer, error) {

	driverName := C.CString("Memory")
	defer C.free(unsafe.Pointer(driverName))

	driver := C.OGRGetDriverByName(driverName)
	if driver == nil {
		return nil, fmt.Errorf("无法获取Memory驱动")
	}

	dsName := C.CString("")
	defer C.free(unsafe.Pointer(dsName))

	dataset := C.OGR_Dr_CreateDataSource(driver, dsName, nil)
	if dataset == nil {
		return nil, fmt.Errorf("创建数据源失败")
	}

	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))

	layer := C.OGR_DS_CreateLayer(dataset, cLayerName, nil, C.OGRwkbGeometryType(geomType), nil)
	if layer == nil {
		C.OGR_DS_Destroy(dataset)
		return nil, fmt.Errorf("创建图层失败")
	}

	return &GDALLayer{
		layer:   layer,
		dataset: dataset,
		driver:  driver,
	}, nil
}
