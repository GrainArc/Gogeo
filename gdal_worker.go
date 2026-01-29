// gdal_worker.go
package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// ==================== GDAL 工作池 ====================

// TileRequest 瓦片生成请求
type TileRequest struct {
	Features []VectorFeature
	SRID     int
	GeomType C.OGRwkbGeometryType
	Bounds   VectorTileBounds
	Config   VectorTileConfig
	ResultCh chan WmtsTileResult
}

// WmtsTileResult 瓦片生成结果
type WmtsTileResult struct {
	Data []byte
	Err  error
}

// GDALWorkerPool GDAL工作池
type GDALWorkerPool struct {
	workers    int
	taskQueues []chan *TileRequest
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	counter    uint64
	mu         sync.Mutex
}

var (
	globalPool     *GDALWorkerPool
	globalPoolOnce sync.Once
)

// GetGDALWorkerPool 获取全局GDAL工作池
func GetGDALWorkerPool() *GDALWorkerPool {
	globalPoolOnce.Do(func() {
		// 工作者数量 = CPU核心数，每个工作者串行执行GDAL操作
		workerCount := runtime.NumCPU()
		if workerCount < 2 {
			workerCount = 2
		}
		if workerCount > 8 {
			workerCount = 8 // 限制最大工作者数
		}

		ctx, cancel := context.WithCancel(context.Background())
		globalPool = &GDALWorkerPool{
			workers:    workerCount,
			taskQueues: make([]chan *TileRequest, workerCount),
			ctx:        ctx,
			cancel:     cancel,
		}

		// 启动工作者
		for i := 0; i < workerCount; i++ {
			globalPool.taskQueues[i] = make(chan *TileRequest, 100) // 每个队列缓冲100个请求
			globalPool.wg.Add(1)
			go globalPool.worker(i)
		}

		fmt.Printf("GDAL工作池已启动，工作者数量: %d\n", workerCount)
	})
	return globalPool
}

// worker 工作者goroutine - 串行执行所有GDAL操作
func (p *GDALWorkerPool) worker(id int) {
	defer p.wg.Done()

	// 每个工作者绑定到固定的OS线程，避免GDAL线程问题
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for {
		select {
		case <-p.ctx.Done():
			return
		case req := <-p.taskQueues[id]:
			if req == nil {
				continue
			}
			result := p.processRequest(req)
			req.ResultCh <- result
		}
	}
}

// processRequest 处理单个请求（在工作者goroutine中串行执行）
func (p *GDALWorkerPool) processRequest(req *TileRequest) WmtsTileResult {
	if len(req.Features) == 0 {
		return WmtsTileResult{Err: fmt.Errorf("要素列表为空")}
	}

	// 创建矢量图层
	layer, err := createVectorLayerInternal(req.Features, req.SRID, req.GeomType)
	if err != nil {
		return WmtsTileResult{Err: err}
	}
	defer destroyLayerInternal(layer)

	// 栅格化
	data, err := rasterizeLayerInternal(layer, req.Bounds, req.Config)
	if err != nil {
		return WmtsTileResult{Err: err}
	}

	return WmtsTileResult{Data: data}
}

// Submit 提交任务到工作池
func (p *GDALWorkerPool) Submit(req *TileRequest) WmtsTileResult {
	// 轮询分配到不同的工作者
	p.mu.Lock()
	workerID := int(p.counter % uint64(p.workers))
	p.counter++
	p.mu.Unlock()

	req.ResultCh = make(chan WmtsTileResult, 1)

	select {
	case p.taskQueues[workerID] <- req:
		// 等待结果，设置超时
		select {
		case result := <-req.ResultCh:
			return result
		case <-time.After(30 * time.Second):
			return WmtsTileResult{Err: fmt.Errorf("瓦片生成超时")}
		}
	default:
		// 队列满，尝试其他工作者
		for i := 0; i < p.workers; i++ {
			select {
			case p.taskQueues[i] <- req:
				select {
				case result := <-req.ResultCh:
					return result
				case <-time.After(30 * time.Second):
					return WmtsTileResult{Err: fmt.Errorf("瓦片生成超时")}
				}
			default:
				continue
			}
		}
		return WmtsTileResult{Err: fmt.Errorf("工作池繁忙")}
	}
}

// Shutdown 关闭工作池
func (p *GDALWorkerPool) Shutdown() {
	p.cancel()
	p.wg.Wait()
}

// ==================== 内部GDAL操作（无锁，由工作者串行调用） ====================

// internalLayer 内部图层结构
type internalLayer struct {
	dataset C.OGRDataSourceH
	layer   C.OGRLayerH
}

// createVectorLayerInternal 创建矢量图层（内部使用，无锁）
func createVectorLayerInternal(features []VectorFeature, srid int, geomType C.OGRwkbGeometryType) (*internalLayer, error) {
	driverName := C.CString("MEM")
	defer C.free(unsafe.Pointer(driverName))

	driver := C.OGRGetDriverByName(driverName)
	if driver == nil {
		return nil, fmt.Errorf("无法获取MEM驱动")
	}

	dsName := C.CString("")
	defer C.free(unsafe.Pointer(dsName))

	ds := C.OGR_Dr_CreateDataSource(driver, dsName, nil)
	if ds == nil {
		return nil, fmt.Errorf("创建内存数据源失败")
	}

	// 创建空间参考
	srs := C.OSRNewSpatialReference(nil)
	if srs == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建空间参考失败")
	}
	defer C.OSRDestroySpatialReference(srs)

	if C.OSRImportFromEPSG(srs, C.int(srid)) != C.OGRERR_NONE {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("设置EPSG失败")
	}

	// 确保Multi类型
	multiGeomType := ensureMultiType(geomType)

	layerName := C.CString("layer")
	defer C.free(unsafe.Pointer(layerName))

	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, multiGeomType, nil)
	if layer == nil {
		C.OGR_DS_Destroy(ds)
		return nil, fmt.Errorf("创建图层失败")
	}

	// 创建字段
	createFieldsInternal(layer, features)

	// 添加要素
	addFeaturesInternal(layer, features, srs, multiGeomType)

	return &internalLayer{dataset: ds, layer: layer}, nil
}

// destroyLayerInternal 销毁图层
func destroyLayerInternal(l *internalLayer) {
	if l != nil && l.dataset != nil {
		C.OGR_DS_Destroy(l.dataset)
	}
}

// createFieldsInternal 创建字段
func createFieldsInternal(layer C.OGRLayerH, features []VectorFeature) {
	if len(features) == 0 || len(features[0].Attributes) == 0 {
		return
	}

	for attrName, attrValue := range features[0].Attributes {
		fieldType := inferFieldTypeInternal(attrValue)
		fieldName := C.CString(attrName)
		fieldDefn := C.OGR_Fld_Create(fieldName, fieldType)

		if fieldDefn != nil {
			if fieldType == C.OFTString {
				C.OGR_Fld_SetWidth(fieldDefn, 254)
			}
			C.OGR_L_CreateField(layer, fieldDefn, 1)
			C.OGR_Fld_Destroy(fieldDefn)
		}
		C.free(unsafe.Pointer(fieldName))
	}
}

// addFeaturesInternal 添加要素
func addFeaturesInternal(layer C.OGRLayerH, features []VectorFeature, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) {
	featureDefn := C.OGR_L_GetLayerDefn(layer)
	if featureDefn == nil {
		return
	}

	for _, feat := range features {
		feature := C.OGR_F_Create(featureDefn)
		if feature == nil {
			continue
		}

		// 设置几何
		if len(feat.WKB) > 0 {
			setGeometryInternal(feature, feat.WKB, srs, multiGeomType)
		}

		// 设置属性
		setAttributesInternal(feature, feat.Attributes)

		C.OGR_L_CreateFeature(layer, feature)
		C.OGR_F_Destroy(feature)
	}
}

// setGeometryInternal 设置几何
func setGeometryInternal(feature C.OGRFeatureH, wkb []byte, srs C.OGRSpatialReferenceH, multiGeomType C.OGRwkbGeometryType) {
	var geom C.OGRGeometryH
	cWkb := (*C.uchar)(C.CBytes(wkb))
	defer C.free(unsafe.Pointer(cWkb))

	if C.OGR_G_CreateFromWkb(unsafe.Pointer(cWkb), srs, &geom, C.int(len(wkb))) != C.OGRERR_NONE || geom == nil {
		return
	}
	defer C.OGR_G_DestroyGeometry(geom)

	if C.OGR_G_IsEmpty(geom) != 0 {
		return
	}

	var finalGeom C.OGRGeometryH
	currentType := C.OGR_G_GetGeometryType(geom)

	if isMultiGeometryType(currentType) {
		finalGeom = C.OGR_G_Clone(geom)
	} else {
		finalGeom = convertToMultiGeometry(geom, multiGeomType)
	}

	if finalGeom != nil {
		C.OGR_F_SetGeometry(feature, finalGeom)
		C.OGR_G_DestroyGeometry(finalGeom)
	}
}

// setAttributesInternal 设置属性
func setAttributesInternal(feature C.OGRFeatureH, attributes map[string]string) {
	for attrName, attrValue := range attributes {
		cAttrName := C.CString(attrName)
		fieldIndex := C.OGR_F_GetFieldIndex(feature, cAttrName)

		if fieldIndex >= 0 {
			cValue := C.CString(attrValue)
			C.OGR_F_SetFieldString(feature, fieldIndex, cValue)
			C.free(unsafe.Pointer(cValue))
		}
		C.free(unsafe.Pointer(cAttrName))
	}
}

// rasterizeLayerInternal 栅格化图层
func rasterizeLayerInternal(l *internalLayer, bounds VectorTileBounds, config VectorTileConfig) ([]byte, error) {
	tileSize := config.TileSize
	if tileSize <= 0 {
		tileSize = 256
	}

	// 创建栅格数据集
	rasterDS := C.createRasterDataset(
		C.int(tileSize), C.int(tileSize), 4,
		C.double(bounds.MinLon), C.double(bounds.MinLat),
		C.double(bounds.MaxLon), C.double(bounds.MaxLat),
		4326,
	)
	if rasterDS == nil {
		return nil, fmt.Errorf("创建栅格数据集失败")
	}
	defer C.GDALClose(rasterDS)

	// 执行栅格化
	if err := rasterizeWithConfigInternal(rasterDS, l.layer, config); err != nil {
		return nil, err
	}

	// 转PNG
	return rasterToPNGInternal(rasterDS)
}

// rasterizeWithConfigInternal 根据配置栅格化
func rasterizeWithConfigInternal(rasterDS C.GDALDatasetH, layer C.OGRLayerH, config VectorTileConfig) error {
	C.OGR_L_ResetReading(layer)

	opacity := config.Opacity
	if opacity <= 0 || opacity > 1.0 {
		opacity = 1.0
	}
	alpha := C.int(opacity * 255)

	// 无颜色配置，使用默认灰色
	if len(config.ColorMap) == 0 {
		return rasterizeSingleColorInternal(rasterDS, layer, 128, 128, 128, alpha)
	}

	rule := config.ColorMap[0]

	// 单一颜色模式
	if rule.AttributeName == "默认" && rule.AttributeValue == "默认" {
		rgb := ParseColor(rule.Color)
		return rasterizeSingleColorInternal(rasterDS, layer, C.int(rgb.R), C.int(rgb.G), C.int(rgb.B), alpha)
	}

	// 按属性分类渲染
	if len(rule.ColorValues) > 0 {
		return rasterizeByAttributeInternal(rasterDS, layer, rule.AttributeName, rule.ColorValues, alpha)
	}

	// 默认颜色
	rgb := ParseColor(rule.Color)
	return rasterizeSingleColorInternal(rasterDS, layer, C.int(rgb.R), C.int(rgb.G), C.int(rgb.B), alpha)
}

// rasterizeSingleColorInternal 单色栅格化
func rasterizeSingleColorInternal(rasterDS C.GDALDatasetH, layer C.OGRLayerH, r, g, b, a C.int) error {
	result := C.rasterizeLayerWithColor(rasterDS, layer, r, g, b, a)
	if result != 0 {
		return fmt.Errorf("栅格化失败: %d", int(result))
	}
	return nil
}

// rasterizeByAttributeInternal 按属性栅格化
func rasterizeByAttributeInternal(rasterDS C.GDALDatasetH, layer C.OGRLayerH, attrName string, colorValues map[string]string, alpha C.int) error {
	for attrValue, colorStr := range colorValues {
		rgb := ParseColor(colorStr)

		cAttrName := C.CString(attrName)
		cAttrValue := C.CString(attrValue)

		result := C.rasterizeLayerByAttribute(
			rasterDS, layer,
			cAttrName, cAttrValue,
			C.int(rgb.R), C.int(rgb.G), C.int(rgb.B), alpha,
		)

		C.free(unsafe.Pointer(cAttrName))
		C.free(unsafe.Pointer(cAttrValue))

		if result != 0 {
			// 清理并返回错误
			C.OGR_L_SetAttributeFilter(layer, nil)
			C.OGR_L_ResetReading(layer)
			return fmt.Errorf("按属性栅格化失败: %s=%s, 错误码=%d", attrName, attrValue, int(result))
		}
	}

	C.OGR_L_SetAttributeFilter(layer, nil)
	C.OGR_L_ResetReading(layer)
	return nil
}

// rasterToPNGInternal 转PNG
func rasterToPNGInternal(rasterDS C.GDALDatasetH) ([]byte, error) {
	imageBuffer := C.rasterDatasetToPNG(rasterDS)
	if imageBuffer == nil {
		return nil, fmt.Errorf("PNG转换失败")
	}
	defer C.freeImageBuffer(imageBuffer)

	if imageBuffer.size <= 0 || imageBuffer.data == nil {
		return nil, fmt.Errorf("PNG数据为空")
	}

	return C.GoBytes(unsafe.Pointer(imageBuffer.data), C.int(imageBuffer.size)), nil
}

// ==================== 辅助函数 ====================

func ensureMultiType(geomType C.OGRwkbGeometryType) C.OGRwkbGeometryType {
	if isMultiGeometryType(geomType) {
		return geomType
	}
	switch geomType {
	case C.wkbPoint:
		return C.wkbMultiPoint
	case C.wkbLineString:
		return C.wkbMultiLineString
	case C.wkbPolygon:
		return C.wkbMultiPolygon
	default:
		return C.wkbMultiPolygon
	}
}

func inferFieldTypeInternal(value string) C.OGRFieldType {
	return C.OFTString // 简化处理，全部作为字符串
}
