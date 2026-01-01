package Gogeo

/*
#include "osgeo_utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// ImportToGDBOptionsV2 导入选项（增强版，支持目标坐标系）
type ImportToGDBOptionsV2 struct {
	// 基础选项
	GeomField           string            // PostGIS几何字段名（默认"geom"）
	BatchSize           int               // 批处理大小（默认1000）
	UseTransaction      bool              // 是否使用事务（默认false）
	StrictMode          bool              // 严格模式，遇到错误立即停止
	SkipInvalidGeometry bool              // 跳过无效几何（默认false）
	ValidateGeometry    bool              // 验证几何有效性（默认false）
	AllowNullGeometry   bool              // 允许空几何（默认false）
	FieldMapping        map[string]string // 字段映射（源字段->目标字段）
	ExcludeFields       []string          // 排除的字段列表
	IncludeFields       []string          // 仅包含的字段列表
	// 坐标系选项（新增）
	TargetSRS        *GDBSpatialReference // 目标坐标系（如果为nil，则使用源坐标系或图层默认坐标系）
	SourceSRS        *GDBSpatialReference // 源坐标系（如果源数据没有坐标系信息时使用）
	ForceTransform   bool                 // 强制进行坐标转换（即使源和目标坐标系相同）
	TransformOptions *TransformOptions    // 坐标转换选项
}

// TransformOptions 坐标转换选项
type TransformOptions struct {
	// 转换精度控制
	AreaOfInterest *AreaOfInterest // 感兴趣区域（用于选择最佳转换方法）
	// 高程处理
	IncludeZ bool // 是否包含Z值转换
	// 错误处理
	OnTransformError TransformErrorAction // 转换错误时的处理方式
}

// AreaOfInterest 感兴趣区域
type AreaOfInterest struct {
	WestLongitude float64
	SouthLatitude float64
	EastLongitude float64
	NorthLatitude float64
}

// TransformErrorAction 转换错误处理方式
type TransformErrorAction int

const (
	TransformErrorSkip    TransformErrorAction = iota // 跳过该要素
	TransformErrorFail                                // 失败并停止
	TransformErrorKeepRaw                             // 保留原始坐标
)

// NewImportToGDBOptionsV2 创建默认的导入选项
func NewImportToGDBOptionsV2() *ImportToGDBOptionsV2 {
	return &ImportToGDBOptionsV2{
		GeomField:           "geom",
		BatchSize:           1000,
		UseTransaction:      true,
		StrictMode:          false,
		SkipInvalidGeometry: true,
		ValidateGeometry:    true,
		AllowNullGeometry:   false,
		TransformOptions: &TransformOptions{
			IncludeZ:         false,
			OnTransformError: TransformErrorSkip,
		},
	}
}

// WithTargetSRS 设置目标坐标系
func (opts *ImportToGDBOptionsV2) WithTargetSRS(srs *GDBSpatialReference) *ImportToGDBOptionsV2 {
	opts.TargetSRS = srs
	return opts
}

// WithSourceSRS 设置源坐标系
func (opts *ImportToGDBOptionsV2) WithSourceSRS(srs *GDBSpatialReference) *ImportToGDBOptionsV2 {
	opts.SourceSRS = srs
	return opts
}

// WithCGCS2000_3DegreeZone 设置目标坐标系为CGCS2000 3度带
func (opts *ImportToGDBOptionsV2) WithCGCS2000_3DegreeZone(zone int) (*ImportToGDBOptionsV2, error) {
	srs, err := GetCGCS2000_3DegreeZone(zone)
	if err != nil {
		return opts, err
	}
	opts.TargetSRS = srs
	return opts, nil
}

// WithCGCS2000_3DegreeByCM 设置目标坐标系为CGCS2000 3度带（按中央经线）
func (opts *ImportToGDBOptionsV2) WithCGCS2000_3DegreeByCM(centralMeridian int) (*ImportToGDBOptionsV2, error) {
	srs, err := GetCGCS2000_3DegreeByCentralMeridian(centralMeridian)
	if err != nil {
		return opts, err
	}
	opts.TargetSRS = srs
	return opts, nil
}

// =====================================================
// 优化后的导入函数
// =====================================================
// ImportPostGISToNewGDBLayerV2 将PostGIS数据表导入到GDB文件，创建新图层（支持目标坐标系）
func ImportPostGISToNewGDBLayerV2(postGISConfig *PostGISConfig, gdbPath string, layerName string, options *ImportToGDBOptionsV2) (*ImportResult, error) {

	// 设置默认选项
	if options == nil {
		options = NewImportToGDBOptionsV2()
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.GeomField == "" {
		options.GeomField = "geom"
	}
	result := &ImportResult{
		Errors: make([]string, 0),
	}
	// 1. 从PostGIS读取数据
	reader := NewPostGISReader(postGISConfig)
	sourceLayer, err := reader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取PostGIS表失败: %v", err)
	}
	defer sourceLayer.Close()
	// 2. 打开GDB数据源（可写模式）
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))
	// 获取FileGDB驱动
	cDriverName := C.CString("FileGDB")
	defer C.free(unsafe.Pointer(cDriverName))
	driver := C.OGRGetDriverByName(cDriverName)
	if driver == nil {
		cDriverName2 := C.CString("OpenFileGDB")
		defer C.free(unsafe.Pointer(cDriverName2))
		driver = C.OGRGetDriverByName(cDriverName2)
		if driver == nil {
			return nil, fmt.Errorf("无法获取GDB驱动")
		}
	}
	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)
	// 3. 检查图层是否已存在
	cLayerName := C.CString(layerName)
	defer C.free(unsafe.Pointer(cLayerName))
	existingLayer := C.OGR_DS_GetLayerByName(targetDataset, cLayerName)
	if existingLayer != nil {
		fmt.Printf("图层 '%s' 已存在，将追加数据\n", layerName)
		return ImportPostGISToGDBV2(postGISConfig, gdbPath, layerName, options)
	}
	// 4. 获取源图层信息
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	sourceGeomType := C.OGR_FD_GetGeomType(sourceLayerDefn)
	// 5. 确定源坐标系
	var sourceSRS C.OGRSpatialReferenceH
	sourceSRS = C.OGR_L_GetSpatialRef(sourceLayer.layer)
	// 如果源图层没有坐标系，使用选项中指定的源坐标系
	if sourceSRS == nil && options.SourceSRS != nil {
		var err error
		sourceSRS, err = options.SourceSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建源坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(sourceSRS)
		fmt.Printf("使用指定的源坐标系: %s\n", options.SourceSRS.String())
	}
	// 6. 确定目标坐标系
	var targetSRS C.OGRSpatialReferenceH
	var needTransform bool
	var transform C.OGRCoordinateTransformationH
	if options.TargetSRS != nil {
		// 使用指定的目标坐标系
		var err error
		targetSRS, err = options.TargetSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建目标坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(targetSRS)
		fmt.Printf("目标坐标系: %s\n", options.TargetSRS.String())
		// 检查是否需要坐标转换
		if sourceSRS != nil {
			if options.ForceTransform || C.OSRIsSame(sourceSRS, targetSRS) == 0 {
				transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
				if transform != nil {
					needTransform = true
					defer C.OCTDestroyCoordinateTransformation(transform)
					fmt.Println("已创建坐标转换对象")
				} else {
					return nil, fmt.Errorf("无法创建坐标转换对象")
				}
			}
		}
	} else {
		// 使用源坐标系
		targetSRS = sourceSRS
		if targetSRS != nil {
			fmt.Println("使用源图层坐标系作为目标坐标系")
		}
	}
	// 7. 创建新图层
	targetLayer := C.OGR_DS_CreateLayer(targetDataset, cLayerName, targetSRS, sourceGeomType, nil)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法创建图层: %s", layerName)
	}
	// 8. 复制字段定义
	err = copyFieldDefinitionsV2(sourceLayerDefn, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("复制字段定义失败: %v", err)
	}
	// 9. 创建字段映射
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)
	fieldMapping, err := createImportFieldMappingV2(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}
	// 10. 开始事务
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
		}
	}
	// 11. 导入要素
	C.OGR_L_ResetReading(sourceLayer.layer)
	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}
		result.TotalCount++
		// 创建目标要素
		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}
		// 处理几何
		geomProcessed := processGeometryWithTransform(
			sourceFeature, targetFeature,
			needTransform, transform,
			options, result,
		)
		if !geomProcessed {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			continue
		}
		// 复制属性字段
		err := copyImportFeatureFieldsV2(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)
		if err != nil && options.StrictMode {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: %v", result.TotalCount, err))
			continue
		}
		// 插入要素
		createResult := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if createResult == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
			result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: 插入失败", result.TotalCount))
		}
		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)
		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录，成功 %d 条...\n", result.TotalCount, result.InsertedCount)
		}
	}
	// 12. 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}
	// 13. 同步到磁盘
	C.OGR_L_SyncToDisk(targetLayer)
	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)
	return result, nil
}

// ImportPostGISToGDBV2 将PostGIS数据表导入到GDB文件的指定图层（支持目标坐标系）
func ImportPostGISToGDBV2(postGISConfig *PostGISConfig, gdbPath string, gdbLayerName string, options *ImportToGDBOptionsV2) (*ImportResult, error) {

	// 设置默认选项
	if options == nil {
		options = NewImportToGDBOptionsV2()
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	if options.GeomField == "" {
		options.GeomField = "geom"
	}
	result := &ImportResult{
		Errors: make([]string, 0),
	}
	// 1. 从PostGIS读取数据
	reader := NewPostGISReader(postGISConfig)
	sourceLayer, err := reader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取PostGIS表失败: %v", err)
	}
	defer sourceLayer.Close()
	// 2. 打开GDB数据源（可写模式）
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))
	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)
	// 3. 获取GDB目标图层
	cGDBLayerName := C.CString(gdbLayerName)
	defer C.free(unsafe.Pointer(cGDBLayerName))
	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cGDBLayerName)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法找到GDB图层: %s", gdbLayerName)
	}
	// 4. 获取源坐标系
	var sourceSRS C.OGRSpatialReferenceH
	sourceSRS = C.OGR_L_GetSpatialRef(sourceLayer.layer)
	if sourceSRS == nil && options.SourceSRS != nil {
		var err error
		sourceSRS, err = options.SourceSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建源坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(sourceSRS)
	}
	// 5. 确定目标坐标系和坐标转换
	var targetSRS C.OGRSpatialReferenceH
	var needTransform bool
	var transform C.OGRCoordinateTransformationH
	// 获取目标图层的坐标系
	layerSRS := C.OGR_L_GetSpatialRef(targetLayer)
	if options.TargetSRS != nil {
		// 使用指定的目标坐标系
		var err error
		targetSRS, err = options.TargetSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建目标坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(targetSRS)
		// 检查指定的目标坐标系是否与图层坐标系一致
		if layerSRS != nil && C.OSRIsSame(targetSRS, layerSRS) == 0 {
			fmt.Printf("警告: 指定的目标坐标系与图层坐标系不一致，将使用指定的坐标系进行转换\n")
		}
		fmt.Printf("目标坐标系: %s\n", options.TargetSRS.String())
	} else if layerSRS != nil {
		// 使用图层的坐标系
		targetSRS = layerSRS
		fmt.Println("使用目标图层的坐标系")
	}
	// 创建坐标转换
	if sourceSRS != nil && targetSRS != nil {
		if options.ForceTransform || C.OSRIsSame(sourceSRS, targetSRS) == 0 {
			transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
			if transform != nil {
				needTransform = true
				defer C.OCTDestroyCoordinateTransformation(transform)
				fmt.Println("已创建坐标转换对象")
			} else {
				fmt.Println("警告: 无法创建坐标转换对象，将使用原始坐标")
			}
		}
	}
	// 6. 验证几何类型
	err = validateGeometryTypesV2(sourceLayer.layer, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("几何类型验证失败: %v", err)
	}
	// 7. 创建字段映射
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)
	fieldMapping, err := createImportFieldMappingV2(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}
	fmt.Printf("字段映射: %d 个字段将被导入\n", len(fieldMapping))
	// 8. 开始事务
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
			fmt.Println("警告: 无法启动事务，将使用非事务模式")
		}
	}
	// 9. 重置源图层读取位置
	C.OGR_L_ResetReading(sourceLayer.layer)
	// 10. 遍历源图层并导入要素
	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}
		result.TotalCount++
		// 创建目标要素
		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			result.Errors = append(result.Errors, "无法创建目标要素")
			continue
		}
		// 处理几何（包含坐标转换）
		geomProcessed := processGeometryWithTransform(
			sourceFeature, targetFeature,
			needTransform, transform,
			options, result,
		)
		if !geomProcessed {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			continue
		}
		// 复制属性字段
		err := copyImportFeatureFieldsV2(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)
		if err != nil && options.StrictMode {
			errMsg := fmt.Sprintf("要素 %d: 复制字段失败: %v", result.TotalCount, err)
			result.Errors = append(result.Errors, errMsg)
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}
		// 插入要素到目标图层
		createResult := C.OGR_L_CreateFeature(targetLayer, targetFeature)
		if createResult == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
			errMsg := fmt.Sprintf("要素 %d: 插入失败，错误代码: %d", result.TotalCount, int(createResult))
			result.Errors = append(result.Errors, errMsg)
		}
		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)
		// 批量同步
		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录，成功插入 %d 条...\n", result.TotalCount, result.InsertedCount)
		}
	}
	// 11. 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}
	// 12. 最终同步到磁盘
	if C.OGR_L_SyncToDisk(targetLayer) != C.OGRERR_NONE {
		return result, fmt.Errorf("同步到磁盘失败")
	}
	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)
	printErrorSummary(result)
	return result, nil
}

// ImportGDALLayerToGDBV2 将GDALLayer直接导入到GDB（支持目标坐标系）
func ImportGDALLayerToGDBV2(sourceLayer *GDALLayer, gdbPath string, gdbLayerName string, options *ImportToGDBOptionsV2) (*ImportResult, error) {
	if sourceLayer == nil || sourceLayer.layer == nil {
		return nil, fmt.Errorf("源图层为空")
	}

	// 设置默认选项
	if options == nil {
		options = NewImportToGDBOptionsV2()
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	result := &ImportResult{
		Errors: make([]string, 0),
	}
	// 打开GDB数据源
	cGDBPath := C.CString(gdbPath)
	defer C.free(unsafe.Pointer(cGDBPath))
	targetDataset := C.OGROpen(cGDBPath, C.int(1), nil)
	if targetDataset == nil {
		return nil, fmt.Errorf("无法以可写模式打开GDB文件: %s", gdbPath)
	}
	defer C.OGR_DS_Destroy(targetDataset)
	// 获取目标图层
	cGDBLayerName := C.CString(gdbLayerName)
	defer C.free(unsafe.Pointer(cGDBLayerName))
	targetLayer := C.OGR_DS_GetLayerByName(targetDataset, cGDBLayerName)
	if targetLayer == nil {
		return nil, fmt.Errorf("无法找到GDB图层: %s", gdbLayerName)
	}
	// 验证几何类型
	err := validateGeometryTypesV2(sourceLayer.layer, targetLayer, options)
	if err != nil {
		return nil, fmt.Errorf("几何类型验证失败: %v", err)
	}
	// 获取源坐标系
	var sourceSRS C.OGRSpatialReferenceH
	sourceSRS = C.OGR_L_GetSpatialRef(sourceLayer.layer)
	if sourceSRS == nil && options.SourceSRS != nil {
		var err error
		sourceSRS, err = options.SourceSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建源坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(sourceSRS)
	}
	// 确定目标坐标系
	var targetSRS C.OGRSpatialReferenceH
	var needTransform bool
	var transform C.OGRCoordinateTransformationH
	layerSRS := C.OGR_L_GetSpatialRef(targetLayer)
	if options.TargetSRS != nil {
		var err error
		targetSRS, err = options.TargetSRS.ToOGRGDBSpatialReference()
		if err != nil {
			return nil, fmt.Errorf("创建目标坐标系失败: %v", err)
		}
		defer C.OSRDestroySpatialReference(targetSRS)
	} else if layerSRS != nil {
		targetSRS = layerSRS
	}
	// 创建坐标转换
	if sourceSRS != nil && targetSRS != nil {
		if options.ForceTransform || C.OSRIsSame(sourceSRS, targetSRS) == 0 {
			// 继续 ImportGDALLayerToGDBV2 函数
			transform = C.OCTNewCoordinateTransformation(sourceSRS, targetSRS)
			if transform != nil {
				needTransform = true
				defer C.OCTDestroyCoordinateTransformation(transform)
				fmt.Println("已创建坐标转换对象")
			} else {
				fmt.Println("警告: 无法创建坐标转换对象，将使用原始坐标")
			}
		}
	}

	// 创建字段映射
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer.layer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	fieldMapping, err := createImportFieldMappingV2(sourceLayerDefn, targetLayerDefn, options)
	if err != nil {
		return nil, fmt.Errorf("创建字段映射失败: %v", err)
	}

	// 开始事务
	useTransaction := options.UseTransaction && C.OGR_L_TestCapability(targetLayer, C.CString("Transactions")) != 0
	if useTransaction {
		if C.OGR_L_StartTransaction(targetLayer) != C.OGRERR_NONE {
			useTransaction = false
		}
	}

	// 导入要素
	C.OGR_L_ResetReading(sourceLayer.layer)

	batchCount := 0
	for {
		sourceFeature := C.OGR_L_GetNextFeature(sourceLayer.layer)
		if sourceFeature == nil {
			break
		}

		result.TotalCount++

		targetFeature := C.OGR_F_Create(targetLayerDefn)
		if targetFeature == nil {
			C.OGR_F_Destroy(sourceFeature)
			result.FailedCount++
			continue
		}

		// 处理几何（包含坐标转换）
		geomProcessed := processGeometryWithTransform(
			sourceFeature, targetFeature,
			needTransform, transform,
			options, result,
		)

		if !geomProcessed {
			C.OGR_F_Destroy(targetFeature)
			C.OGR_F_Destroy(sourceFeature)
			continue
		}

		// 复制字段
		copyImportFeatureFieldsV2(sourceFeature, targetFeature, fieldMapping, targetLayerDefn)

		// 插入要素
		if C.OGR_L_CreateFeature(targetLayer, targetFeature) == C.OGRERR_NONE {
			result.InsertedCount++
		} else {
			result.FailedCount++
		}

		C.OGR_F_Destroy(targetFeature)
		C.OGR_F_Destroy(sourceFeature)

		batchCount++
		if options.BatchSize > 0 && batchCount%options.BatchSize == 0 {
			C.OGR_L_SyncToDisk(targetLayer)
			fmt.Printf("已处理 %d 条记录...\n", result.TotalCount)
		}
	}

	// 提交事务
	if useTransaction {
		if C.OGR_L_CommitTransaction(targetLayer) != C.OGRERR_NONE {
			C.OGR_L_RollbackTransaction(targetLayer)
			return result, fmt.Errorf("提交事务失败")
		}
	}

	// 同步到磁盘
	C.OGR_L_SyncToDisk(targetLayer)

	fmt.Printf("导入完成: 总数=%d, 成功=%d, 失败=%d, 跳过=%d\n",
		result.TotalCount, result.InsertedCount, result.FailedCount, result.SkippedCount)

	return result, nil
}

// =====================================================
// 辅助函数
// =====================================================

// processGeometryWithTransform 处理几何对象（包含坐标转换）
func processGeometryWithTransform(
	sourceFeature, targetFeature C.OGRFeatureH,
	needTransform bool,
	transform C.OGRCoordinateTransformationH,
	options *ImportToGDBOptionsV2,
	result *ImportResult,
) bool {
	sourceGeom := C.OGR_F_GetGeometryRef(sourceFeature)

	// 检查源几何是否为空
	if sourceGeom == nil {
		if options.AllowNullGeometry {
			return true // 允许空几何，继续处理
		}
		result.SkippedCount++
		return false
	}

	// 克隆几何对象
	clonedGeom := C.OGR_G_Clone(sourceGeom)
	if clonedGeom == nil {
		result.FailedCount++
		result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: 克隆几何失败", result.TotalCount))
		return false
	}

	// 执行坐标转换
	if needTransform && transform != nil {
		transformResult := C.OGR_G_Transform(clonedGeom, transform)
		if transformResult != C.OGRERR_NONE {
			// 根据选项处理转换错误
			action := TransformErrorSkip
			if options.TransformOptions != nil {
				action = options.TransformOptions.OnTransformError
			}

			switch action {
			case TransformErrorSkip:
				C.OGR_G_DestroyGeometry(clonedGeom)
				result.SkippedCount++
				return false
			case TransformErrorFail:
				C.OGR_G_DestroyGeometry(clonedGeom)
				result.FailedCount++
				result.Errors = append(result.Errors, fmt.Sprintf("要素 %d: 坐标转换失败", result.TotalCount))
				return false
			case TransformErrorKeepRaw:
				// 保留原始坐标，重新克隆
				C.OGR_G_DestroyGeometry(clonedGeom)
				clonedGeom = C.OGR_G_Clone(sourceGeom)
				if clonedGeom == nil {
					result.FailedCount++
					return false
				}
				fmt.Printf("警告: 要素 %d 坐标转换失败，保留原始坐标\n", result.TotalCount)
			}
		}
	}

	// 验证几何有效性
	if options.ValidateGeometry {
		if C.OGR_G_IsValid(clonedGeom) == 0 {
			// 尝试修复几何
			fixedGeom := C.OGR_G_MakeValid(clonedGeom)
			if fixedGeom != nil && C.OGR_G_IsValid(fixedGeom) == 1 {
				C.OGR_G_DestroyGeometry(clonedGeom)
				clonedGeom = fixedGeom
			} else {
				if fixedGeom != nil {
					C.OGR_G_DestroyGeometry(fixedGeom)
				}
				if options.SkipInvalidGeometry {
					C.OGR_G_DestroyGeometry(clonedGeom)
					result.SkippedCount++
					return false
				}
				// 不跳过无效几何，继续使用
			}
		}
	}

	// 设置几何到目标要素
	C.OGR_F_SetGeometry(targetFeature, clonedGeom)
	C.OGR_G_DestroyGeometry(clonedGeom)

	return true
}

// validateGeometryTypesV2 验证几何类型（V2版本）
func validateGeometryTypesV2(sourceLayer, targetLayer C.OGRLayerH, options *ImportToGDBOptionsV2) error {
	sourceLayerDefn := C.OGR_L_GetLayerDefn(sourceLayer)
	targetLayerDefn := C.OGR_L_GetLayerDefn(targetLayer)

	sourceGeomType := C.OGR_FD_GetGeomType(sourceLayerDefn)
	targetGeomType := C.OGR_FD_GetGeomType(targetLayerDefn)

	sourceBaseType := getBaseGeometryType(sourceGeomType)
	targetBaseType := getBaseGeometryType(targetGeomType)

	fmt.Printf("源图层几何类型: %s, 目标图层几何类型: %s\n",
		getGeometryTypeName(sourceGeomType), getGeometryTypeName(targetGeomType))

	// 如果目标是Unknown类型，接受任何几何
	if targetBaseType == C.wkbUnknown {
		return nil
	}

	// 检查几何类型兼容性
	if !isGeometryTypeCompatible(sourceBaseType, targetBaseType) {
		return fmt.Errorf("几何类型不兼容: 源=%s, 目标=%s",
			getGeometryTypeName(sourceGeomType), getGeometryTypeName(targetGeomType))
	}

	return nil
}

// copyFieldDefinitionsV2 复制字段定义（V2版本）
func copyFieldDefinitionsV2(sourceLayerDefn C.OGRFeatureDefnH, targetLayer C.OGRLayerH, options *ImportToGDBOptionsV2) error {
	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceLayerDefn))

	// 构建排除字段集合
	excludeSet := make(map[string]bool)
	for _, field := range options.ExcludeFields {
		excludeSet[field] = true
	}

	// 构建包含字段集合
	includeSet := make(map[string]bool)
	if len(options.IncludeFields) > 0 {
		for _, field := range options.IncludeFields {
			includeSet[field] = true
		}
	}

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		fieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 检查是否在包含列表中
		if len(includeSet) > 0 && !includeSet[fieldName] {
			continue
		}

		// 检查是否在排除列表中
		if excludeSet[fieldName] {
			continue
		}

		// 确定目标字段名
		targetFieldName := fieldName
		if options.FieldMapping != nil {
			if mappedName, ok := options.FieldMapping[fieldName]; ok {
				targetFieldName = mappedName
			}
		}

		// 创建新的字段定义
		cTargetFieldName := C.CString(targetFieldName)
		fieldType := C.OGR_Fld_GetType(sourceFieldDefn)
		newFieldDefn := C.OGR_Fld_Create(cTargetFieldName, fieldType)
		C.free(unsafe.Pointer(cTargetFieldName))

		if newFieldDefn == nil {
			continue
		}

		// 复制字段属性
		width := C.OGR_Fld_GetWidth(sourceFieldDefn)
		precision := C.OGR_Fld_GetPrecision(sourceFieldDefn)
		nullable := C.OGR_Fld_IsNullable(sourceFieldDefn)

		if width > 0 {
			C.OGR_Fld_SetWidth(newFieldDefn, width)
		}
		if precision > 0 {
			C.OGR_Fld_SetPrecision(newFieldDefn, precision)
		}
		C.OGR_Fld_SetNullable(newFieldDefn, nullable)

		// 添加字段到目标图层
		result := C.OGR_L_CreateField(targetLayer, newFieldDefn, C.int(1))
		C.OGR_Fld_Destroy(newFieldDefn)

		if result != C.OGRERR_NONE {
			fmt.Printf("警告: 创建字段 '%s' 失败\n", targetFieldName)
		}
	}

	return nil
}

// createImportFieldMappingV2 创建导入字段映射（V2版本）
func createImportFieldMappingV2(sourceLayerDefn, targetLayerDefn C.OGRFeatureDefnH, options *ImportToGDBOptionsV2) ([]ImportFieldMapping, error) {
	var mappings []ImportFieldMapping

	sourceFieldCount := int(C.OGR_FD_GetFieldCount(sourceLayerDefn))

	// 构建排除字段集合
	excludeSet := make(map[string]bool)
	for _, field := range options.ExcludeFields {
		excludeSet[field] = true
	}

	// 构建包含字段集合
	includeSet := make(map[string]bool)
	if len(options.IncludeFields) > 0 {
		for _, field := range options.IncludeFields {
			includeSet[field] = true
		}
	}

	for i := 0; i < sourceFieldCount; i++ {
		sourceFieldDefn := C.OGR_FD_GetFieldDefn(sourceLayerDefn, C.int(i))
		if sourceFieldDefn == nil {
			continue
		}

		sourceFieldName := C.GoString(C.OGR_Fld_GetNameRef(sourceFieldDefn))

		// 检查是否在包含列表中
		if len(includeSet) > 0 && !includeSet[sourceFieldName] {
			continue
		}

		// 检查是否在排除列表中
		if excludeSet[sourceFieldName] {
			continue
		}

		// 确定目标字段名
		targetFieldName := sourceFieldName
		if options.FieldMapping != nil {
			if mappedName, ok := options.FieldMapping[sourceFieldName]; ok {
				targetFieldName = mappedName
			}
		}

		// 在目标图层中查找字段
		cTargetFieldName := C.CString(targetFieldName)
		targetIndex := C.OGR_FD_GetFieldIndex(targetLayerDefn, cTargetFieldName)
		C.free(unsafe.Pointer(cTargetFieldName))

		if targetIndex >= 0 {
			sourceFieldType := C.OGR_Fld_GetType(sourceFieldDefn)
			targetFieldDefn := C.OGR_FD_GetFieldDefn(targetLayerDefn, targetIndex)
			targetFieldType := C.OGR_Fld_GetType(targetFieldDefn)

			mappings = append(mappings, ImportFieldMapping{
				SourceIndex: i,
				TargetIndex: int(targetIndex),
				SourceName:  sourceFieldName,
				TargetName:  targetFieldName,
				SourceType:  sourceFieldType,
				TargetType:  targetFieldType,
			})
		}
	}

	return mappings, nil
}

// copyImportFeatureFieldsV2 复制导入要素的字段（V2版本）
func copyImportFeatureFieldsV2(sourceFeature, targetFeature C.OGRFeatureH, mappings []ImportFieldMapping, targetLayerDefn C.OGRFeatureDefnH) error {
	for _, mapping := range mappings {
		// 检查源字段是否已设置
		if C.OGR_F_IsFieldSet(sourceFeature, C.int(mapping.SourceIndex)) == 0 {
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 检查是否为NULL
		if C.OGR_F_IsFieldNull(sourceFeature, C.int(mapping.SourceIndex)) != 0 {
			C.OGR_F_SetFieldNull(targetFeature, C.int(mapping.TargetIndex))
			continue
		}

		// 复制字段值（带类型转换）
		err := copyImportFieldValueV2(sourceFeature, targetFeature,
			C.int(mapping.SourceIndex), C.int(mapping.TargetIndex),
			mapping.SourceType, mapping.TargetType)

		if err != nil {
			return fmt.Errorf("复制字段 '%s' 失败: %v", mapping.SourceName, err)
		}
	}

	return nil
}

// copyImportFieldValueV2 复制单个字段值（V2版本，支持类型转换）
func copyImportFieldValueV2(sourceFeature, targetFeature C.OGRFeatureH,
	sourceIndex, targetIndex C.int,
	sourceType, targetType C.OGRFieldType) error {

	// 根据目标类型设置值
	switch targetType {
	case C.OFTInteger:
		var intVal C.int
		switch sourceType {
		case C.OFTInteger:
			intVal = C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		case C.OFTInteger64:
			intVal = C.int(C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex))
		case C.OFTReal:
			intVal = C.int(C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex))
		default:
			intVal = C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldInteger(targetFeature, targetIndex, intVal)

	case C.OFTInteger64:
		var int64Val C.longlong
		switch sourceType {
		case C.OFTInteger:
			int64Val = C.longlong(C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex))
		case C.OFTInteger64:
			int64Val = C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		case C.OFTReal:
			int64Val = C.longlong(C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex))
		default:
			int64Val = C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldInteger64(targetFeature, targetIndex, int64Val)

	case C.OFTReal:
		var doubleVal C.double
		switch sourceType {
		case C.OFTInteger:
			doubleVal = C.double(C.OGR_F_GetFieldAsInteger(sourceFeature, sourceIndex))
		case C.OFTInteger64:
			doubleVal = C.double(C.OGR_F_GetFieldAsInteger64(sourceFeature, sourceIndex))
		case C.OFTReal:
			doubleVal = C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		default:
			doubleVal = C.OGR_F_GetFieldAsDouble(sourceFeature, sourceIndex)
		}
		C.OGR_F_SetFieldDouble(targetFeature, targetIndex, doubleVal)

	case C.OFTString:
		strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)

	case C.OFTDate, C.OFTTime, C.OFTDateTime:
		if sourceType == C.OFTDate || sourceType == C.OFTTime || sourceType == C.OFTDateTime {
			var year, month, day, hour, minute, second, tzflag C.int
			C.OGR_F_GetFieldAsDateTime(sourceFeature, sourceIndex,
				&year, &month, &day, &hour, &minute, &second, &tzflag)
			C.OGR_F_SetFieldDateTime(targetFeature, targetIndex,
				year, month, day, hour, minute, second, tzflag)
		} else {
			strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
			C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)
		}

	default:
		// 默认转为字符串
		strVal := C.OGR_F_GetFieldAsString(sourceFeature, sourceIndex)
		C.OGR_F_SetFieldString(targetFeature, targetIndex, strVal)
	}

	return nil
}

// printErrorSummary 打印错误摘要
func printErrorSummary(result *ImportResult) {
	if len(result.Errors) > 0 && len(result.Errors) <= 10 {
		fmt.Printf("错误详情:\n")
		for _, err := range result.Errors {
			fmt.Printf("  - %s\n", err)
		}
	} else if len(result.Errors) > 10 {
		fmt.Printf("发生 %d 个错误（仅显示前10个）:\n", len(result.Errors))
		for i := 0; i < 10; i++ {
			fmt.Printf("  - %s\n", result.Errors[i])
		}
	}
}
