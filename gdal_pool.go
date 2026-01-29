// gdal_pool.go
package Gogeo

import (
	"runtime"
	"sync"
)

/*
#include "osgeo_utils.h"
*/
import "C"

// GDALWorkerPool GDAL工作池 - 控制并发数量
type GDALWorkerPool struct {
	semaphore chan struct{}
	size      int
}

var (
	gdalPool     *GDALWorkerPool
	gdalPoolOnce sync.Once
)

// GetGDALPool 获取全局GDAL工作池（单例）
func GetGDALPool() *GDALWorkerPool {
	gdalPoolOnce.Do(func() {
		// 根据CPU核心数设置工作池大小，GDAL操作密集型建议 CPU核心数 * 2
		poolSize := runtime.NumCPU() * 2
		if poolSize < 4 {
			poolSize = 4
		}
		if poolSize > 16 {
			poolSize = 16 // 上限，避免GDAL资源竞争
		}

		gdalPool = &GDALWorkerPool{
			semaphore: make(chan struct{}, poolSize),
			size:      poolSize,
		}
	})
	return gdalPool
}

// Acquire 获取工作槽
func (p *GDALWorkerPool) Acquire() {
	p.semaphore <- struct{}{}
}

// Release 释放工作槽
func (p *GDALWorkerPool) Release() {
	<-p.semaphore
}

// Execute 在工作池中执行GDAL操作
func (p *GDALWorkerPool) Execute(fn func() ([]byte, error)) ([]byte, error) {
	p.Acquire()
	defer p.Release()
	return fn()
}
