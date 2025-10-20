/*
 * @File : fileops
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 文件操作功能
 */

package fileops

import (
	"fmt"
	"os"
	"path/filepath"
)

// FileManager 文件管理器
type FileManager struct {
	tempDir string
}

// NewFileManager 创建文件管理器
func NewFileManager(tempDir string) *FileManager {
	return &FileManager{tempDir: tempDir}
}

// EnsureTempDir 确保临时目录存在
func (fm *FileManager) EnsureTempDir() error {
	if err := os.MkdirAll(fm.tempDir, 0755); err != nil {
		return fmt.Errorf("创建临时目录失败: %w", err)
	}
	return nil
}

// GetClickHouseSchemaPath 获取ClickHouse表结构文件路径
func (fm *FileManager) GetClickHouseSchemaPath(dbName, tableName string) string {
	return filepath.Join(fm.tempDir, "clickhouse", dbName, fmt.Sprintf("%s.sql", tableName))
}

// GetStarRocksSchemaPath 获取StarRocks表结构文件路径
func (fm *FileManager) GetStarRocksSchemaPath(dbName, tableName string) string {
	return filepath.Join(fm.tempDir, "starrocks", dbName, fmt.Sprintf("%s.sql", tableName))
}

// CleanTempDir 清理临时目录
func (fm *FileManager) CleanTempDir() error {
	if err := os.RemoveAll(fm.tempDir); err != nil {
		return fmt.Errorf("清理临时目录失败: %w", err)
	}
	return nil
}

// ListCommonTables 列出ClickHouse和StarRocks中共同存在的表
func (fm *FileManager) ListCommonTables(ckSchemas, srSchemas map[string]string) []string {
	var commonTables []string
	for tableName := range ckSchemas {
		if _, exists := srSchemas[tableName]; exists {
			commonTables = append(commonTables, tableName)
		}
	}
	return commonTables
}