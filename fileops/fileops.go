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

// WriteClickHouseSchemas 将ClickHouse表结构写入文件
func (fm *FileManager) WriteClickHouseSchemas(schemas map[string]string, dbName string) error {
	if err := fm.EnsureTempDir(); err != nil {
		return err
	}

	// 创建ClickHouse子目录
	ckDir := filepath.Join(fm.tempDir, "clickhouse", dbName)
	if err := os.MkdirAll(ckDir, 0755); err != nil {
		return fmt.Errorf("创建ClickHouse目录失败: %w", err)
	}

	// 写入每个表的DDL
	for tableName, ddl := range schemas {
		filePath := filepath.Join(ckDir, fmt.Sprintf("%s.sql", tableName))
		if err := os.WriteFile(filePath, []byte(ddl), 0644); err != nil {
			return fmt.Errorf("写入ClickHouse表%s的DDL失败: %w", tableName, err)
		}
	}

	return nil
}

// WriteStarRocksSchemas 将StarRocks表结构写入文件
func (fm *FileManager) WriteStarRocksSchemas(schemas map[string]string, dbName string) error {
	if err := fm.EnsureTempDir(); err != nil {
		return err
	}

	// 创建StarRocks子目录
	srDir := filepath.Join(fm.tempDir, "starrocks", dbName)
	if err := os.MkdirAll(srDir, 0755); err != nil {
		return fmt.Errorf("创建StarRocks目录失败: %w", err)
	}

	// 写入每个表的DDL
	for tableName, ddl := range schemas {
		filePath := filepath.Join(srDir, fmt.Sprintf("%s.sql", tableName))
		if err := os.WriteFile(filePath, []byte(ddl), 0644); err != nil {
			return fmt.Errorf("写入StarRocks表%s的DDL失败: %w", tableName, err)
		}
	}

	return nil
}

// WriteGeneratedSQL 写入生成的ALTER TABLE和CREATE VIEW语句
func (fm *FileManager) WriteGeneratedSQL(alterSQL, viewSQL, tableName string) error {
	if err := fm.EnsureTempDir(); err != nil {
		return err
	}

	// 创建生成的SQL目录
	genDir := filepath.Join(fm.tempDir, "generated")
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return fmt.Errorf("创建生成SQL目录失败: %w", err)
	}

	// 写入ALTER TABLE语句
	if alterSQL != "" {
		alterPath := filepath.Join(genDir, fmt.Sprintf("%s_alter.sql", tableName))
		if err := os.WriteFile(alterPath, []byte(alterSQL), 0644); err != nil {
			return fmt.Errorf("写入ALTER TABLE语句失败: %w", err)
		}
	}

	// 写入CREATE VIEW语句
	if viewSQL != "" {
		viewPath := filepath.Join(genDir, fmt.Sprintf("%s_view.sql", tableName))
		if err := os.WriteFile(viewPath, []byte(viewSQL), 0644); err != nil {
			return fmt.Errorf("写入CREATE VIEW语句失败: %w", err)
		}
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