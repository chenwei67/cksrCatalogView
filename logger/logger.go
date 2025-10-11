package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LogLevel 定义日志级别
type LogLevel int

const (
	// SILENT 静默模式，不输出任何日志
	SILENT LogLevel = iota
	// ERROR 只输出错误信息
	ERROR
	// WARN 输出警告和错误信息
	WARN
	// INFO 输出基本信息、警告和错误信息
	INFO
	// DEBUG 输出所有调试信息
	DEBUG
)

// 全局日志级别
var currentLogLevel LogLevel = INFO

// 全局日志输出目标
var logOutput io.Writer = os.Stdout
var errorOutput io.Writer = os.Stderr

// 日志文件句柄
var logFile *os.File

// SetLogLevel 设置日志级别
func SetLogLevel(level LogLevel) {
	currentLogLevel = level
}

// InitFileLogging 初始化文件日志
func InitFileLogging(enableFileLog bool, logFilePath string, tempDir string) error {
	if !enableFileLog {
		// 如果不启用文件日志，使用默认的标准输出
		logOutput = os.Stdout
		errorOutput = os.Stderr
		return nil
	}

	// 如果没有指定日志文件路径，使用临时目录
	if logFilePath == "" {
		// 创建临时目录下的logs子目录
		logsDir := filepath.Join(tempDir, "logs")
		if err := os.MkdirAll(logsDir, 0755); err != nil {
			return fmt.Errorf("创建日志目录失败: %w", err)
		}
		
		// 生成带时间戳的日志文件名
		timestamp := time.Now().Format("20060102_150405")
		logFilePath = filepath.Join(logsDir, fmt.Sprintf("cksr_%s.log", timestamp))
	} else {
		// 确保日志文件的目录存在
		logDir := filepath.Dir(logFilePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("创建日志文件目录失败: %w", err)
		}
	}

	// 打开日志文件
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("打开日志文件失败: %w", err)
	}

	// 关闭之前的日志文件（如果存在）
	if logFile != nil {
		logFile.Close()
	}

	logFile = file
	logOutput = file
	errorOutput = file

	fmt.Printf("日志将写入文件: %s\n", logFilePath)
	return nil
}

// CloseLogFile 关闭日志文件
func CloseLogFile() {
	if logFile != nil {
		logFile.Close()
		logFile = nil
	}
}

// ParseLogLevel 从字符串解析日志级别
func ParseLogLevel(levelStr string) LogLevel {
	switch strings.ToUpper(levelStr) {
	case "SILENT":
		return SILENT
	case "ERROR":
		return ERROR
	case "WARN", "WARNING":
		return WARN
	case "INFO":
		return INFO
	case "DEBUG":
		return DEBUG
	default:
		return INFO // 默认级别
	}
}

// LogLevelString 返回日志级别的字符串表示
func LogLevelString(level LogLevel) string {
	switch level {
	case SILENT:
		return "SILENT"
	case ERROR:
		return "ERROR"
	case WARN:
		return "WARN"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	default:
		return "INFO"
	}
}

// Error 输出错误日志
func Error(format string, args ...interface{}) {
	if currentLogLevel >= ERROR {
		fmt.Fprintf(errorOutput, "ERROR: "+format+"\n", args...)
	}
}

// Warn 输出警告日志
func Warn(format string, args ...interface{}) {
	if currentLogLevel >= WARN {
		fmt.Fprintf(logOutput, "警告: "+format+"\n", args...)
	}
}

// Info 输出信息日志
func Info(format string, args ...interface{}) {
	if currentLogLevel >= INFO {
		fmt.Fprintf(logOutput, format+"\n", args...)
	}
}

// Debug 输出调试日志
func Debug(format string, args ...interface{}) {
	if currentLogLevel >= DEBUG {
		fmt.Fprintf(logOutput, "DEBUG: "+format+"\n", args...)
	}
}

// GetCurrentLevel 获取当前日志级别
func GetCurrentLevel() LogLevel {
	return currentLogLevel
}