/*
 * @File : lock
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 互斥锁机制，支持调试模式和k8s模式
 */

package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cksr/logger"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// LockManager 锁管理器接口
type LockManager interface {
	// AcquireLock 获取锁，返回释放锁的函数
	AcquireLock(ctx context.Context) (func(), error)
	// IsLocked 检查是否已被锁定
	IsLocked(ctx context.Context) (bool, error)
}

// DummyLockManager 调试模式的虚拟锁管理器
type DummyLockManager struct {
	mu     sync.Mutex
	locked bool
}

// NewDummyLockManager 创建虚拟锁管理器
func NewDummyLockManager() *DummyLockManager {
	return &DummyLockManager{}
}

func (d *DummyLockManager) AcquireLock(ctx context.Context) (func(), error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.locked {
		return nil, fmt.Errorf("锁已被占用（调试模式）")
	}

	d.locked = true
	logger.Debug("获取虚拟锁成功（调试模式）")

	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.locked = false
		logger.Debug("释放虚拟锁成功（调试模式）")
	}, nil
}

func (d *DummyLockManager) IsLocked(ctx context.Context) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.locked, nil
}

// K8sLeaseLockManager k8s lease锁管理器
type K8sLeaseLockManager struct {
	client    kubernetes.Interface
	namespace string
	leaseName string
	identity  string
	duration  time.Duration
}

// NewK8sLeaseLockManager 创建k8s lease锁管理器
func NewK8sLeaseLockManager(namespace, leaseName, identity string, duration time.Duration) (*K8sLeaseLockManager, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("获取k8s集群配置失败: %w", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("创建k8s客户端失败: %w", err)
	}

	return &K8sLeaseLockManager{
		client:    client,
		namespace: namespace,
		leaseName: leaseName,
		identity:  identity,
		duration:  duration,
	}, nil
}

func (k *K8sLeaseLockManager) AcquireLock(ctx context.Context) (func(), error) {
	leaseClient := k.client.CoordinationV1().Leases(k.namespace)

	// 尝试获取现有的lease
	lease, err := leaseClient.Get(ctx, k.leaseName, metav1.GetOptions{})
	if err != nil {
		// 如果lease不存在，创建新的lease
		now := metav1.NewMicroTime(time.Now())
		durationSeconds := int32(k.duration.Seconds())

		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      k.leaseName,
				Namespace: k.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &k.identity,
				LeaseDurationSeconds: &durationSeconds,
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}

		_, err = leaseClient.Create(ctx, newLease, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建lease失败: %w", err)
		}

		logger.Info("成功获取k8s lease锁: %s", k.leaseName)
	} else {
		// 检查lease是否已过期或者是否为当前实例持有
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != k.identity {
			if lease.Spec.RenewTime != nil {
				renewTime := lease.Spec.RenewTime.Time
				if time.Since(renewTime) < k.duration {
					return nil, fmt.Errorf("lease被其他实例持有: %s", *lease.Spec.HolderIdentity)
				}
			}
		}

		// 更新lease
		now := metav1.NewMicroTime(time.Now())
		lease.Spec.HolderIdentity = &k.identity
		lease.Spec.RenewTime = &now

		_, err = leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("更新lease失败: %w", err)
		}

		logger.Info("成功更新k8s lease锁: %s", k.leaseName)
	}

	// 启动续约协程
	renewCtx, cancel := context.WithCancel(ctx)
	go k.renewLease(renewCtx)

	// 返回释放锁的函数
	return func() {
		cancel() // 停止续约

		// 删除lease
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer deleteCancel()

		err := leaseClient.Delete(deleteCtx, k.leaseName, metav1.DeleteOptions{})
		if err != nil {
			logger.Error("删除lease失败: %v", err)
		} else {
			logger.Info("成功释放k8s lease锁: %s", k.leaseName)
		}
	}, nil
}

func (k *K8sLeaseLockManager) IsLocked(ctx context.Context) (bool, error) {
	leaseClient := k.client.CoordinationV1().Leases(k.namespace)

	lease, err := leaseClient.Get(ctx, k.leaseName, metav1.GetOptions{})
	if err != nil {
		return false, nil // lease不存在，表示未锁定
	}

	// 检查lease是否有效
	if lease.Spec.HolderIdentity == nil || lease.Spec.RenewTime == nil {
		return false, nil
	}

	renewTime := lease.Spec.RenewTime.Time
	return time.Since(renewTime) < k.duration, nil
}

// renewLease 续约lease
func (k *K8sLeaseLockManager) renewLease(ctx context.Context) {
	ticker := time.NewTicker(k.duration / 3) // 每1/3租期续约一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			leaseClient := k.client.CoordinationV1().Leases(k.namespace)

			lease, err := leaseClient.Get(ctx, k.leaseName, metav1.GetOptions{})
			if err != nil {
				logger.Error("续约时获取lease失败: %v", err)
				continue
			}

			now := metav1.NewMicroTime(time.Now())
			lease.Spec.RenewTime = &now

			_, err = leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
			if err != nil {
				logger.Error("续约lease失败: %v", err)
			} else {
				logger.Debug("成功续约lease: %s", k.leaseName)
			}
		}
	}
}

// CreateLockManager 根据配置创建锁管理器
func CreateLockManager(debugMode bool, namespace, leaseName, identity string, duration time.Duration) (LockManager, error) {
	if debugMode {
		logger.Info("使用调试模式锁管理器")
		return NewDummyLockManager(), nil
	}

	logger.Info("使用k8s lease锁管理器")
	return NewK8sLeaseLockManager(namespace, leaseName, identity, duration)
}
