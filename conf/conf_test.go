package conf

import (
	"sync/atomic"
	"testing"
)

func TestStopRegistryRegisterAndStop(t *testing.T) {
	r := NewStopRegistry()
	ch, unregister := r.Register("8080tcp")
	defer unregister()

	// 未投递前 Lookup 返回同一通道
	if r.Lookup("8080tcp") == nil {
		t.Fatal("Lookup 应返回已注册通道")
	}

	// 投递停止信号
	if !r.Stop("8080tcp") {
		t.Fatal("Stop 应命中已注册项")
	}
	select {
	case <-ch:
		// 成功收到
	default:
		t.Fatal("应能从通道读到停止信号")
	}

	// 不存在的 key
	if r.Stop("9999udp") {
		t.Fatal("未注册的 key Stop 应返回 false")
	}
}

func TestStopRegistryStopAll(t *testing.T) {
	r := NewStopRegistry()
	ch1, un1 := r.Register("1tcp")
	ch2, un2 := r.Register("2tcp")
	defer un1()
	defer un2()

	r.StopAll()

	var got atomic.Int64
	done := make(chan struct{})
	go func() { <-ch1; got.Add(1); done <- struct{}{} }()
	go func() { <-ch2; got.Add(1); done <- struct{}{} }()

	for i := 0; i < 2; i++ {
		<-done
	}
	if got.Load() != 2 {
		t.Fatalf("StopAll 应通知所有 2 条，实际 %d", got.Load())
	}
}

func TestStopRegistryUnregister(t *testing.T) {
	r := NewStopRegistry()
	_, unregister := r.Register("8080tcp")
	unregister()
	if r.Lookup("8080tcp") != nil {
		t.Fatal("反注册后 Lookup 应为 nil")
	}
}
