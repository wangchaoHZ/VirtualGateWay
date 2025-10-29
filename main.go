package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/goburrow/modbus"
)

const (
	upstreamIP       = "192.168.1.122"
	upstreamPort     = 502
	upstreamSlaveID  = 17
	startAddress     = 99 // 缓存起始地址
	registerCount    = 60 // 缓存寄存器数量
	pollInterval     = 2 * time.Second
	connectTimeout   = 5 * time.Second
	handlerIdleTout  = 30 * time.Second
	retryDelayOnFail = 2 * time.Second

	localListenAddr = ":1502"
	maxConnections  = 10
	unitID          = 1
	logPrefix       = "[ModbusProxy] "
)

// DataCache 保存从上游采集的 uint16 保持寄存器数据
type DataCache struct {
	mu        sync.RWMutex
	registers []uint16
	timestamp time.Time
	lastErr   error
}

func NewDataCache(size int) *DataCache {
	return &DataCache{
		registers: make([]uint16, size),
	}
}

func (c *DataCache) Update(values []uint16, ts time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		if values != nil && len(values) == len(c.registers) {
			copy(c.registers, values)
		}
		c.timestamp = ts
		c.lastErr = nil
	} else {
		c.lastErr = err
	}
}

func (c *DataCache) ReadSlice(offset, qty int) ([]uint16, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if offset < 0 || qty < 0 || offset+qty > len(c.registers) {
		return nil, errors.New("out of range")
	}
	out := make([]uint16, qty)
	copy(out, c.registers[offset:offset+qty])
	return out, nil
}

func (c *DataCache) SnapshotMeta() (time.Time, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp, c.lastErr
}

// 上游采集协程
func startPollingUpstream(ctx context.Context, cache *DataCache, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			handler := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%d", upstreamIP, upstreamPort))
			handler.Timeout = connectTimeout
			handler.SlaveId = byte(upstreamSlaveID)
			handler.IdleTimeout = handlerIdleTout

			if err := handler.Connect(); err != nil {
				log.Printf(logPrefix+"连接上游失败: %v，%v 后重试", err, retryDelayOnFail)
				cache.Update(nil, time.Now(), err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryDelayOnFail):
				}
				continue
			}

			client := modbus.NewClient(handler)
			ticker := time.NewTicker(pollInterval)

		POLL_LOOP:
			for {
				select {
				case <-ctx.Done():
					ticker.Stop()
					_ = handler.Close()
					return
				case <-ticker.C:
					ts := time.Now()
					raw, err := client.ReadHoldingRegisters(uint16(startAddress), uint16(registerCount))
					if err != nil {
						log.Printf(logPrefix+"读取失败: %v", err)
						cache.Update(nil, time.Now(), err)
						ticker.Stop()
						_ = handler.Close()
						break POLL_LOOP
					}
					if len(raw) != registerCount*2 {
						err = fmt.Errorf("返回长度异常，期望 %d 实际 %d", registerCount*2, len(raw))
						cache.Update(nil, time.Now(), err)
						log.Println(logPrefix + err.Error())
						ticker.Stop()
						_ = handler.Close()
						break POLL_LOOP
					}
					regs := make([]uint16, registerCount)
					for i := 0; i < registerCount; i++ {
						regs[i] = binary.BigEndian.Uint16(raw[i*2 : i*2+2])
					}
					cache.Update(regs, ts, nil)
					log.Printf(logPrefix+"采集成功：%d 寄存器 (%d~%d) 时间=%s",
						registerCount, startAddress, startAddress+registerCount-1, ts.Format(time.RFC3339))
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelayOnFail):
			}
		}
	}()
}

// 启动自定义 Modbus TCP 从站（只支持 FC=0x03）
func startLocalSlave(ctx context.Context, cache *DataCache, wg *sync.WaitGroup) error {
	l, err := net.Listen("tcp", localListenAddr)
	if err != nil {
		return fmt.Errorf("监听失败: %w", err)
	}
	log.Println(logPrefix + "本地从站已启动，地址 " + localListenAddr)

	sem := make(chan struct{}, maxConnections)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					log.Printf(logPrefix+"Accept 错误: %v", err)
					continue
				}
			}
			select {
			case sem <- struct{}{}:
				log.Printf(logPrefix+"新客户端 %s 已接入 (当前: %d)", conn.RemoteAddr(), len(sem))
				wg.Add(1)
				go handleConnection(ctx, wg, conn, cache, sem)
			default:
				log.Printf(logPrefix+"拒绝客户端 %s：超过最大并发 %d", conn.RemoteAddr(), maxConnections)
				_ = conn.Close()
			}
		}
	}()
	return nil
}

func handleConnection(ctx context.Context, wg *sync.WaitGroup, conn net.Conn, cache *DataCache, sem chan struct{}) {
	defer wg.Done()
	defer func() {
		_ = conn.Close()
		<-sem
		log.Printf(logPrefix+"客户端 %s 断开 (当前: %d)", conn.RemoteAddr(), len(sem))
	}()

	setDeadline := func() {
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		setDeadline()
		header := make([]byte, 7)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf(logPrefix+"读取头失败 %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		transactionID := binary.BigEndian.Uint16(header[0:2])
		protocolID := binary.BigEndian.Uint16(header[2:4])
		lengthField := binary.BigEndian.Uint16(header[4:6])
		reqUnitID := header[6]

		if protocolID != 0 {
			sendException(conn, transactionID, reqUnitID, 0x03, 0x01)
			continue
		}
		if reqUnitID != unitID {
			continue
		}
		if lengthField < 2 {
			sendException(conn, transactionID, reqUnitID, 0x03, 0x03)
			continue
		}
		pduLen := int(lengthField - 1)
		pdu := make([]byte, pduLen)
		_, err = io.ReadFull(conn, pdu)
		if err != nil {
			log.Printf(logPrefix+"读取 PDU 失败 %s: %v", conn.RemoteAddr(), err)
			return
		}
		if len(pdu) < 5 {
			sendException(conn, transactionID, reqUnitID, 0x03, 0x03)
			continue
		}
		functionCode := pdu[0]
		switch functionCode {
		case 0x03:
			startAddr := int(binary.BigEndian.Uint16(pdu[1:3]))
			quantity := int(binary.BigEndian.Uint16(pdu[3:5]))
			if quantity <= 0 || quantity > 125 {
				sendException(conn, transactionID, reqUnitID, functionCode, 0x03)
				continue
			}
			if startAddr < startAddress || (startAddr+quantity) > (startAddress+registerCount) {
				sendException(conn, transactionID, reqUnitID, functionCode, 0x02)
				continue
			}
			offset := startAddr - startAddress
			values, err := cache.ReadSlice(offset, quantity)
			if err != nil {
				sendException(conn, transactionID, reqUnitID, functionCode, 0x02)
				continue
			}
			byteCount := quantity * 2
			respPDU := make([]byte, 2+byteCount)
			respPDU[0] = functionCode
			respPDU[1] = byte(byteCount)
			for i, v := range values {
				binary.BigEndian.PutUint16(respPDU[2+i*2:2+i*2+2], v)
			}
			respMBAP := make([]byte, 7)
			binary.BigEndian.PutUint16(respMBAP[0:2], transactionID)
			binary.BigEndian.PutUint16(respMBAP[2:4], 0)
			binary.BigEndian.PutUint16(respMBAP[4:6], uint16(len(respPDU)+1))
			respMBAP[6] = reqUnitID
			packet := append(respMBAP, respPDU...)
			setDeadline()
			_, err = conn.Write(packet)
			if err != nil {
				log.Printf(logPrefix+"写响应失败 %s: %v", conn.RemoteAddr(), err)
				return
			}
		default:
			sendException(conn, transactionID, reqUnitID, functionCode, 0x01)
		}
	}
}

func sendException(conn net.Conn, transactionID uint16, unitID byte, functionCode byte, exceptionCode byte) {
	respPDU := []byte{functionCode | 0x80, exceptionCode}
	respMBAP := make([]byte, 7)
	binary.BigEndian.PutUint16(respMBAP[0:2], transactionID)
	binary.BigEndian.PutUint16(respMBAP[2:4], 0)
	binary.BigEndian.PutUint16(respMBAP[4:6], uint16(len(respPDU)+1))
	respMBAP[6] = unitID
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write(append(respMBAP, respPDU...))
}

func startDebugPrinter(ctx context.Context, cache *DataCache, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ts, err := cache.SnapshotMeta()
				if err != nil {
					log.Printf(logPrefix+"缓存状态：上游错误=%v", err)
					continue
				}
				if ts.IsZero() {
					log.Println(logPrefix + "缓存尚未填充")
					continue
				}
				values, _ := cache.ReadSlice(0, min(6, registerCount))
				log.Printf(logPrefix+"缓存快照时间=%s 前6=%v", ts.Format(time.RFC3339), values)
			}
		}
	}()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cache := NewDataCache(registerCount)

	// 后台常驻，无需信号退出
	ctx := context.Background()
	var wg sync.WaitGroup

	startPollingUpstream(ctx, cache, &wg)
	if err := startLocalSlave(ctx, cache, &wg); err != nil {
		log.Fatalf(logPrefix+"启动本地从站失败: %v", err)
	}
	startDebugPrinter(ctx, cache, &wg)

	log.Printf(logPrefix+"后台代理运行中，UnitID=%d，本地监听 %s，地址范围 [%d..%d]，最大并发 %d。",
		unitID, localListenAddr, startAddress, startAddress+registerCount-1, maxConnections)

	// 阻塞主协程：持续运行（也可以使用 select{}）
	select {}
}
