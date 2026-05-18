package udpnat

import (
	"context"
	"net/netip"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/pipe"
	"github.com/sagernet/sing/contrab/freelru"
	"github.com/sagernet/sing/contrab/maphash"
)

type Service struct {
	cache   freelru.Cache[sessionKey, *natConn]
	handler N.UDPConnectionHandlerEx
	prepare PrepareFunc
	mode    NATMode
}

type PrepareFunc func(source M.Socksaddr, destination M.Socksaddr, userData any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc)

type NATMode uint8

const (
	NATModeEndpointIndependent NATMode = iota
	NATModeDestinationDependent
)

type sessionKey struct {
	Source      netip.AddrPort
	Destination netip.AddrPort
}

func New(handler N.UDPConnectionHandlerEx, prepare PrepareFunc, timeout time.Duration, shared bool) *Service {
	return NewWithMode(handler, prepare, timeout, shared, NATModeEndpointIndependent)
}

func NewWithMode(handler N.UDPConnectionHandlerEx, prepare PrepareFunc, timeout time.Duration, shared bool, mode NATMode) *Service {
	if timeout == 0 {
		panic("invalid timeout")
	}
	var cache freelru.Cache[sessionKey, *natConn]
	if !shared {
		cache = common.Must1(freelru.NewSynced[sessionKey, *natConn](1024, maphash.NewHasher[sessionKey]().Hash32))
	} else {
		cache = common.Must1(freelru.NewSharded[sessionKey, *natConn](1024, maphash.NewHasher[sessionKey]().Hash32))
	}
	cache.SetLifetime(timeout)
	cache.SetHealthCheck(func(key sessionKey, conn *natConn) bool {
		select {
		case <-conn.doneChan:
			return false
		default:
			return true
		}
	})
	cache.SetOnEvict(func(_ sessionKey, conn *natConn) {
		conn.Close()
	})
	return &Service{
		cache:   cache,
		handler: handler,
		prepare: prepare,
		mode:    mode,
	}
}

func (s *Service) NewPacket(bufferSlices [][]byte, source M.Socksaddr, destination M.Socksaddr, userData any) {
	key := s.sessionKey(source, destination)
	conn, _, ok := s.cache.GetAndRefreshOrAdd(key, func() (*natConn, bool) {
		ok, ctx, writer, onClose := s.prepare(source, destination, userData)
		if !ok {
			return nil, false
		}
		newConn := &natConn{
			cache:        s.cache,
			key:          key,
			writer:       writer,
			localAddr:    source,
			packetChan:   make(chan *N.PacketBuffer, 64),
			doneChan:     make(chan struct{}),
			readDeadline: pipe.MakeDeadline(),
		}
		go s.handler.NewPacketConnectionEx(ctx, newConn, source, destination, onClose)
		return newConn, true
	})
	if !ok {
		return
	}
	conn.handlerAccess.RLock()
	readWaitOptions := conn.readWaitOptions
	handler := conn.handler
	conn.handlerAccess.RUnlock()
	var dataLen int
	for _, bufferSlice := range bufferSlices {
		dataLen += len(bufferSlice)
	}
	buffer := readWaitOptions.NewBufferSize(dataLen)
	for _, bufferSlice := range bufferSlices {
		buffer.Write(bufferSlice)
	}
	readWaitOptions.PostReturn(buffer)
	if handler != nil {
		handler.NewPacketEx(buffer, destination)
		return
	}
	packet := N.NewPacketBuffer()
	*packet = N.PacketBuffer{
		Buffer:      buffer,
		Destination: destination,
	}
	select {
	case conn.packetChan <- packet:
	default:
		packet.Buffer.Release()
		N.PutPacketBuffer(packet)
	}
}

func (s *Service) sessionKey(source M.Socksaddr, destination M.Socksaddr) sessionKey {
	key := sessionKey{Source: source.AddrPort()}
	if s.mode == NATModeDestinationDependent {
		key.Destination = destination.AddrPort()
	}
	return key
}

func (s *Service) NewPacketBatch(buffers []*buf.Buffer, sources []M.Socksaddr, destination M.Socksaddr, userData any) {
	if len(buffers) != len(sources) {
		buf.ReleaseMulti(buffers)
		return
	}
	for index, buffer := range buffers {
		s.NewPacket([][]byte{buffer.Bytes()}, sources[index], destination, userData)
		buffer.Release()
	}
}

func (s *Service) Purge() {
	s.cache.Purge()
}

func (s *Service) PurgeExpired() {
	s.cache.PurgeExpired()
}
