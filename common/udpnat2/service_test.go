package udpnat

import (
	"context"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestServiceEndpointIndependentKey(t *testing.T) {
	t.Parallel()

	handler := &testUDPConnectionHandler{connections: make(chan M.Socksaddr, 2)}
	service := New(handler, testPrepareFunc, time.Minute, false)
	defer service.Purge()

	source := M.ParseSocksaddr("192.0.2.1:12345")
	service.NewPacket([][]byte{[]byte("a")}, source, M.ParseSocksaddr("198.51.100.1:53"), nil)
	service.NewPacket([][]byte{[]byte("b")}, source, M.ParseSocksaddr("198.51.100.2:443"), nil)

	requireConnectionCount(t, handler.connections, 1)
}

func TestServiceDestinationDependentKey(t *testing.T) {
	t.Parallel()

	handler := &testUDPConnectionHandler{connections: make(chan M.Socksaddr, 2)}
	service := NewWithMode(handler, testPrepareFunc, time.Minute, false, NATModeDestinationDependent)
	defer service.Purge()

	source := M.ParseSocksaddr("192.0.2.1:12345")
	service.NewPacket([][]byte{[]byte("a")}, source, M.ParseSocksaddr("198.51.100.1:53"), nil)
	service.NewPacket([][]byte{[]byte("b")}, source, M.ParseSocksaddr("198.51.100.2:443"), nil)

	requireConnectionCount(t, handler.connections, 2)
}

func testPrepareFunc(source M.Socksaddr, destination M.Socksaddr, userData any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc) {
	return true, context.Background(), testPacketWriter{}, nil
}

type testUDPConnectionHandler struct {
	connections chan M.Socksaddr
}

func (h *testUDPConnectionHandler) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	h.connections <- destination
}

func requireConnectionCount(t *testing.T, connections <-chan M.Socksaddr, expected int) {
	t.Helper()

	deadline := time.After(time.Second)
	for count := 0; count < expected; count++ {
		select {
		case <-connections:
		case <-deadline:
			t.Fatalf("timed out waiting for connection %d of %d", count+1, expected)
		}
	}
	select {
	case destination := <-connections:
		t.Fatalf("unexpected extra connection to %v", destination)
	case <-time.After(50 * time.Millisecond):
	}
}
