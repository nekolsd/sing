package network

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func startServer(t *testing.T) (uint16, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	return port, func() { ln.Close() }
}

// simulatedDialer wraps a real net.Dialer but adds artificial delay per
// destination address before actually dialing. It also maps all dials to
// a real local server port so the TCP handshake succeeds.
type simulatedDialer struct {
	mu         sync.Mutex
	delays     map[netip.Addr]time.Duration
	realPort   uint16
	dialCounts map[netip.Addr]int
}

func (d *simulatedDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	delay := d.delays[destination.Addr]

	d.mu.Lock()
	d.dialCounts[destination.Addr]++
	d.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	nd := net.Dialer{}
	realDest := fmt.Sprintf("127.0.0.1:%d", d.realPort)
	return nd.DialContext(ctx, network, realDest)
}

func (d *simulatedDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "")
}

func TestConcurrentVsSerialVsParallel(t *testing.T) {
	port, cleanup := startServer(t)
	defer cleanup()

	// Three "addresses" with different simulated latencies.
	// In real life these would be different IPs from DNS resolution of the same domain.
	addrFast := netip.MustParseAddr("198.18.0.1")    // 10ms
	addrMedium := netip.MustParseAddr("198.18.0.2")   // 200ms
	addrSlowest := netip.MustParseAddr("198.18.0.3")  // 500ms

	addresses := []netip.Addr{addrSlowest, addrMedium, addrFast}
	destination := M.Socksaddr{Fqdn: "example.com", Port: port}

	const rounds = 5
	t.Logf("Simulated delays: %v=500ms, %v=200ms, %v=10ms", addrSlowest, addrMedium, addrFast)
	t.Logf("Address order given to dialer (worst first): %v", addresses)
	t.Logf("All dials go to a real local TCP server at 127.0.0.1:%d", port)
	t.Logf("Running %d rounds per strategy\n", rounds)

	// --- Serial ---
	serialTimes := make([]time.Duration, rounds)
	for i := 0; i < rounds; i++ {
		d := &simulatedDialer{
			delays:     map[netip.Addr]time.Duration{addrFast: 10 * time.Millisecond, addrMedium: 200 * time.Millisecond, addrSlowest: 500 * time.Millisecond},
			realPort:   port,
			dialCounts: make(map[netip.Addr]int),
		}
		start := time.Now()
		conn, err := DialSerial(context.Background(), d, "tcp", destination, addresses)
		serialTimes[i] = time.Since(start)
		if err != nil {
			t.Fatalf("serial round %d: %v", i, err)
		}
		conn.Close()
	}

	// --- Concurrent ---
	concurrentTimes := make([]time.Duration, rounds)
	for i := 0; i < rounds; i++ {
		d := &simulatedDialer{
			delays:     map[netip.Addr]time.Duration{addrFast: 10 * time.Millisecond, addrMedium: 200 * time.Millisecond, addrSlowest: 500 * time.Millisecond},
			realPort:   port,
			dialCounts: make(map[netip.Addr]int),
		}
		start := time.Now()
		conn, err := DialTCPConcurrent(context.Background(), d, destination, addresses)
		concurrentTimes[i] = time.Since(start)
		if err != nil {
			t.Fatalf("concurrent round %d: %v", i, err)
		}
		conn.Close()
	}

	// --- Parallel (Happy Eyeballs) ---
	// DialParallel requires a mix of ipv4 + ipv6, otherwise falls back to serial.
	// We use 198.18.0.x (ipv4) + fd00::x (ipv6) to trigger the dual-stack path.
	addr4Fast := netip.MustParseAddr("198.18.0.1")
	addr6Slow := netip.MustParseAddr("fd00::1")
	mixedAddresses := []netip.Addr{addr4Fast, addr6Slow}

	parallelTimes := make([]time.Duration, rounds)
	for i := 0; i < rounds; i++ {
		d := &simulatedDialer{
			delays:     map[netip.Addr]time.Duration{addr4Fast: 10 * time.Millisecond, addr6Slow: 500 * time.Millisecond},
			realPort:   port,
			dialCounts: make(map[netip.Addr]int),
		}
		start := time.Now()
		conn, err := DialParallel(context.Background(), d, "tcp",
			M.Socksaddr{Fqdn: "example.com", Port: port}, mixedAddresses, false, 300*time.Millisecond)
		parallelTimes[i] = time.Since(start)
		if err != nil {
			t.Fatalf("parallel round %d: %v", i, err)
		}
		conn.Close()
	}

	// --- Results ---
	t.Logf("\n=== Results (3 addrs: 500ms, 200ms, 10ms; worst-first order) ===")
	t.Logf("%-12s  %s", "Strategy", "Round times")
	t.Logf("%-12s  %s", "Serial", fmtDurations(serialTimes))
	t.Logf("%-12s  %s", "Concurrent", fmtDurations(concurrentTimes))
	t.Logf("%-12s  %s  (ipv4=10ms + ipv6=500ms, fallback=300ms)", "Parallel", fmtDurations(parallelTimes))

	avgSerial := avg(serialTimes)
	avgConcurrent := avg(concurrentTimes)
	avgParallel := avg(parallelTimes)

	t.Logf("\n%-12s  avg = %v", "Serial", avgSerial.Truncate(time.Millisecond))
	t.Logf("%-12s  avg = %v", "Concurrent", avgConcurrent.Truncate(time.Millisecond))
	t.Logf("%-12s  avg = %v", "Parallel", avgParallel.Truncate(time.Millisecond))

	speedup := float64(avgSerial) / float64(avgConcurrent)
	t.Logf("\nConcurrent is %.1fx faster than Serial", speedup)

	if avgConcurrent >= avgSerial {
		t.Errorf("concurrent should be faster than serial: serial=%v concurrent=%v", avgSerial, avgConcurrent)
	}
	if avgConcurrent > 50*time.Millisecond {
		t.Errorf("concurrent should complete in ~10ms (fastest addr delay), but avg=%v", avgConcurrent)
	}
}

func fmtDurations(ds []time.Duration) string {
	s := ""
	for i, d := range ds {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%v", d.Truncate(time.Millisecond))
	}
	return s
}

func avg(ds []time.Duration) time.Duration {
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	return total / time.Duration(len(ds))
}
