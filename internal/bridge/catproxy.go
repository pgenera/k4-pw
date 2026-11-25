package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const clientWriteTimeout = 2 * time.Second

// CATProxyConfig configures a local TCP CAT listener (e.g. 127.0.0.1:9200 for WSJT-X/Hamlib)
// that multiplexes all ASCII CAT commands and replies directly over the Session's :9205
// connection (single TCP client slot on the K4, native :9205 TX audio ownership).
type CATProxyConfig struct {
	ListenAddr string
}

// CATProxy manages a local TCP listener for Hamlib/WSJT-X CAT control over :9205.
type CATProxy struct {
	cfg    CATProxyConfig
	sess   *Session
	logger *log.Logger

	ln        net.Listener
	closeOnce sync.Once
	wg        sync.WaitGroup

	clientsMu sync.Mutex
	clients   map[net.Conn]struct{}
}

// StartCATProxy binds cfg.ListenAddr and multiplexes CAT over sess's :9205 connection.
func StartCATProxy(ctx context.Context, cfg CATProxyConfig, sess *Session, logger *log.Logger) (*CATProxy, error) {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return nil, errors.New("catproxy: ListenAddr is required")
	}
	if sess == nil {
		return nil, errors.New("catproxy: session is required")
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("catproxy listen %s: %w", cfg.ListenAddr, err)
	}

	p := &CATProxy{
		cfg:     cfg,
		sess:    sess,
		logger:  logger,
		ln:      ln,
		clients: make(map[net.Conn]struct{}),
	}
	sess.SetCATReplySink(p.broadcastReply)

	p.wg.Add(1)
	go p.acceptLoop(ctx)
	return p, nil
}

// Addr returns the bound TCP listener address.
func (p *CATProxy) Addr() net.Addr {
	return p.ln.Addr()
}

// Close stops accepting new CAT connections and closes active client sockets.
func (p *CATProxy) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.sess.SetCATReplySink(nil)
		err = p.ln.Close()
		p.clientsMu.Lock()
		for c := range p.clients {
			_ = c.Close()
		}
		p.clientsMu.Unlock()
	})
	p.wg.Wait()
	return err
}

func (p *CATProxy) broadcastReply(rawCAT string) {
	if rawCAT == "" {
		return
	}
	data := []byte(rawCAT)
	p.clientsMu.Lock()
	conns := make([]net.Conn, 0, len(p.clients))
	for c := range p.clients {
		conns = append(conns, c)
	}
	p.clientsMu.Unlock()

	for _, c := range conns {
		if err := writePacket(c, data, clientWriteTimeout); err != nil {
			_ = c.Close()
		}
	}
}

func (p *CATProxy) acceptLoop(ctx context.Context) {
	defer p.wg.Done()
	go func() {
		<-ctx.Done()
		_ = p.ln.Close()
	}()

	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			p.logger.Printf("catproxy accept error: %v", err)
			continue
		}
		if tcpC, ok := clientConn.(*net.TCPConn); ok {
			_ = tcpC.SetNoDelay(true)
		}
		p.clientsMu.Lock()
		p.clients[clientConn] = struct{}{}
		p.clientsMu.Unlock()

		p.wg.Add(1)
		go func(c net.Conn) {
			defer p.wg.Done()
			p.handleClient(ctx, c)
		}(clientConn)
	}
}

func isPTTKeyCmd(cmdUpper string) bool {
	return cmdUpper == "TX" || cmdUpper == "TX0" || cmdUpper == "TX1"
}

func isPTTUnkeyCmd(cmdUpper string) bool {
	return cmdUpper == "RX"
}

func (p *CATProxy) handleClient(ctx context.Context, clientConn net.Conn) {
	defer func() {
		p.clientsMu.Lock()
		delete(p.clients, clientConn)
		p.clientsMu.Unlock()
		_ = clientConn.Close()
	}()

	var clientKeyed bool
	defer func() {
		if clientKeyed {
			unkeyCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = p.sess.SendClientCAT(unkeyCtx, "RX;")
			cancel()
		}
	}()

	var pending []byte
	buf := make([]byte, 4096)
	for {
		n, rerr := clientConn.Read(buf)
		if n > 0 {
			pending = append(pending, buf[:n]...)
			for {
				semi := indexByte(pending, ';')
				if semi < 0 {
					break
				}
				rawToken := string(pending[:semi+1])
				cmdBody := strings.ToUpper(strings.TrimSpace(string(pending[:semi])))

				if isPTTKeyCmd(cmdBody) {
					clientKeyed = true
				} else if isPTTUnkeyCmd(cmdBody) {
					clientKeyed = false
				}

				if err := p.sess.SendClientCAT(ctx, rawToken); err != nil {
					p.logger.Printf("catproxy :9205 send %q failed: %v", rawToken, err)
					return
				}

				rem := len(pending) - (semi + 1)
				if rem > 0 {
					copy(pending[:rem], pending[semi+1:])
				}
				pending = pending[:rem]
			}
		}
		if rerr != nil {
			return
		}
	}
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}
