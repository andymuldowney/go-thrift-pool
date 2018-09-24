package pool

import (
	"fmt"
	"net"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type ThriftPool struct {
	sync.RWMutex
	clients chan *ThriftPoolClient
	factory ThriftFactory
	maxSize int
}

type ThriftPoolClient struct {
	sync.RWMutex
	*thrift.TStandardClient
	net.Conn
	p        *ThriftPool
	unusable bool
}

type ThriftFactory func() (*ThriftPoolClient, error)

// Create a new pool of Thrift clients
func NewThriftPool(initial, max int, factory ThriftFactory) (*ThriftPool, error) {
	if initial <= 0 || max <= 0 {
		return nil, fmt.Errorf("Invalid settings")
	}

	if initial > max {
		initial = max
	}

	p := &ThriftPool{
		clients: make(chan *ThriftPoolClient, max),
		factory: factory,
		maxSize: max,
	}

	for i := 0; i < initial; i++ {
		client, err := factory()
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("Factory error during pool generation: %v", err)
		}
		p.clients <- client
	}

	return p, nil
}

// Get a single client from the pool
func (p *ThriftPool) Get() (*ThriftPoolClient, error) {
	p.RLock()
	clients := p.clients
	factory := p.factory
	p.RUnlock()

	if clients == nil {
		return nil, fmt.Errorf("Channel closed")
	}

	select {
	case client := <-clients:
		if client == nil {
			return nil, fmt.Errorf("Channel closed")
		}
		return client, nil
	default:
		if cap(clients) < p.maxSize {
			p.Lock()
			defer p.Unlock()
			client, err := factory()
			if err != nil {
				return nil, err
			}
			client.p = p
			return client, nil
		}
	}

	return nil, fmt.Errorf("No clients available")
}

// Put a client back into the pool
func (p *ThriftPool) put(c *ThriftPoolClient) error {
	if c == nil {
		return fmt.Errorf("Invalid client")
	}

	p.RLock()
	defer p.RUnlock()

	if p.clients == nil {
		// Pool is closed
		c.Conn.Close()
	}

	// Try to put the resource back into the pool
	select {
	case p.clients <- c:
		return nil
	default:
		// Pool is full
		return c.Conn.Close()
	}
}

// Close the entire pool
func (p *ThriftPool) Close() {
	p.Lock()
	clients := p.clients
	p.clients = nil
	p.factory = nil
	p.Unlock()

	if clients == nil {
		return
	}

	close(clients)
	for client := range clients {
		client.Conn.Close()
	}
}

// Get the current size of the client pool
func (p *ThriftPool) Len() int {
	p.RLock()
	clients := p.clients
	p.RUnlock()
	return len(clients)
}

// Close the client and return it to the pool
func (c *ThriftPoolClient) Close() error {
	c.RLock()
	defer c.Unlock()

	if c.unusable {
		return nil
	}

	return c.p.put(c)
}

// Mark an individual client unusable
func (c *ThriftPoolClient) MarkUnusable() {
	c.Lock()
	c.unusable = true
	c.Unlock()
}
