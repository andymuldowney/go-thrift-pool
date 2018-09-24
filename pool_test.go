package pool

import (
	"fmt"
	"net"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

func testPoolFactory() (*ThriftPoolClient, error) {
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("Error creating test server: %v", err)
	}
	addr := listen.Addr().String()

	clientChan := make(chan *ThriftPoolClient)
	errChan := make(chan error)

	go func(addr string) {
		protocolFactory := thrift.NewTBinaryProtocolFactory(true, true)
		transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

		socket, err := thrift.NewTSocket(addr)
		if err != nil {
			errChan <- err
		}
		if socket == nil {
			errChan <- fmt.Errorf("Error opening socket, got nil transport")
		}

		transport, err := transportFactory.GetTransport(socket)
		if err != nil {
			errChan <- fmt.Errorf("Error from transportFactory, got nil transport")
		}

		err = transport.Open()
		if err != nil {
			errChan <- err
		}

		clientChan <- &ThriftPoolClient{TStandardClient: thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport)), Conn: socket.Conn()}
	}(addr)

	select {
	case client := <-clientChan:
		return client, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(time.Second * 3):
		return nil, fmt.Errorf("Timeout waiting for connection")
	}
}

// Test a new client pool
func TestNewClientPool(t *testing.T) {
	_, err := NewThriftPool(10, 100, testPoolFactory)
	if err != nil {
		t.Errorf("Error creating client pool: %v", err)
	}
}

// Confirm the initial client pool is of the correct size
func TestNewClientInitialSize(t *testing.T) {
	initialSize := 5
	maxSize := 100
	testPool, err := NewThriftPool(initialSize, maxSize, testPoolFactory)
	if err != nil {
		t.Errorf("Error creating client pool: %v", err)
	}

	poolSize := testPool.Len()
	if poolSize != initialSize {
		fmt.Errorf("Client pool size is wrong - got %d, expected %d", poolSize, initialSize)
	}
}

// Create a new client with an initial size that exceeds the maximum size
func TestNewClientInitialSizeExceedsMaxSize(t *testing.T) {
	maxSize := 100
	initialSize := maxSize * 2
	testPool, err := NewThriftPool(initialSize, maxSize, testPoolFactory)
	if err != nil {
		t.Errorf("Error creating client pool: %v", err)
	}

	poolSize := testPool.Len()
	if poolSize != maxSize {
		fmt.Errorf("Client pool size is wrong - got %d, expected %d", poolSize, maxSize)
	}
}

// Get a single client
func TestGetClient(t *testing.T) {
	testPool, err := NewThriftPool(1, 2, testPoolFactory)
	if err != nil {
		t.Errorf("Error creating client pool: %v", err)
	}

	client, err := testPool.Get()
	if err != nil || client == nil {
		t.Errorf("Could not get a client: %v", err)
	}
}

// Expect a client to be unavailable for a period of time
func TestGetClientUnavailable(t *testing.T) {
	testPool, err := NewThriftPool(1, 1, testPoolFactory)
	if err != nil {
		t.Errorf("Error creating client pool: %v", err)
	}

	client, err := testPool.Get()
	if err != nil || client == nil {
		t.Errorf("Could not get a client: %v", err)
	}

	client, err = testPool.Get()
	if err == nil && client != nil {
		t.Errorf("We got a client when the pool should have been exhausted")
	}
}
