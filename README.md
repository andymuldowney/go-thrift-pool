# Golang Thrift client pooling

The default Thrift client for Golang is somewhat expensive to instantiate.  Under most use cases, you want multiple clients available to your application.  This pooling code creates a simple client pool with a starting and maximum size.

You must specify a pool factory function to create the client with the Thrift options you need.

## Use

### Creating the pool factory
```
func thriftPoolFactory() (*pool.ThriftPoolClient, error) {
	addr := "127.0.0.1"
	clientChan := make(chan *pool.ThriftPoolClient)
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

		clientChan <- &pool.ThriftPoolClient{TStandardClient: thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport)), Conn: socket.Conn()}
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
```

### Instantiating the pool
```
clientPool, err := pool.NewThriftPool(10, 100, thriftPoolFactory)
if err != nil {
	return err
}
```

### Getting a client from the pool
```
thriftClient, err := clientPool.Get()
```

### Returning a client to the pool
```
thriftClient.Close()
```
