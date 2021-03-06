package pkg

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (mr *Master) Shutdown(_, _ *struct{}) error {
	Debug("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.l.Close() // causes the Accept to fail
	return nil
}

// startRPCServer staarts the Master's RPC server. It continues accepting RPC
// calls (Register in particular) for as long as the worker is alive.
func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.Address) // only needed for "unix"
	l, e := net.Listen("unix", mr.Address)
	if e != nil {
		log.Fatal("RegistrationServer", mr.Address, " error: ", e)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				Debug("RegistrationServer: accept error=%s", err.Error())
				break
			}
		}
		Debug("RegistrationServer: done\n")
	}()
}

// stopRPCServer stops the master RPC server.
// This must be done through an RPC to avoid race conditions between the RPC
// server thread and the current thread.
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.Address, "Master.Shutdown", new(struct{}), &reply)
	if !ok {
		fmt.Printf("Cleanup: RPC %s error\n", mr.Address)
	}
	Debug("cleanupRegistration: done\n")
}
