// Copyright (c) 2021 Andy Pan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package gnet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/v2/internal/netpoll"
	"github.com/panjf2000/gnet/v2/internal/queue"
	"github.com/panjf2000/gnet/v2/internal/socket"
	"github.com/panjf2000/gnet/v2/internal/toolkit"
	"github.com/panjf2000/gnet/v2/pkg/buffer/elastic"
	"github.com/panjf2000/gnet/v2/pkg/buffer/ring"
	gerrors "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

// ClientAsync of gnet.
type ClientAsync struct {
	opts     *Options
	el       *eventloop
	logFlush func() error
}

// NewClientAsync creates an instance of ClientAsync.
func NewClientAsync(eventHandler EventHandler, opts ...Option) (cli *ClientAsync, err error) {
	options := loadOptions(opts...)
	cli = new(ClientAsync)
	cli.opts = options
	var logger logging.Logger
	if options.LogPath != "" {
		if logger, cli.logFlush, err = logging.CreateLoggerAsLocalFile(options.LogPath, options.LogLevel); err != nil {
			return
		}
	} else {
		logger = logging.GetDefaultLogger()
	}
	if options.Logger == nil {
		options.Logger = logger
	}
	var p *netpoll.Poller
	if p, err = netpoll.OpenPoller(); err != nil {
		return
	}
	eng := new(engine)
	eng.opts = options
	eng.eventHandler = eventHandler
	eng.ln = &listener{network: "udp"}
	eng.cond = sync.NewCond(&sync.Mutex{})
	if options.Ticker {
		eng.tickerCtx, eng.cancelTicker = context.WithCancel(context.Background())
	}
	el := new(eventloop)
	el.ln = eng.ln
	el.engine = eng
	el.poller = p

	rbc := options.ReadBufferCap
	switch {
	case rbc <= 0:
		options.ReadBufferCap = MaxStreamBufferCap
	case rbc <= ring.DefaultBufferSize:
		options.ReadBufferCap = ring.DefaultBufferSize
	default:
		options.ReadBufferCap = toolkit.CeilToPowerOfTwo(rbc)
	}
	wbc := options.WriteBufferCap
	switch {
	case wbc <= 0:
		options.WriteBufferCap = MaxStreamBufferCap
	case wbc <= ring.DefaultBufferSize:
		options.WriteBufferCap = ring.DefaultBufferSize
	default:
		options.WriteBufferCap = toolkit.CeilToPowerOfTwo(wbc)
	}

	el.buffer = make([]byte, options.ReadBufferCap)
	el.udpSockets = make(map[int]*conn)
	el.connections = make(map[int]*conn)
	el.eventHandler = eventHandler
	cli.el = el
	return
}

// Start starts the client event-loop, handing IO events.
func (cli *ClientAsync) Start() error {
	cli.el.eventHandler.OnBoot(Engine{})
	cli.el.engine.wg.Add(1)
	go func() {
		cli.el.run(cli.opts.LockOSThread)
		cli.el.engine.wg.Done()
	}()
	// Start the ticker.
	if cli.opts.Ticker {
		go cli.el.ticker(cli.el.engine.tickerCtx)
	}
	return nil
}

// Stop stops the client event-loop.
func (cli *ClientAsync) Stop() (err error) {
	logging.Error(cli.el.poller.UrgentTrigger(func(_ interface{}) error { return gerrors.ErrEngineShutdown }, nil))
	cli.el.engine.wg.Wait()
	logging.Error(cli.el.poller.Close())
	cli.el.eventHandler.OnShutdown(Engine{})
	// Stop the ticker.
	if cli.opts.Ticker {
		cli.el.engine.cancelTicker()
	}
	if cli.logFlush != nil {
		err = cli.logFlush()
	}
	logging.Cleanup()
	return
}

func (cli *ClientAsync) DialIpv4(network, address string) (Conn, error) {
	var (
		fd             int
		remoteAddr     net.Addr
		localSockAddr  unix.Sockaddr
		remoteSockAddr unix.Sockaddr
		err            error
	)
	switch strings.ToLower(network) {
	case "tcp", "tcp4", "tcp6":
		fd, remoteAddr, err = socket.TCPSocket(network, address, false)
	case "udp", "udp4", "udp6":
		fd, remoteAddr, err = socket.UDPSocket(network, address, false)
	case "unix":
		_ = os.RemoveAll(address)
		fd, remoteAddr, err = socket.UnixSocket(network, address, false)
	default:
		err = errors.New("unsupported network: " + network)
	}

	if t, ok := err.(*os.SyscallError); ok {
		err = t.Err
	}

	if err != nil && err != unix.EINPROGRESS {
		return nil, err
	}

	if err != unix.EINPROGRESS {
		localSockAddr, err = unix.Getsockname(fd)
		if err != nil {
			log.Println("get sockname error", err)
			return nil, err
		}
		remoteSockAddr, err = unix.Getpeername(fd)
		if err != nil {
			return nil, err
		}
	}

	conn := newClientTCPConn(fd, cli.el, remoteSockAddr, socket.SockaddrToTCPOrUnixAddr(localSockAddr), remoteAddr)

	if err == unix.EINPROGRESS {
		cli.el.poller.UrgentTrigger(cli.el.registerAsyncClient, conn)
		return conn, nil
	}
	return conn, nil
}

// Dial is like net.Dial().
func (cli *ClientAsync) Dial(network, address string) (Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	sc, ok := c.(syscall.Conn)
	if !ok {
		return nil, errors.New("failed to convert net.Conn to syscall.Conn")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, errors.New("failed to get syscall.RawConn from net.Conn")
	}

	var DupFD int
	e := rc.Control(func(fd uintptr) {
		DupFD, err = unix.Dup(int(fd))
	})
	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	if strings.HasPrefix(network, "tcp") {
		if cli.opts.TCPNoDelay == TCPDelay {
			if err = socket.SetNoDelay(DupFD, 0); err != nil {
				return nil, err
			}
		}
		if cli.opts.TCPKeepAlive > 0 {
			if err = socket.SetKeepAlivePeriod(DupFD, int(cli.opts.TCPKeepAlive.Seconds())); err != nil {
				return nil, err
			}
		}
	}

	if cli.opts.SocketSendBuffer > 0 {
		if err = socket.SetSendBuffer(DupFD, cli.opts.SocketSendBuffer); err != nil {
			return nil, err
		}
	}
	if cli.opts.SocketRecvBuffer > 0 {
		if err = socket.SetRecvBuffer(DupFD, cli.opts.SocketRecvBuffer); err != nil {
			return nil, err
		}
	}

	var (
		sockAddr unix.Sockaddr
		gc       Conn
	)
	switch c.(type) {
	case *net.UnixConn:
		if sockAddr, _, _, err = socket.GetUnixSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		ua := c.LocalAddr().(*net.UnixAddr)
		ua.Name = c.RemoteAddr().String() + "." + strconv.Itoa(DupFD)
		gc = newTCPConn(DupFD, cli.el, sockAddr, c.LocalAddr(), c.RemoteAddr())
	case *net.TCPConn:
		if sockAddr, _, _, _, err = socket.GetTCPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		gc = newTCPConn(DupFD, cli.el, sockAddr, c.LocalAddr(), c.RemoteAddr())
	case *net.UDPConn:
		if sockAddr, _, _, _, err = socket.GetUDPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String()); err != nil {
			return nil, err
		}
		gc = newUDPConn(DupFD, cli.el, c.LocalAddr(), sockAddr, true)
	default:
		return nil, gerrors.ErrUnsupportedProtocol
	}
	err = cli.el.poller.UrgentTrigger(cli.el.register, gc)
	if err != nil {
		gc.Close()
		return nil, err
	}
	return gc, nil
}

func newClientTCPConn(fd int, el *eventloop, sa unix.Sockaddr, localAddr, remoteAddr net.Addr) (c *conn) {
	c = &conn{
		fd:         fd,
		peer:       sa,
		loop:       el,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
	}
	c.outboundBuffer, _ = elastic.New(el.engine.opts.WriteBufferCap)
	c.pollAttachment = netpoll.GetPollAttachment()
	c.pollAttachment.FD = fd
	// c.pollAttachment.Callback = c.handleAsyncClientEvents
	return
}

func (el *eventloop) registerAsyncClient(itf interface{}) error {
	c := itf.(*conn)
	if c.pollAttachment == nil { // UDP socket
		c.pollAttachment = netpoll.GetPollAttachment()
		c.pollAttachment.FD = c.fd
		c.pollAttachment.Callback = el.readUDP
		if err := el.poller.AddReadWrite(c.pollAttachment); err != nil {
			_ = unix.Close(c.fd)
			c.releaseUDP()
			return err
		}
		el.udpSockets[c.fd] = c
		return nil
	}
	if err := el.poller.AddReadWrite(c.pollAttachment); err != nil {
		_ = unix.Close(c.fd)
		c.releaseTCP()
		return err
	}
	logging.Debugf("registerAsyncClient: %v", c.fd)
	el.connections[c.fd] = c
	// return el.open(c)
	return nil
}

// func (c *conn) handleAsyncClientEvents(_ int, ev uint32) error {
// 	logging.Debugf("handleAsyncClientEvents are called")

// 	if ev&netpoll.InEvents != 0 {
// 		n, _, err := unix.Recvfrom(c.fd, make([]byte, 1), unix.MSG_PEEK)
// 		if err != nil {
// 			return err
// 		}
// 		if n == 0 {
// 			return io.EOF
// 		}
// 	} else if ev&netpoll.OutEvents == 0 {
// 		return errors.New("unexpected event")
// 	}
// 	err := c.loop.open(c)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (el *eventloop) handleConnectError(fd int, ev uint32, c *conn) error {
	var (
		err  error
		rerr error
	)
	// TODO set correct error
	err = os.NewSyscallError("connect", errors.New("connect error"))
	err0, err1 := el.poller.Delete(c.fd), unix.Close(c.fd)
	if err0 != nil {
		rerr = fmt.Errorf("failed to delete fd=%d from poller in event-loop(%d): %v", c.fd, el.idx, err0)
	}
	if err1 != nil {
		err1 = fmt.Errorf("failed to close fd=%d in event-loop(%d): %v", c.fd, el.idx, os.NewSyscallError("close", err1))
		if rerr != nil {
			rerr = errors.New(rerr.Error() + " & " + err1.Error())
		} else {
			rerr = err1
		}
	}

	delete(el.connections, c.fd)
	if el.eventHandler.OnClose(c, err) == Shutdown {
		rerr = gerrors.ErrEngineShutdown
	}
	c.releaseTCP()
	if rerr != nil {
		return rerr
	}
	return nil
}

func Submit(c Conn, fn queue.TaskFunc, arg interface{}) error {
	return c.(*conn).loop.poller.Trigger(fn, arg)
}

func UrgentSubmit(c Conn, fn queue.TaskFunc, arg interface{}) error {
	return c.(*conn).loop.poller.UrgentTrigger(fn, arg)
}
