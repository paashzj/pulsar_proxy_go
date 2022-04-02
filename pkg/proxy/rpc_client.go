// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proxy

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	pb "github.com/paashzj/pulsar_proto_go"
	"github.com/paashzj/pulsar_proxy_go/pkg/util"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type rpcClient struct {
	networkClient *gnet.Client
	conn          gnet.Conn
	eventsChan    chan *sendRequest
	closeCh       chan struct{}
	pendingQueue  chan *sendRequest
}

type rpcEndpoint struct {
	host string
	port int
}

func newRpcClient(endpoint rpcEndpoint) (*rpcClient, error) {
	cli := &rpcClient{}
	var err error
	cli.networkClient, err = gnet.NewClient(cli, gnet.WithCodec(util.Codec))
	if err != nil {
		return nil, err
	}
	err = cli.networkClient.Start()
	if err != nil {
		return nil, err
	}
	cli.conn, err = cli.networkClient.Dial("tcp", fmt.Sprintf("%s:%d", endpoint.host, endpoint.port))
	if err != nil {
		return nil, err
	}
	cli.eventsChan = make(chan *sendRequest, 100)
	cli.closeCh = make(chan struct{})
	cli.pendingQueue = make(chan *sendRequest, 100)
	go cli.run()
	return cli, nil
}

func (r *rpcClient) Write(bytes []byte) (*pb.BaseCommand, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result *pb.BaseCommand
	var err error
	r.WriteAsync(bytes, func(cmd *pb.BaseCommand, e error) {
		result = cmd
		err = e
		wg.Done()
	})
	wg.Wait()
	return result, err
}

func (r *rpcClient) WriteAsync(bytes []byte, callback func(*pb.BaseCommand, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	r.eventsChan <- sr
}

type sendRequest struct {
	bytes    []byte
	callback func(*pb.BaseCommand, error)
}

func (r *rpcClient) run() {
	for {
		select {
		case req := <-r.eventsChan:
			err := r.conn.AsyncWrite(req.bytes)
			if err != nil {
				req.callback(nil, err)
			}
			r.pendingQueue <- req
		case <-r.closeCh:
			logrus.Info("close rpc client, exit reconnect")
			return
		}
	}
}

func (r *rpcClient) close() {
	_ = r.networkClient.Stop()
	select {
	case r.closeCh <- struct{}{}:
	default:
	}
}

func (r *rpcClient) OnInitComplete(server gnet.Server) (action gnet.Action) {
	return gnet.None
}

func (r *rpcClient) OnShutdown(server gnet.Server) {
}

func (r *rpcClient) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logrus.Info("rpc connect opened ", c.RemoteAddr())
	return nil, gnet.None
}

func (r *rpcClient) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logrus.Info("rpc connect closed ", c.RemoteAddr())
	return gnet.None
}

func (r *rpcClient) PreWrite(c gnet.Conn) {
	logrus.Debug("pre write to ", c.RemoteAddr())
}

func (r *rpcClient) AfterWrite(c gnet.Conn, b []byte) {
	logrus.Debug("after write to ", c.RemoteAddr())
}

func (r *rpcClient) React(packet []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	cmd := &pb.BaseCommand{}
	err := proto.Unmarshal(packet[4:], cmd)
	if err != nil {
		logrus.Error("unmarshal cmd failed ", err)
		return nil, gnet.Close
	}
	request := <-r.pendingQueue
	request.callback(cmd, nil)
	return nil, gnet.None
}

func (r *rpcClient) Tick() (delay time.Duration, action gnet.Action) {
	return 5, gnet.None
}
