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
	"errors"
	"fmt"
	pb "github.com/paashzj/pulsar_proto_go"
	"github.com/paashzj/pulsar_proxy_go/pkg/api"
	"github.com/paashzj/pulsar_proxy_go/pkg/network"
	"github.com/paashzj/pulsar_proxy_go/pkg/util"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	MaxMessageSize    int32 = 0
	ThroughServiceUrl       = true
)

func Run(networkConfig *api.NetworkConfig, proxyConfig *api.ProxyConfig) error {
	server, err := newProxyServer(proxyConfig)
	if err != nil {
		return err
	}
	return network.Run(networkConfig, server)
}

func newProxyServer(proxyConfig *api.ProxyConfig) (api.PulsarServer, error) {
	p := &proxyServer{}
	p.proxyConfig = proxyConfig
	p.rpcClientManager = map[string]*rpcClient{}
	return p, nil
}

type proxyServer struct {
	proxyConfig      *api.ProxyConfig
	rpcClientManager map[string]*rpcClient
	rpcClientLock    sync.RWMutex
}

func (p *proxyServer) Connect(addr net.Addr, connect *pb.CommandConnect) (*pb.CommandConnected, error) {
	if connect.ProxyToBrokerUrl != nil {
		p.buildRpcClient(addr.String(), *connect.ProxyToBrokerUrl)
		command := util.BaseCommand(pb.BaseCommand_CONNECT, connect)
		bytes, err := util.MarshalPulsarCmd(command)
		if err != nil {
			logrus.Errorf("%s, do connect failed, cause: %s", addr.String(), err)
			return nil, err
		}
		client, _ := p.getRpcClient(addr.String())
		response, err := client.Write(bytes)
		if err != nil {
			logrus.Errorf("send msg to broker failed, cause: %s", err)
			return nil, errors.New("connect to broker failed")
		}
		return response.Connected, nil
	} else {
		p.buildDefaultRpcClient(addr.String())
	}
	return &pb.CommandConnected{
		ServerVersion:   &p.proxyConfig.ProxyVersion,
		ProtocolVersion: &p.proxyConfig.ProxyProtocolVersion,
		MaxMessageSize:  &MaxMessageSize,
	}, nil
}

func (p *proxyServer) Lookup(addr net.Addr, lookup *pb.CommandLookupTopic) (*pb.CommandLookupTopicResponse, error) {
	command := util.BaseCommand(pb.BaseCommand_LOOKUP, lookup)
	bytes, err := util.MarshalPulsarCmd(command)
	if err != nil {
		logrus.Errorf("%s do lookup failed, cause: %s", addr.String(), err)
		return nil, err
	}
	response, err := p.getDefaultRpcClient(addr.String()).Write(bytes)
	if err != nil {
		logrus.Errorf("send msg to broker failed, cause: %s", err)
		return nil, err
	}
	lookupTopicResponse := response.LookupTopicResponse
	lookupTopicResponse.ProxyThroughServiceUrl = &ThroughServiceUrl
	return lookupTopicResponse, nil
}

func (p *proxyServer) DirectHandler(addr net.Addr, command *pb.BaseCommand) ([]byte, gnet.Action) {
	bytes, err := util.MarshalPulsarCmd(command)
	client, err := p.getRpcClient(addr.String())
	if err != nil {
		logrus.Errorf("get rpc client failed, cause: %s", err)
		return nil, gnet.Close
	}
	response, err := client.Write(bytes)
	if err != nil {
		logrus.Errorf("send msg to broker failed, cause: %s", err)
		return nil, gnet.Close
	}
	responseBytes, err := util.MarshalPulsarCmd(response)
	if err != nil {
		logrus.Errorf("marshal response from broker failed, cause: %s", err)
		return nil, gnet.Close
	}
	return responseBytes, gnet.None
}

func (p *proxyServer) getDefaultRpcClient(addr string) *rpcClient {
	p.rpcClientLock.RLock()
	client, exist := p.rpcClientManager[addr]
	p.rpcClientLock.RUnlock()
	if !exist {
		p.buildDefaultRpcClient(addr)
		client = p.rpcClientManager[addr]
	}
	return client
}

func (p *proxyServer) getRpcClient(addr string) (*rpcClient, error) {
	p.rpcClientLock.RLock()
	client, exist := p.rpcClientManager[addr]
	p.rpcClientLock.RUnlock()
	if !exist {
		return nil, errors.New("rpc client is not exist")
	}
	return client, nil
}

func (p *proxyServer) buildDefaultRpcClient(addr string) {
	defaultBrokerUrl := fmt.Sprintf("%s:%d", p.proxyConfig.PulsarHost, p.proxyConfig.PulsarTcpPort)
	p.buildRpcClient(addr, defaultBrokerUrl)
}

func (p *proxyServer) buildRpcClient(addr, brokerUrl string) {
	p.rpcClientLock.RLock()
	_, exist := p.rpcClientManager[addr]
	p.rpcClientLock.RUnlock()
	if !exist {
		p.rpcClientLock.Lock()
		split := strings.Split(brokerUrl, ":")
		host := split[0]
		port, _ := strconv.Atoi(split[1])
		client, err := newRpcClient(rpcEndpoint{host: host, port: port})
		if err != nil {
			logrus.Errorf("build rpc client failed, cause: %s", err)
		}
		p.rpcClientManager[addr] = client
		p.rpcClientLock.Unlock()
	}
}
