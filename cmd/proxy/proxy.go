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

package main

import (
	"flag"
	"github.com/paashzj/pulsar_proxy_go/pkg/api"
	"github.com/paashzj/pulsar_proxy_go/pkg/proxy"
	"os"
	"os/signal"
)

var (
	proxyVersion         = flag.String("proxy_version", "2.9.1", "proxy version")
	proxyProtocolVersion = flag.Int("proxy_protocol_version", 19, "proxy protocol version")
	proxyListenHost      = flag.String("proxy_listen_host", "localhost", "proxy listen host")
	proxyHttpPort        = flag.Int("proxy_http_port", 6680, "proxy http port")
	proxyTcpPort         = flag.Int("proxy_tcp_port", 8850, "proxy tcp port")
	pulsarHost           = flag.String("pulsar_host", "localhost", "proxy pulsar host")
	pulsarHttpPort       = flag.Int("pulsar_http_port", 8080, "pulsar http port")
	pulsarTcpPort        = flag.Int("pulsar_tcp_port", 6650, "pulsar tcp port")
)

func main() {
	networkConfig := &api.NetworkConfig{}
	networkConfig.ListenHost = *proxyListenHost
	networkConfig.ListenHttpPort = *proxyHttpPort
	networkConfig.ListenTcpPort = *proxyTcpPort
	proxyConfig := &api.ProxyConfig{}
	proxyConfig.ProxyVersion = *proxyVersion
	proxyConfig.ProxyProtocolVersion = int32(*proxyProtocolVersion)
	proxyConfig.PulsarHost = *pulsarHost
	proxyConfig.PulsarHttpPort = *pulsarHttpPort
	proxyConfig.PulsarTcpPort = *pulsarTcpPort
	_ = proxy.Run(networkConfig, proxyConfig)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
		return
	}
}
