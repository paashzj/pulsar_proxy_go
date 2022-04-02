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
	"github.com/gogo/protobuf/proto"
	pb "github.com/paashzj/pulsar_proto_go"
	"github.com/paashzj/pulsar_proxy_go/pkg/test"
	"github.com/paashzj/pulsar_proxy_go/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommandConnect(t *testing.T) {
	test.SetupPulsar()
	endpoint := rpcEndpoint{}
	endpoint.host = "localhost"
	endpoint.port = 6650
	client, err := newRpcClient(endpoint)
	assert.Nil(t, err)
	defer client.close()
	cmdConnect := &pb.CommandConnect{
		ProtocolVersion: proto.Int32(PulsarProtocolVersion),
		ClientVersion:   proto.String(ClientVersionString),
	}
	p := baseCommand(pb.BaseCommand_CONNECT, cmdConnect)
	bytes, err := util.MarshalPulsarCmd(p)
	assert.Nil(t, err)
	commandResp, err := client.Write(bytes)
	assert.Nil(t, err)
	assert.NotNil(t, commandResp)
}
