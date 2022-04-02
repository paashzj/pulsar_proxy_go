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
)

const (
	ClientVersionString   = "Pulsar Proxy Go 0.1"
	PulsarProtocolVersion = int32(pb.ProtocolVersion_v13)
)

func baseCommand(cmdType pb.BaseCommand_Type, msg proto.Message) *pb.BaseCommand {
	cmd := &pb.BaseCommand{
		Type: &cmdType,
	}
	switch cmdType {
	case pb.BaseCommand_CONNECT:
		cmd.Connect = msg.(*pb.CommandConnect)
	case pb.BaseCommand_LOOKUP:
		cmd.LookupTopic = msg.(*pb.CommandLookupTopic)
	case pb.BaseCommand_PARTITIONED_METADATA:
		cmd.PartitionMetadata = msg.(*pb.CommandPartitionedTopicMetadata)
	case pb.BaseCommand_PRODUCER:
		cmd.Producer = msg.(*pb.CommandProducer)
	case pb.BaseCommand_SUBSCRIBE:
		cmd.Subscribe = msg.(*pb.CommandSubscribe)
	case pb.BaseCommand_FLOW:
		cmd.Flow = msg.(*pb.CommandFlow)
	case pb.BaseCommand_PING:
		cmd.Ping = msg.(*pb.CommandPing)
	case pb.BaseCommand_PONG:
		cmd.Pong = msg.(*pb.CommandPong)
	case pb.BaseCommand_SEND:
		cmd.Send = msg.(*pb.CommandSend)
	case pb.BaseCommand_SEND_ERROR:
		cmd.SendError = msg.(*pb.CommandSendError)
	case pb.BaseCommand_CLOSE_PRODUCER:
		cmd.CloseProducer = msg.(*pb.CommandCloseProducer)
	case pb.BaseCommand_CLOSE_CONSUMER:
		cmd.CloseConsumer = msg.(*pb.CommandCloseConsumer)
	case pb.BaseCommand_ACK:
		cmd.Ack = msg.(*pb.CommandAck)
	case pb.BaseCommand_SEEK:
		cmd.Seek = msg.(*pb.CommandSeek)
	case pb.BaseCommand_UNSUBSCRIBE:
		cmd.Unsubscribe = msg.(*pb.CommandUnsubscribe)
	case pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		cmd.RedeliverUnacknowledgedMessages = msg.(*pb.CommandRedeliverUnacknowledgedMessages)
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE:
		cmd.GetTopicsOfNamespace = msg.(*pb.CommandGetTopicsOfNamespace)
	case pb.BaseCommand_GET_LAST_MESSAGE_ID:
		cmd.GetLastMessageId = msg.(*pb.CommandGetLastMessageId)
	case pb.BaseCommand_AUTH_RESPONSE:
		cmd.AuthResponse = msg.(*pb.CommandAuthResponse)
	default:
		panic(fmt.Sprintf("Missing command type: %v", cmdType))
	}
	return cmd
}
