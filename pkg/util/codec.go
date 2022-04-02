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

package util

import (
	"encoding/binary"
	pb "github.com/paashzj/pulsar_proto_go"
	"github.com/panjf2000/gnet"
)

var (
	encoderConfig = gnet.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}
	decoderConfig = gnet.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
	Codec = gnet.NewLengthFieldBasedFrameCodec(encoderConfig, decoderConfig)
)

func MarshalPulsarCmd(cmd *pb.BaseCommand) ([]byte, error) {
	size := cmd.Size()
	bytes := make([]byte, size+4)
	_, err := cmd.MarshalTo(bytes[4:])
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(bytes, uint32(size))
	return bytes, nil
}
