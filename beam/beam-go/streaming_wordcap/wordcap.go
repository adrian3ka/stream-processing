// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// streaming_wordcap is a toy streaming pipeline that uses PubSub. It
// does the following:
//    (1) create a topic and publish a few messages to it
//    (2) start a streaming pipeline that converts the messages to
//        upper case and logs the result.
//
// NOTE: it only runs on Dataflow and must be manually cancelled.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/go/pkg/beam/util/pubsubx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
	"os"
	"strings"
)

var (
	data = []string{
		"foo",
		"bar",
		"baz",
	}
)

var (
	input = flag.String("input", os.ExpandEnv("streaming-wordcap"), "Pubsub Input topic.")

	_ = flag.String("output", "gs://streaming-wordcap/.dev/output.txt", "Output file (required).")
	_ = flag.Set("project", "beam-tutorial-272917")
	_ = flag.Set("temp_location", "gs://streaming-wordcap/.dev")
	_ = flag.Set("staging_location", "gs://streaming-wordcap/.staging")
	_ = flag.Set("region", "us-east1")
	_ = flag.Set("runner", "dataflow")
)

// printFn is a DoFn that emits the words in a given line.
func printFn(ctx context.Context, message []uint8, emit func(string)) {
	fmt.Println("printFn >> ", string(message))

	emit(string(message))
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	log.Infof(ctx, "Publishing %v messages to: %v", len(data), *input)

	defer pubsubx.CleanupTopic(ctx, project, *input)

	sub, err := pubsubx.Publish(ctx, project, *input, data...)

	if err != nil {
		log.Fatal(ctx, err)
	}

	p := beam.NewPipeline()

	source := p.Root()

	fmt.Println("source >> ", source)

	log.Infof(ctx, "Running streaming wordcap with subscription: %v", sub.ID())

	col := pubsubio.Read(source, project, *input, &pubsubio.ReadOptions{
		Subscription: sub.ID(),
	})

	str := beam.ParDo(source, stringx.FromBytes, col)
	capitalize := beam.ParDo(source, strings.ToUpper, str)
	debug.Print(source, capitalize)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
