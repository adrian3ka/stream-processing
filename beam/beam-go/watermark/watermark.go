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

// windowed_wordcount counts words in text, and can run over either unbounded
// or bounded input collections.
//
// This example is the last in a series of four successively more
// detailed 'word count' examples. First take a look at minimal_wordcount,
// wordcount, and debugging_wordcount.
//
// Basic concepts, also in the preceeding examples: Reading text files;
// counting a PCollection; writing to GCS; executing a Pipeline both locally
// and using a selected runner; defining DoFns; user-defined PTransforms;
// defining pipeline options.
//
// New Concepts:
//
//  1. Unbounded and bounded pipeline input modes
//  2. Adding timestamps to data
//  3. Windowing
//  4. Re-using PTransforms over windowed PCollections
//  5. Accessing the window of an element
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"math/rand"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/satori/go.uuid"
)

var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "./input.txt", "File(s) to read.")

	// Set this required option to specify where to write the output file.
	mergedOutput    = flag.String("mergedOutput", "./mergedOutput.txt", "Output (required).")
	unmergedOutput  = flag.String("unmergedOutput", "./unmergedOutput.txt", "Output (required).")
	unmergedOutput2 = flag.String("unmergedOutput2", "./unmergedOutput2.txt", "Output (required).")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*addTimestampFn)(nil)).Elem())
}

// Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
// this example, for the bounded data case.
//
// Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
// his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
// 2-hour period.

type addTimestampFn struct {
	Min beam.EventTime `json:"min"`
}

func displayTime(timestamp mtime.Time) time.Time {
	return time.Unix(int64(timestamp/1000), 0)
}

func (f *addTimestampFn) ProcessElement(x beam.X) (beam.EventTime, beam.X) {
	timestamp := f.Min.Add(time.Duration(rand.Int63n(2 * time.Hour.Nanoseconds())))

	fmt.Println("Processing ", x, "on server time at ", displayTime(timestamp))

	return timestamp, x
}

// Concept #5: formatFn accesses the window of each element.

// formatFn is a DoFn that formats a windowed word and its count as a string.
func formatFn(iw beam.Window, et beam.EventTime, w string, c int) string {
	s := fmt.Sprintf("Time : %s - %v@%v %s: %v", displayTime(et), et, iw, w, c)
	return s
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	trigger := pipeline_v1.Trigger{
		Trigger: &pipeline_v1.Trigger_AfterEndOfWindow_{
			AfterEndOfWindow: nil,
		},
	}

	customWindow := pipeline_v1.WindowingStrategy{
		WindowFn: &pipeline_v1.FunctionSpec{
			Urn:     "",
			Payload: nil,
		},
		MergeStatus:        pipeline_v1.MergeStatus_NEEDS_MERGE,
		WindowCoderId:      uuid.NewV4().String(),
		Trigger:            &trigger,
		AccumulationMode:   pipeline_v1.AccumulationMode_ACCUMULATING,
		OutputTime:         pipeline_v1.OutputTime_LATEST_IN_PANE,
		ClosingBehavior:    pipeline_v1.ClosingBehavior_EMIT_IF_NONEMPTY,
		AllowedLateness:    60 * 1000, // 1 minutes
		OnTimeBehavior:     pipeline_v1.OnTimeBehavior_FIRE_IF_NONEMPTY,
		AssignsToOneWindow: false,
	}

	myGraph := graph.New()

	beam.WindowInto(window.Fn{})

	graphRoot := myGraph.Root()

	windowStrategies := make(map[string]*pipeline_v1.WindowingStrategy)

	windowStrategies[customWindow.GetWindowCoderId()] = &customWindow

	pipeline_v1.Components{
		Transforms:          nil,
		Pcollections:        nil,
		WindowingStrategies: windowStrategies,
		Coders:              nil,
		Environments:        nil,
	}

}
