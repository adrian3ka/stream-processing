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
	"math/rand"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/test/integration/wordcount"
)

var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "./input.txt", "File(s) to read.")

	// Set this required option to specify where to write the output file.
	mergedOutput = flag.String("mergedOutput", "./mergedOutput.txt", "Output (required).")
)

var (
	duration = time.Second * 60
	period   = time.Second * 10
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
	upperWindow := iw.MaxTimestamp() + 1
	lowerWindow := upperWindow.Subtract(period)

	// 1. Search first column data -> et on human readable form
	// 2. Block and find the data, it's should be found on another range time on the specific period. Will be splitted
	//    into 6 because 60 seconds divide by 10 seconds will be passed into 6 windows.
	//    e.g.
	//      18:20:20 : 18:20:30
	//      18:20:30 : 18:20:40
	//      18:20:40 : 18:20:50
	//      18:20:50 : 18:21:00
	//      18:21:00 : 18:21:10
	//      18:21:10 : 18:21:20
	// 3. You should realize that sliding window will be duplicated

	s := fmt.Sprintf("Time : %s | %s:%s- %s: %v",
		displayTime(et), displayTime(lowerWindow), displayTime(upperWindow), w, c)
	return s
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *mergedOutput == "" {
		log.Exit(ctx, "No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	// Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
	// unbounded input source.
	lines := textio.Read(s, *input)

	// Concept #2: Add an element timestamp, using an artificial time just to show windowing.
	timestampedLines := beam.ParDo(s, &addTimestampFn{Min: mtime.Now()}, lines)

	// Concept #3: WindowingStrategy into sliding windows. The sliding window size for this example is 1
	// minute with period 10 seconds. The data will splitted into 6 frame, every 10 seconds will wrap the data
	// to be processed for the next pipe
	// See the documentation for more information on how sliding windows work, and
	// for information on the other types of windowing available (e.g., fixed windows).
	windowedLines := beam.WindowInto(s, window.NewSessions(duration), timestampedLines)

	// Concept #4: Re-use our existing CountWords transform that does not have knowledge of
	// windows over a PCollection containing windowed values.
	counted := wordcount.CountWords(s, windowedLines)

	// TODO(herohde) 4/16/2018: textio.Write does not support windowed writes, so we
	// simply include the window in the output and re-window back into the global window
	// before the write.

	formatted := beam.ParDo(s, formatFn, counted)
	merged := beam.WindowInto(s, window.NewGlobalWindows(), formatted)
	textio.Write(s, *mergedOutput, merged)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
