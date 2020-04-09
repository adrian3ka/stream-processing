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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", os.ExpandEnv("./old_pond.txt"), "Files to read.")
	output = flag.String("output", "./output", "Prefix of output.")
)

// stitch constructs two composite PTranformations that provide input to each other. It
// is a (deliberately) complex DAG to show what kind of structures are possible.
func stitch(s beam.Scope, words beam.PCollection) (beam.PCollection, beam.PCollection) {
	fmt.Println("Setting up stitch pipeline.....")
	ping := s.Scope("ping")
	pong := ping // s.Scope("pong")

	// NOTE(herohde) 2/23/2017: Dataflow does not allow cyclic composite structures.

	small1, big1 := beam.ParDo2(ping, multiFn, words, beam.SideInput{Input: words}) // self-sample (ping)

	debug.Printf(s, "finish small1 ..... %s", small1)
	debug.Printf(s, "finish big1 ..... %s", big1)

	small2, big2 := beam.ParDo2(pong, multiFn, words, beam.SideInput{Input: big1}) // big-sample  (pong). More words are small.

	debug.Printf(s, "finish small2 ..... %s", small2)
	debug.Printf(s, "finish big2 ..... %s", big2)

	_, big3 := beam.ParDo2(ping, multiFn, big2, beam.SideInput{Input: small1}) // small-sample big (ping). All words are big.

	debug.Printf(s, "finish big3 ..... %s", big3)

	small4, _ := beam.ParDo2(pong, multiFn, small2, beam.SideInput{Input: big3}) // big-sample small (pong). All words are small.

	debug.Printf(s, "finish small4 ..... %s", small4)

	return small4, big3
}

// Slice side input.

func multiFn(word string, sample []string, small, big func(string)) error {
	// TODO: side input processing into start bundle, once supported.
	fmt.Println("-----------------------multiFn-----------------------")
	fmt.Println("word >> ", word)
	fmt.Println("sample >> ", sample)

	count := 0
	size := 0
	for _, w := range sample {
		count++
		size += len(w)
	}
	if count == 0 {
		return errors.New("Empty sample")
	}
	avg := size / count

	categorizedAsSmall := len(word) < avg

	fmt.Println("average length : ", avg, " | word >> ",
		word, " | categorizedAsSmall >> ", categorizedAsSmall)

	if categorizedAsSmall {
		small(word)
	} else {
		big(word)
	}

	return nil
}

func subset(s beam.Scope, a, b beam.PCollection) {
	fmt.Println("Setting up subset pipeline.....")
	beam.ParDo0(s, subsetFn, beam.Impulse(s), beam.SideInput{Input: a}, beam.SideInput{Input: b})
}

func subsetFn(_ []byte, a, b func(*string) bool) error {
	fmt.Println("---------------subsetFn------------------")
	larger := make(map[string]bool)
	var elm string

	for b(&elm) {
		larger[elm] = true
	}

	fmt.Println("larger element >> ", larger)

	for a(&elm) {
		fmt.Println("a's element >> ", elm)
		if !larger[elm] {
			return fmt.Errorf("Extra element: %v", elm)
		}
	}
	return nil
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// Step by step :
// lines -> words get all string
// words : [old pond a frog leaps in water's sound]
// multifn :
//   -> small : [a in]
//   -> big : [old pond frog leaps water's sound]

// stitch (input : words) :
//   -> small1 : [a in]
//   -> big1 : [old pond frog leaps water's sound]

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running pingpong")

	// PingPong constructs a convoluted pipeline with two "cyclic" composites.
	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	words := beam.ParDo(s, extractFn, lines)

	// Run baseline and stitch; then compare them.
	small, big := beam.ParDo2(s, multiFn, words, beam.SideInput{Input: words})
	debug.Printf(s, "finish main small -> %s", small)
	debug.Printf(s, "finish main big -> %s", big)

	small2, big2 := stitch(s, words)
	debug.Printf(s, "finish main small2 -> %s", small2)
	debug.Printf(s, "finish main big2 -> %s", big2)

	subset(s, small, small2)
	subset(s, big2, big)

	textio.Write(s, *output+"small.txt", small2)
	textio.Write(s, *output+"big.txt", big2)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
