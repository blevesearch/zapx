//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import "github.com/blevesearch/vellum"

// vellumBuilderOpts is used for every FST zap builds (term dictionaries and
// synonym thesauri). Suffix sharing is off: each term maps to its own offset
// or 1-hit value, so almost no suffixes are identical and the registry search
// costs time and memory for little gain (term dictionaries grow 0-5%). The
// result is an ordinary vellum FST, readable by every existing reader, so the
// zap format is unchanged. Needs a vellum whose builder accepts
// RegistryTableSize 0; vellum v1.2.0 panics on it.
var vellumBuilderOpts = &vellum.BuilderOpts{
	Encoder:           1,
	RegistryTableSize: 0,
	RegistryMRUSize:   1,
}
