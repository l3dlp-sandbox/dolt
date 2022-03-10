// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package globalstate

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func CoerceAutoIncrementValue(val interface{}) (seq uint64, err error) {
	val, err = sql.Uint64.Convert(val)
	if err != nil {
		return 0, err
	}
	seq, ok := val.(uint64)
	if !ok {
		seq = 0
	}
	return
}

func NewAutoIncrementTracker(ctx context.Context, root *doltdb.RootValue) (ait AutoIncrementTracker, err error) {
	ait = AutoIncrementTracker{
		sequences: make(map[string]uint64),
		mu:        &sync.Mutex{},
	}

	// collect auto increment values
	err = root.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (bool, error) {
		ok := schema.HasAutoIncrement(sch)
		if !ok {
			return false, nil
		}
		seq, err := table.GetAutoIncrementValue(ctx)
		if err != nil {
			return true, err
		}
		ait.sequences[name] = seq
		return false, nil
	})
	return ait, err
}

type AutoIncrementTracker struct {
	sequences map[string]uint64
	mu        *sync.Mutex
}

// Current returns the current AUTO_INCREMENT value for |tableName|.
func (a AutoIncrementTracker) Current(tableName string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sequences[tableName]
}

// Next returns the next AUTO_INCREMENT value for |tableName|, considering the provided |insertVal|.
func (a AutoIncrementTracker) Next(tbl string, insertVal interface{}) (seq uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	given, err := CoerceAutoIncrementValue(insertVal)
	if err != nil {
		return 0, err
	}

	var ok bool
	seq, ok = a.sequences[tbl]
	if !ok {
		return 0, fmt.Errorf("missing AUTO_INCREMENT sequence")
	}

	if given >= seq {
		seq = given
		a.sequences[tbl] = given
		a.sequences[tbl]++
	}
	return
}

// Set sets the current AUTO_INCREMENT value for |tableName|.
func (a AutoIncrementTracker) Set(tableName string, val uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sequences[tableName] = val
}

// AddTable adds |tablename| to the AutoIncrementTracker.
func (a AutoIncrementTracker) AddTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sequences[tableName] = uint64(1)
}

// DropTable drops |tablename| from the AutoIncrementTracker.
func (a AutoIncrementTracker) DropTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sequences, tableName)
}
