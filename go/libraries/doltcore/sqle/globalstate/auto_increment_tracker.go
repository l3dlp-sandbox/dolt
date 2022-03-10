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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func init() {
	if uint64Type, _ = sql.CreateNumberType(sqltypes.Uint64); uint64Type == nil {
		panic("cannot create auto increment type")
	}
}

var uint64Type sql.Type

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

func (a AutoIncrementTracker) Current(tablename string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sequences[tablename]
}

func (a AutoIncrementTracker) Next(tbl string, insertVal interface{}) (seq uint64, err error) {
	insertVal, err = uint64Type.Convert(insertVal)
	if err != nil {
		return 0, err
	}

	given, ok := insertVal.(uint64)
	if !ok {
		given = 0
	}

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

// todo(andy) can we alter to less than current?
func (a AutoIncrementTracker) Set(tableName string, val interface{}) (err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	val, err = uint64Type.Convert(val)
	if err != nil {
		return err
	}

	seq, ok := val.(uint64)
	if !ok {
		return fmt.Errorf("invalid AUTO_INCREMENT value %v", val)
	}
	a.sequences[tableName] = seq
	return
}

func (a AutoIncrementTracker) AddTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sequences[tableName] = uint64(1)
}

func (a AutoIncrementTracker) DropTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sequences, tableName)
}
