// Copyright 2020 Dolthub, Inc.
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

package writer

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type writeSession struct {
	writers map[string]tableWriter
	root    *doltdb.RootValue
	mu      *sync.RWMutex

	opts editor.Options
}

var _ WriteSession = &writeSession{}

func CreateWriteSession(root *doltdb.RootValue, opts editor.Options) WriteSession {
	return &writeSession{
		opts:    opts,
		root:    root,
		writers: make(map[string]tableWriter),
		mu:      &sync.RWMutex{},
	}
}

func (ws *writeSession) GetTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.getOrCreateTableWriter(ctx, table, database, ait, setter, batched)
}

func (ws *writeSession) getOrCreateTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (tw tableWriter, err error) {
	tw, ok := ws.writers[table]
	if ok {
		return tw, nil
	}

	tbl, ok, err := ws.root.GetTable(ctx, table)
	if err != nil {
		return tw, err
	}
	if !ok {
		return tw, doltdb.ErrTableNotFound
	}

	nomsWriter, err := makeNomsTableWriter(ctx, ws.root, table, database, ws.opts)
	if err != nil {
		return tw, err
	}

	tw = tableWriter{
		table:        table,
		database:     database,
		dependencies: nil,
		writer:       nomsWriter,
		autoTrack:    ait,
		autoSet:      nomsWriter,
		sess:         ws,
		setter:       setter,
		batched:      batched,
	}

	deps, err := ws.getConstraintsForTable(ctx, tbl, tw)
	if err != nil {
		return tw, err
	}
	tw.dependencies = deps

	ws.writers[table] = tw

	return tw, nil
}

func (ws *writeSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	return ws.flush(ctx)
}

func (ws *writeSession) UpdateRoot(ctx context.Context, cb func(ctx context.Context, current *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	current, err := ws.flush(ctx)
	if err != nil {
		return err
	}

	mutated, err := cb(ctx, current)
	if err != nil {
		return err
	}

	return ws.setRoot(ctx, mutated)
}

func (ws *writeSession) GetOptions() editor.Options {
	return ws.opts
}

func (ws *writeSession) SetOptions(opts editor.Options) {
	ws.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (ws *writeSession) flush(ctx context.Context) (*doltdb.RootValue, error) {
	flushed := make(map[string]*doltdb.Table, len(ws.writers))
	mut := &sync.Mutex{}

	eg, ctx := errgroup.WithContext(ctx)
	for name, writer := range ws.writers {
		eg.Go(func() error {
			t, err := writer.Table(ctx)
			if err != nil {
				return err
			}

			mut.Lock()
			defer mut.Unlock()
			flushed[name] = t

			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	// todo(andy): update tables in unison
	root := ws.root
	for name, table := range flushed {
		root, err = root.PutTable(ctx, name, table)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (ws *writeSession) setRoot(ctx context.Context, root *doltdb.RootValue) error {
	if root == nil {
		return fmt.Errorf("cannot set a writeSession's root to nil once it has been created")
	}

	// todo(andy): are old pointers still valid?

	ws.root = root
	ws.writers = make(map[string]tableWriter)

	return nil
}

func (ws *writeSession) getConstraintsForTable(ctx context.Context, tbl *doltdb.Table, tw tableWriter) (deps []writeDependency, err error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// PRIMARY KEY
	if !schema.IsKeyless(sch) {
		pk, err := primaryKeyFromTable(ctx, tw.database, tw.table, tbl, sch)
		if err != nil {
			return nil, err
		}
		deps = append(deps, pk)
	}

	// UNIQUE KEY
	uks, err := uniqueKeysFromTable(ctx, tw.database, tw.table, tbl, sch)
	if err != nil {
		return nil, err
	}
	deps = append(deps, uks...)

	// FOREIGN KEY
	fkc, err := ws.root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	children, parents := fkc.KeysForTable(tw.table)

	for _, fk := range children {
		dep, err := makeFkChildConstraint(ctx, tw.database, ws.root, fk)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}

	for _, fk := range parents {
		childWriter, err := ws.getOrCreateTableWriter(ctx, fk.TableName, tw.database, tw.autoTrack, tw.setter, tw.batched)
		if err != nil {
			return nil, err
		}

		dep, err := makeFkParentConstraint(ctx, tw.database, ws.root, fk, childWriter)
		if err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}

	return nil, nil
}
