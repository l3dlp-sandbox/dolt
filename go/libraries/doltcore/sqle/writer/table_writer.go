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

package writer

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
)

type tableWriter struct {
	table    string
	database string

	dependencies []writeDependency
	writer       nomsTableWriter

	autoTrack globalstate.AutoIncrementTracker
	autoSet   sql.AutoIncrementSetter

	sess   WriteSession
	setter SessionRootSetter

	batched bool
}

var _ TableWriter = tableWriter{}

func (tw tableWriter) Insert(ctx *sql.Context, row sql.Row) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.ValidateInsert(ctx, row); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Insert(ctx, row); err != nil {
			return err
		}
	}
	return tw.writer.Insert(ctx, row)
}

func (tw tableWriter) Update(ctx *sql.Context, old, new sql.Row) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.ValidateUpdate(ctx, old, new); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Update(ctx, old, new); err != nil {
			return err
		}
	}
	return tw.writer.Update(ctx, old, new)
}

func (tw tableWriter) Delete(ctx *sql.Context, row sql.Row) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.ValidateDelete(ctx, row); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Delete(ctx, row); err != nil {
			return err
		}
	}
	return tw.writer.Delete(ctx, row)
}

func (tw tableWriter) NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error) {
	return tw.autoTrack.Next(tw.table, potentialVal, tableVal)
}

func (tw tableWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) (err error) {
	if err = tw.autoSet.SetAutoIncrementValue(ctx, val); err != nil {
		return err
	}
	tw.autoTrack.Reset(tw.table, val)

	return tw.flush(ctx)
}

func (tw tableWriter) Table(ctx context.Context) (*doltdb.Table, error) {
	return tw.writer.Table(ctx)
}

func (tw tableWriter) Close(ctx *sql.Context) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.Close(ctx); err != nil {
			return err
		}
	}

	// If we're running in batched mode, don't flush
	// the edits until explicitly told to do so
	if tw.batched {
		return nil
	}
	return tw.flush(ctx)
}

func (tw tableWriter) flush(ctx *sql.Context) error {
	newRoot, err := tw.sess.Flush(ctx)
	if err != nil {
		return err
	}

	return tw.setter(ctx, tw.database, newRoot)
}

// todo(andy): the following functions are deprecated

func (tw tableWriter) StatementBegin(ctx *sql.Context) {
	for _, dep := range tw.dependencies {
		dep.StatementBegin(ctx)
	}
}

func (tw tableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return
}

func (tw tableWriter) StatementComplete(ctx *sql.Context) (err error) {
	for _, dep := range tw.dependencies {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return err
}
