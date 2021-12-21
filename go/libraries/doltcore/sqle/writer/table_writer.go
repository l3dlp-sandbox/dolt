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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

type tableWriter struct {
	table    string
	database string

	validators   []writeDependency
	dependencies []writeDependency

	autoTrack globalstate.AutoIncrementTracker
	autoSet   sql.AutoIncrementSetter

	sess   WriteSession
	setter SessionRootSetter

	batched bool
}

var _ TableWriter = tableWriter{}

func (tw tableWriter) Insert(ctx *sql.Context, row sql.Row) (err error) {
	for _, val := range tw.validators {
		if err = val.Insert(ctx, row); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Insert(ctx, row); err != nil {
			return err
		}
	}
	return
}

func (tw tableWriter) Update(ctx *sql.Context, old, new sql.Row) (err error) {
	for _, val := range tw.validators {
		if err = val.Update(ctx, old, new); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Update(ctx, old, new); err != nil {
			return err
		}
	}
	return
}

func (tw tableWriter) Delete(ctx *sql.Context, row sql.Row) (err error) {
	for _, val := range tw.validators {
		if err = val.Delete(ctx, row); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.Delete(ctx, row); err != nil {
			return err
		}
	}
	return
}

func (tw tableWriter) StatementBegin(ctx *sql.Context) {
	for _, val := range tw.validators {
		val.StatementBegin(ctx)
	}
	for _, dep := range tw.dependencies {
		dep.StatementBegin(ctx)
	}
}

func (tw tableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	for _, val := range tw.validators {
		if err = val.StatementComplete(ctx); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return
}

func (tw tableWriter) StatementComplete(ctx *sql.Context) (err error) {
	for _, val := range tw.validators {
		if err = val.StatementComplete(ctx); err != nil {
			return err
		}
	}
	for _, dep := range tw.dependencies {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return err
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

func (tw tableWriter) Close(ctx *sql.Context) (err error) {
	for _, val := range tw.validators {
		if err = val.Close(ctx); err != nil {
			return err
		}
	}
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

type nomsTableWriter struct {
	tableName   string
	dbName      string
	sch         schema.Schema
	autoIncCol  schema.Column
	vrw         types.ValueReadWriter
	kvToSQLRow  *index.KVToSqlRowConverter
	tableEditor editor.TableEditor
}

var _ writeDependency = &nomsTableWriter{}
var _ sql.AutoIncrementSetter = &nomsTableWriter{}

func (te *nomsTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(te.sch) {
		k, v, tagToVal, err := sqlutil.DoltKeyValueAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		err = te.tableEditor.InsertKeyVal(ctx, k, v, tagToVal, te.duplicateKeyErrFunc)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.InsertKeyVal(ctx, k, v, tagToVal, te.duplicateKeyErrFunc)
		}
		return err
	}
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
	if err != nil {
		return err
	}
	err = te.tableEditor.InsertRow(ctx, dRow, te.duplicateKeyErrFunc)
	if sql.ErrForeignKeyNotResolved.Is(err) {
		if err = te.resolveFks(ctx); err != nil {
			return err
		}
		return te.tableEditor.InsertRow(ctx, dRow, te.duplicateKeyErrFunc)
	}
	return err
}

func (te *nomsTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(te.sch) {
		k, tagToVal, err := sqlutil.DoltKeyAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}

		err = te.tableEditor.DeleteByKey(ctx, k, tagToVal)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.DeleteByKey(ctx, k, tagToVal)
		}
		return err
	} else {
		dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		err = te.tableEditor.DeleteRow(ctx, dRow)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.DeleteRow(ctx, dRow)
		}
		return err
	}
}

func (te *nomsTableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, oldRow, te.sch)
	if err != nil {
		return err
	}
	dNewRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, newRow, te.sch)
	if err != nil {
		return err
	}

	err = te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, te.duplicateKeyErrFunc)
	if sql.ErrForeignKeyNotResolved.Is(err) {
		if err = te.resolveFks(ctx); err != nil {
			return err
		}
		return te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, te.duplicateKeyErrFunc)
	}
	return err
}

func (te *nomsTableWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	nomsVal, err := te.autoIncCol.TypeInfo.ConvertValueToNomsValue(ctx, te.vrw, val)
	if err != nil {
		return err
	}
	if err = te.tableEditor.SetAutoIncrementValue(nomsVal); err != nil {
		return err
	}
	return nil
}

// StatementBegin implements the interface sql.TableEditor.
func (te *nomsTableWriter) StatementBegin(ctx *sql.Context) {
	te.tableEditor.StatementStarted(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (te *nomsTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return te.tableEditor.StatementFinished(ctx, true)
}

// StatementComplete implements the interface sql.TableEditor.
func (te *nomsTableWriter) StatementComplete(ctx *sql.Context) error {
	return te.tableEditor.StatementFinished(ctx, false)
}

// Close implements Closer
func (te *nomsTableWriter) Close(ctx *sql.Context) error {
	return nil
}

func (te *nomsTableWriter) resolveFks(ctx *sql.Context) error {
	panic("unimplemented")
}

func (te *nomsTableWriter) duplicateKeyErrFunc(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	panic("unimplemented")
}
