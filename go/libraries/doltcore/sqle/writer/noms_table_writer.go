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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

type nomsTableWriter struct {
	tableName   string
	dbName      string
	sch         schema.Schema
	autoIncCol  schema.Column
	vrw         types.ValueReadWriter
	kvToSQLRow  *index.KVToSqlRowConverter
	tableEditor editor.TableEditor
}

func makeNomsTableWriter(ctx context.Context, root *doltdb.RootValue, table, database string, opts editor.Options) (nw nomsTableWriter, err error) {
	t, ok, err := root.GetTable(ctx, table)
	if err != nil {
		return nw, err
	}
	if !ok {
		return nw, fmt.Errorf("unable to create table editor as `%s` is missing", table)
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nw, err
	}

	tableEditor, err := editor.NewTableEditor(ctx, t, sch, table, opts)
	if err != nil {
		return nw, err
	}

	autoCol := autoIncrementColFromSchema(sch)
	conv := index.NewKVToSqlRowConverterForCols(t.Format(), sch)

	nw = nomsTableWriter{
		tableName:   table,
		dbName:      database,
		sch:         sch,
		autoIncCol:  autoCol,
		vrw:         t.ValueReadWriter(),
		kvToSQLRow:  conv,
		tableEditor: tableEditor,
	}

	return nw, nil
}

var _ sql.AutoIncrementSetter = &nomsTableWriter{}

func (te nomsTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
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

func (te nomsTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
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

func (te nomsTableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
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

func (te nomsTableWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
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
func (te nomsTableWriter) StatementBegin(ctx *sql.Context) {
	te.tableEditor.StatementStarted(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (te nomsTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return te.tableEditor.StatementFinished(ctx, true)
}

// StatementComplete implements the interface sql.TableEditor.
func (te nomsTableWriter) StatementComplete(ctx *sql.Context) error {
	return te.tableEditor.StatementFinished(ctx, false)
}

func (te nomsTableWriter) Table(ctx context.Context) (*doltdb.Table, error) {
	return te.tableEditor.Table(ctx)
}

// Close implements Closer
func (te nomsTableWriter) Close(ctx *sql.Context) error {
	return nil
}

func (te nomsTableWriter) resolveFks(ctx *sql.Context) error {
	return nil // todo(andy)
}

func (te nomsTableWriter) duplicateKeyErrFunc(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	panic("unimplemented")
}
