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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// columnMapping defines a childMap from a table schema to an index schema.
// The ith entry in a columnMapping corresponds to the ith column of the
// index schema, and contains the array index of the corresponding
// table schema column.
type columnMapping []int

// fkChildValidator is a write-validator for tables with FOREIGN KEY
// constraints (ie "child" tables). Before new rows are added to a
// child table, it performs an index lookup on the parent table to
// validate that a corresponding row exists.
type fkChildValidator struct {
	fk          doltdb.ForeignKey
	parentIndex index.DoltIndex
	indexSch    sql.Schema

	// mapping from child table to parent FK index.
	childMap columnMapping
}

var _ writeDependency = fkChildValidator{}

func makeChildValidator(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey) (writeDependency, error) {
	return nil, nil
}

func (v fkChildValidator) Insert(ctx *sql.Context, row sql.Row) error {
	if containsNulls(v.childMap, row) {
		return nil
	}

	lookup, err := v.parentIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	rows, err := sql.RowIterToRows(ctx, iter)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		// todo(andy): incorrect string for key
		s := sql.FormatRow(row)
		return sql.ErrForeignKeyChildViolation.New(v.fk.Name, v.fk.TableName, v.fk.ReferencedTableName, s)
	}

	return nil
}

func (v fkChildValidator) Update(ctx *sql.Context, old, new sql.Row) error {
	ok, err := v.childColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	return v.Insert(ctx, new)
}

func (v fkChildValidator) Delete(ctx *sql.Context, row sql.Row) (err error) {
	return
}

func (v fkChildValidator) StatementBegin(ctx *sql.Context) {
	return
}

func (v fkChildValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (v fkChildValidator) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (v fkChildValidator) Close(ctx *sql.Context) (err error) {
	return
}

// childColumnsUnchanged returns true if the fields indexed by the foreign key are unchanged.
func (v fkChildValidator) childColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(v.indexSch, v.childMap, old, new)
}

func (v fkChildValidator) parentIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, v.parentIndex)

	for i, j := range v.childMap {
		col := v.indexSch[i]
		expr := col.Source + "." + col.Name
		builder.Equals(ctx, expr, row[j])
	}

	return builder.Build(ctx)
}
