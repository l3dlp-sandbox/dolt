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

type writeDependency interface {
	sql.RowReplacer
	sql.RowUpdater
	sql.RowInserter
	sql.RowDeleter
}

type primaryKeyValidator struct {
	idx index.DoltIndex
	sch sql.Schema
}

var _ writeDependency = primaryKeyValidator{}

func primaryKeyValidatorForTable(ctx *sql.Context, tbl *doltdb.Table) (writeDependency, error) {
	return nil, nil
}

func (pk primaryKeyValidator) Insert(ctx *sql.Context, row sql.Row) error {
	lookup, err := pk.pkIndexLookup(ctx, row)
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
		return sql.NewUniqueKeyErr(sql.FormatRow(row), true, rows[0])
	}

	return nil
}

func (pk primaryKeyValidator) Update(ctx *sql.Context, old, new sql.Row) (err error) {
	// assumes |old| and |new| have the same pk
	return
}

func (pk primaryKeyValidator) Delete(ctx *sql.Context, row sql.Row) (err error) {
	return
}

func (pk primaryKeyValidator) StatementBegin(ctx *sql.Context) {
	return
}

func (pk primaryKeyValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (pk primaryKeyValidator) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (pk primaryKeyValidator) Close(ctx *sql.Context) (err error) {
	return
}

func (pk primaryKeyValidator) pkIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, pk.idx)

	for i, col := range pk.sch {
		if col.PrimaryKey {
			expr := col.Source + "." + col.Name
			builder.Equals(ctx, expr, row[i])
		}
	}

	return builder.Build(ctx)
}
