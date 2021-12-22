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

	ValidateInsert(ctx *sql.Context, row sql.Row) error
	ValidateUpdate(ctx *sql.Context, old, new sql.Row) error
	ValidateDelete(ctx *sql.Context, row sql.Row) error
}

// primaryKey enforces Primary Key constraints.
// todo(andy): it should also maintain the PK index
type primaryKey struct {
	idx index.DoltIndex
	sch sql.Schema
}

var _ writeDependency = primaryKey{}

func primaryKeyValidatorForTable(ctx *sql.Context, tbl *doltdb.Table) (writeDependency, error) {
	return nil, nil
}

func (pk primaryKey) ValidateInsert(ctx *sql.Context, row sql.Row) error {
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

func (pk primaryKey) Insert(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (pk primaryKey) ValidateUpdate(ctx *sql.Context, old, new sql.Row) error {
	// assumes |old| and |new| have the same pk nil
	return nil
}

func (pk primaryKey) Update(ctx *sql.Context, old, new sql.Row) error {
	return nil
}

func (pk primaryKey) ValidateDelete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (pk primaryKey) Delete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (pk primaryKey) Close(ctx *sql.Context) error {
	return nil
}

func (pk primaryKey) pkIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, pk.idx)

	for i, col := range pk.sch {
		if col.PrimaryKey {
			expr := col.Source + "." + col.Name
			builder.Equals(ctx, expr, row[i])
		}
	}

	return builder.Build(ctx)
}

// todo(andy): the following functions are deprecated

func (pk primaryKey) StatementBegin(ctx *sql.Context) {
	return
}

func (pk primaryKey) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

func (pk primaryKey) StatementComplete(ctx *sql.Context) error {
	return nil
}
