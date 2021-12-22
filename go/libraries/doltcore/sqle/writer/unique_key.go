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

type uniqueKeyValidator struct {
	index    index.DoltIndex
	indexSch sql.Schema
	indexMap columnMapping
}

var _ writeDependency = uniqueKeyValidator{}

func uniqueKeyValidatorForTable(ctx *sql.Context, tbl *doltdb.Table) (writeDependency, error) {
	return nil, nil
}

func (uk uniqueKeyValidator) Insert(ctx *sql.Context, row sql.Row) error {
	if containsNulls(uk.indexMap, row) {
		return nil
	}

	lookup, err := uk.uniqueIndexLookup(ctx, row)
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

func (uk uniqueKeyValidator) Update(ctx *sql.Context, old, new sql.Row) error {
	ok, err := uk.uniqueColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	return uk.Insert(ctx, new)
}

func (uk uniqueKeyValidator) Delete(ctx *sql.Context, row sql.Row) (err error) {
	return
}

func (uk uniqueKeyValidator) StatementBegin(ctx *sql.Context) {
	return
}

func (uk uniqueKeyValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (uk uniqueKeyValidator) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (uk uniqueKeyValidator) Close(ctx *sql.Context) (err error) {
	return
}

func (uk uniqueKeyValidator) uniqueColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(uk.indexSch, uk.indexMap, old, new)
}

func (uk uniqueKeyValidator) uniqueIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, uk.index)

	for i, j := range uk.indexMap {
		col := uk.indexSch[i]
		expr := col.Source + "." + col.Name
		builder.Equals(ctx, expr, row[j])
	}

	return builder.Build(ctx)
}

func indexColumnsUnchanged(indexSch sql.Schema, indexMap columnMapping, old, new sql.Row) (bool, error) {
	for i, j := range indexMap {
		col := indexSch[i]

		cmp, err := col.Type.Compare(old[j], new[j])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func containsNulls(mapping columnMapping, row sql.Row) bool {
	for _, j := range mapping {
		if row[j] == nil {
			return true
		}
	}
	return false
}
