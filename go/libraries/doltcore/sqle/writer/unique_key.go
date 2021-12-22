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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// uniqueKey enforces Unique Key constraints.
// todo(andy): it should also maintain the uniqueIndex
type uniqueKey struct {
	uniqueIndex index.DoltIndex
	indexMap    columnMapping
	expr        []sql.ColumnExpressionType
}

var _ writeDependency = uniqueKey{}

func uniqueKeysFromTable(ctx context.Context, db, table string, t *doltdb.Table, sch schema.Schema) (deps []writeDependency, err error) {
	err = sch.Indexes().Iter(func(idx schema.Index) (stop bool, err error) {
		if !idx.IsUnique() {
			return
		}

		dep, err := makeUniqueKey(ctx, db, table, t, sch, idx)
		if err != nil {
			return false, err
		}
		deps = append(deps, dep)

		return
	})
	if err != nil {
		return nil, err
	}

	return deps, nil
}

func makeUniqueKey(ctx context.Context, db, table string, t *doltdb.Table, sch schema.Schema, idx schema.Index) (writeDependency, error) {
	i, err := index.GetSecondaryIndex(ctx, db, table, t, sch, idx)
	if err != nil {
		return nil, err
	}

	expr := i.ColumnExpressionTypes(nil) // todo(andy)
	indexMap := indexMapForIndex(sch, idx)

	return uniqueKey{
		uniqueIndex: i,
		indexMap:    indexMap,
		expr:        expr,
	}, nil
}

func indexMapForIndex(sch schema.Schema, idx schema.Index) (mapping columnMapping) {
	indexTags := idx.IndexedColumnTags()
	mapping = make(columnMapping, len(indexTags))
	cols := sch.GetAllCols().GetColumns()

	for i, col := range cols {
		for j, tag := range indexTags {
			if col.Tag == tag {
				mapping[j] = i
				break
			}
		}
	}
	return
}

func (uk uniqueKey) ValidateInsert(ctx *sql.Context, row sql.Row) error {
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

func (uk uniqueKey) Insert(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (uk uniqueKey) ValidateUpdate(ctx *sql.Context, old, new sql.Row) error {
	ok, err := uk.uniqueColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	return uk.Insert(ctx, new)
}

func (uk uniqueKey) Update(ctx *sql.Context, old, new sql.Row) error {
	return nil
}

func (uk uniqueKey) ValidateDelete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (uk uniqueKey) Delete(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (uk uniqueKey) Close(ctx *sql.Context) (err error) {
	return
}

func (uk uniqueKey) uniqueColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(uk.expr, uk.indexMap, old, new)
}

func (uk uniqueKey) uniqueIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, uk.uniqueIndex)

	for i, j := range uk.indexMap {
		builder.Equals(ctx, uk.expr[i].Expression, row[j])
	}

	return builder.Build(ctx)
}

func indexColumnsUnchanged(expr []sql.ColumnExpressionType, indexMap columnMapping, old, new sql.Row) (bool, error) {
	for i, j := range indexMap {
		cmp, err := expr[i].Type.Compare(old[j], new[j])
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

// todo(andy): the following functions are deprecated

func (uk uniqueKey) StatementBegin(ctx *sql.Context) {
	return
}

func (uk uniqueKey) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (uk uniqueKey) StatementComplete(ctx *sql.Context) (err error) {
	return
}
