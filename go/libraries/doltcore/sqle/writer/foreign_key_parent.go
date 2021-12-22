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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// foreignKeyParent enforces the parent side of a Foreign Key
// constraint, and executes reference option logic.
// It does not maintain the Foreign Key index.
type foreignKeyParent struct {
	childIndex index.DoltIndex
	indexSch   sql.Schema
	fk         doltdb.ForeignKey

	// mapping from parent table to child FK index.
	parentMap columnMapping
	// mapping from child table to child FK index.
	childMap columnMapping

	childTable writeDependency
}

var _ writeDependency = foreignKeyParent{}

func makeParentValidator(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey, child tableWriter) (writeDependency, error) {
	return nil, nil
}

func (v foreignKeyParent) ValidateInsert(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (v foreignKeyParent) Insert(ctx *sql.Context, row sql.Row) error {
	return nil
}

func (v foreignKeyParent) ValidateUpdate(ctx *sql.Context, old, new sql.Row) error {
	ok, err := v.parentColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	lookup, err := v.childIndexLookup(ctx, new)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	if isRestrict(v.fk.OnUpdate) {
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			return v.violationErr(new)
		}
	}

	return v.validateUpdateReferenceOption(ctx, new, iter)
}

func (v foreignKeyParent) validateUpdateReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			if err = v.childTable.ValidateUpdate(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			after := childRowCascade(v.parentMap, v.childMap, parent, before)
			if err = v.childTable.ValidateUpdate(ctx, before, after); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v foreignKeyParent) Update(ctx *sql.Context, old, new sql.Row) error {
	if isRestrict(v.fk.OnUpdate) {
		return nil // handled by validator
	}
	if containsNulls(v.parentMap, new) {
		return nil
	}

	ok, err := v.parentColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	lookup, err := v.childIndexLookup(ctx, old)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	return v.executeUpdateReferenceOption(ctx, new, iter)
}

func (v foreignKeyParent) executeUpdateReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			if err = v.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			after := childRowCascade(v.parentMap, v.childMap, parent, before)
			if err = v.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v foreignKeyParent) ValidateDelete(ctx *sql.Context, row sql.Row) error {
	if containsNulls(v.parentMap, row) {
		return nil
	}

	lookup, err := v.childIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	if isRestrict(v.fk.OnDelete) {
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			return v.violationErr(row)
		}
	}

	return v.validateDeleteReferenceOption(ctx, row, iter)
}

func (v foreignKeyParent) validateDeleteReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			if err = v.childTable.ValidateUpdate(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			if err = v.childTable.ValidateDelete(ctx, before); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v foreignKeyParent) Delete(ctx *sql.Context, row sql.Row) error {
	if isRestrict(v.fk.OnUpdate) {
		return nil
	}
	if containsNulls(v.parentMap, row) {
		return nil
	}

	lookup, err := v.childIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	return v.executeDeleteReferenceOption(ctx, row, iter)
}

func (v foreignKeyParent) executeDeleteReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			if err = v.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			if err = v.childTable.Delete(ctx, before); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v foreignKeyParent) Close(ctx *sql.Context) error {
	return nil
}

func (v foreignKeyParent) parentColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(v.indexSch, v.parentMap, old, new)
}

func (v foreignKeyParent) childIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, v.childIndex)

	for i, j := range v.parentMap {
		col := v.indexSch[i]
		expr := col.Source + "." + col.Name
		builder.Equals(ctx, expr, row[j])
	}

	return builder.Build(ctx)
}

func (v foreignKeyParent) violationErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyParentViolation.New(v.fk.Name, v.fk.TableName, v.fk.ReferencedTableName, s)
}

func isRestrict(opt doltdb.ForeignKeyReferenceOption) bool {
	return opt == doltdb.ForeignKeyReferenceOption_DefaultAction ||
		opt == doltdb.ForeignKeyReferenceOption_NoAction ||
		opt == doltdb.ForeignKeyReferenceOption_Restrict
}

func childRowSetNull(childMap columnMapping, child sql.Row) (updated sql.Row) {
	updated = child.Copy()
	for _, j := range childMap {
		updated[j] = nil
	}
	return
}

func childRowCascade(parentMap, childMap columnMapping, parent, child sql.Row) (updated sql.Row) {
	updated = child.Copy()
	for i := range parentMap {
		pi, ci := parentMap[i], childMap[i]
		child[ci] = parent[pi]
	}
	return
}

// todo(andy): the following functions are deprecated

func (v foreignKeyParent) StatementBegin(ctx *sql.Context) {
	return
}

func (v foreignKeyParent) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

func (v foreignKeyParent) StatementComplete(ctx *sql.Context) error {
	return nil
}
