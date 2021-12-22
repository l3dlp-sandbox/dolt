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

type fkParentValidator struct {
	childIndex index.DoltIndex
	indexSch   sql.Schema
	fk         doltdb.ForeignKey

	// mapping from parent table to child FK index.
	parentMap columnMapping
	// mapping from child table to child FK index.
	childMap columnMapping

	childValidators []writeDependency
}

var _ writeDependency = fkParentValidator{}

func makeParentValidator(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey, child tableWriter) (writeDependency, error) {
	return nil, nil
}

func (v fkParentValidator) Insert(ctx *sql.Context, row sql.Row) (err error) {
	return
}

func (v fkParentValidator) Update(ctx *sql.Context, old, new sql.Row) error {
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

func (v fkParentValidator) Delete(ctx *sql.Context, row sql.Row) error {
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

func (v fkParentValidator) StatementBegin(ctx *sql.Context) {
	return
}

func (v fkParentValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (v fkParentValidator) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (v fkParentValidator) Close(ctx *sql.Context) (err error) {
	return
}

func (v fkParentValidator) parentColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(v.indexSch, v.parentMap, old, new)
}

func (v fkParentValidator) childIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, v.childIndex)

	for i, j := range v.parentMap {
		col := v.indexSch[i]
		expr := col.Source + "." + col.Name
		builder.Equals(ctx, expr, row[j])
	}

	return builder.Build(ctx)
}

func (v fkParentValidator) validateUpdateReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			for _, cv := range v.childValidators {
				if err = cv.Update(ctx, before, after); err != nil {
					return err
				}
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			after := childRowCascade(v.parentMap, v.childMap, parent, before)
			for _, cv := range v.childValidators {
				if err = cv.Update(ctx, before, after); err != nil {
					return err
				}
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v fkParentValidator) validateDeleteReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}

		switch v.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(v.childMap, before)
			for _, cv := range v.childValidators {
				if err = cv.Update(ctx, before, after); err != nil {
					return err
				}
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			for _, cv := range v.childValidators {
				if err = cv.Delete(ctx, before); err != nil {
					return err
				}
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (v fkParentValidator) violationErr(row sql.Row) error {
	// todo(andy): incorrect string for key
	s := sql.FormatRow(row)
	return sql.ErrForeignKeyParentViolation.New(v.fk.Name, v.fk.TableName, v.fk.ReferencedTableName, s)
}

type fkParentDependency struct {
	fk         doltdb.ForeignKey
	childIndex index.DoltIndex
	indexSch   sql.Schema

	// mapping from parent table to child FK index.
	parentMap columnMapping
	// mapping from child table to child FK index.
	childMap columnMapping

	childTable writeDependency
}

var _ writeDependency = fkParentDependency{}

func makeParentDependency(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey, child tableWriter) (writeDependency, error) {
	return nil, nil
}

func (d fkParentDependency) Insert(ctx *sql.Context, row sql.Row) (err error) {
	return
}

func (d fkParentDependency) Update(ctx *sql.Context, old, new sql.Row) error {
	if isRestrict(d.fk.OnUpdate) {
		return nil // handled by validator
	}
	if containsNulls(d.parentMap, new) {
		return nil
	}

	ok, err := d.parentColumnsUnchanged(old, new)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	lookup, err := d.childIndexLookup(ctx, old)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	return d.executeUpdateReferenceOption(ctx, new, iter)
}

func (d fkParentDependency) Delete(ctx *sql.Context, row sql.Row) error {
	if isRestrict(d.fk.OnUpdate) {
		return nil
	}
	if containsNulls(d.parentMap, row) {
		return nil
	}

	lookup, err := d.childIndexLookup(ctx, row)
	if err != nil {
		return err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup)
	if err != nil {
		return err
	}

	return d.executeDeleteReferenceOption(ctx, row, iter)
}

func (d fkParentDependency) StatementBegin(ctx *sql.Context) {
	return
}

func (d fkParentDependency) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	return
}

func (d fkParentDependency) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (d fkParentDependency) Close(ctx *sql.Context) (err error) {
	return
}

func (d fkParentDependency) parentColumnsUnchanged(old, new sql.Row) (bool, error) {
	return indexColumnsUnchanged(d.indexSch, d.parentMap, old, new)
}

func (d fkParentDependency) childIndexLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	builder := sql.NewIndexBuilder(ctx, d.childIndex)

	for i, j := range d.parentMap {
		col := d.indexSch[i]
		expr := col.Source + "." + col.Name
		builder.Equals(ctx, expr, row[j])
	}

	return builder.Build(ctx)
}

func (d fkParentDependency) executeUpdateReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}

		switch d.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(d.childMap, before)
			if err = d.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			after := childRowCascade(d.parentMap, d.childMap, parent, before)
			if err = d.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
}

func (d fkParentDependency) executeDeleteReferenceOption(ctx *sql.Context, parent sql.Row, iter sql.RowIter) error {
	for {
		before, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}

		switch d.fk.OnUpdate {
		case doltdb.ForeignKeyReferenceOption_SetNull:
			after := childRowSetNull(d.childMap, before)
			if err = d.childTable.Update(ctx, before, after); err != nil {
				return err
			}

		case doltdb.ForeignKeyReferenceOption_Cascade:
			if err = d.childTable.Delete(ctx, before); err != nil {
				return err
			}

		default:
			panic("unexpected reference option")
		}
	}
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
