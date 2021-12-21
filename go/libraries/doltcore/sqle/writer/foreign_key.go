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

func FkDependenciesForTable(ctx *sql.Context, root *doltdb.RootValue, table string) (validators, dependencies []writeDependency, err error) {
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}

	parentFKs, childFks := fkc.KeysForTable(table)

	for _, pfk := range parentFKs {
		v, err := parentValidatorFromFk(ctx, root, pfk)
		if err != nil {
			return nil, nil, err
		}
		validators = append(validators, v)
	}

	for _, cfk := range childFks {
		d, err := parentValidatorFromFk(ctx, root, cfk)
		if err != nil {
			return nil, nil, err
		}
		dependencies = append(dependencies, d)
	}

	return
}

type fkParentValidator struct {
	parent sql.Table
	idx    index.DoltIndex
	fk     doltdb.ForeignKey
}

var _ writeDependency = fkParentValidator{}

func parentValidatorFromFk(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey) (writeDependency, error) {
	return nil, nil
}

func (pv fkParentValidator) Insert(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (pv fkParentValidator) Update(ctx *sql.Context, old, new sql.Row) error {
	panic("unimplemented")
}

func (pv fkParentValidator) Delete(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (pv fkParentValidator) StatementBegin(ctx *sql.Context) {
	panic("unimplemented")
}

func (pv fkParentValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	panic("unimplemented")
}

func (pv fkParentValidator) StatementComplete(ctx *sql.Context) error {
	panic("unimplemented")
}

func (pv fkParentValidator) Close(ctx *sql.Context) error {
	panic("unimplemented")
}

type fkChildDependency struct {
	child sql.Table
	idx   index.DoltIndex
	fk    doltdb.ForeignKey
}

var _ writeDependency = fkChildDependency{}

func childDependencyFromFk(ctx *sql.Context, root *doltdb.RootValue, fk doltdb.ForeignKey) (writeDependency, error) {
	return nil, nil
}

func (cd fkChildDependency) Insert(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (cd fkChildDependency) Update(ctx *sql.Context, old, new sql.Row) error {
	panic("unimplemented")
}

func (cd fkChildDependency) Delete(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (cd fkChildDependency) StatementBegin(ctx *sql.Context) {
	panic("unimplemented")
}

func (cd fkChildDependency) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	panic("unimplemented")
}

func (cd fkChildDependency) StatementComplete(ctx *sql.Context) error {
	panic("unimplemented")
}

func (cd fkChildDependency) Close(ctx *sql.Context) error {
	panic("unimplemented")
}
