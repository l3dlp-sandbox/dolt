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

type primaryKeyValidator struct {
	parent sql.Table
	idx    index.DoltIndex
	fk     doltdb.ForeignKey
}

var _ writeDependency = primaryKeyValidator{}

func primaryKeyValidatorForTable(ctx *sql.Context, tbl *doltdb.Table) (writeDependency, error) {
	return nil, nil
}

func (pk primaryKeyValidator) Insert(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (pk primaryKeyValidator) Update(ctx *sql.Context, old, new sql.Row) error {
	panic("unimplemented")
}

func (pk primaryKeyValidator) Delete(ctx *sql.Context, row sql.Row) error {
	panic("unimplemented")
}

func (pk primaryKeyValidator) StatementBegin(ctx *sql.Context) {
	panic("unimplemented")
}

func (pk primaryKeyValidator) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	panic("unimplemented")
}

func (pk primaryKeyValidator) StatementComplete(ctx *sql.Context) error {
	panic("unimplemented")
}

func (pk primaryKeyValidator) Close(ctx *sql.Context) error {
	panic("unimplemented")
}
