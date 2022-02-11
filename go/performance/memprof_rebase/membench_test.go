package memprof_rebase

import (
	"context"
	"flag"
	"fmt"
	"log"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var loc = flag.String("doltDir", "", "Directory of dolt database")

func BenchmarkRebaseMemory(b *testing.B) {
	if *loc == "" {
		log.Fatalf("doltDir must be specified")
	}
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		urlStr := "file://" + *loc + dbfactory.DoltDataDir
		doltdb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr, filesys.LocalFS)
		if err != nil {
			log.Fatalf("failed to load doltdb, err: %s", err.Error())
		}

		err = doltdb.Rebase(ctx)
		if err != nil {
			log.Fatalf(fmt.Sprintf("failed to rebase, err: %s", err.Error()))
		}
	}
}
