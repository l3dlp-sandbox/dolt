package memprof_rebase

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var loc = flag.String("f", "", "Directory of dolt database")

func TestMain(m *testing.M) {
	if *loc == "" {
		log.Fatalf("f must be specified")
	}
	code := m.Run()
	os.Exit(code)
}

func BenchmarkRebaseMemory(b *testing.B) {
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
