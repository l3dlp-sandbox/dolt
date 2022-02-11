package memprof_rebase

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var loc = flag.String("doltDir", "", "Directory of dolt database")
var ddb *doltdb.DoltDB

func BenchmarkRebaseMemory(b *testing.B) {
	if *loc == "" {
		b.Fatalf("doltDir must be specified")
	}
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		urlStr := "file://" + *loc + dbfactory.DoltDataDir
		ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr, filesys.LocalFS)
		if err != nil {
			b.Fatalf("failed to load doltdb, err: %s", err.Error())
		}

		PrintMemUsage("Before: ")
		err = ddb.Rebase(ctx)
		PrintMemUsage("After: ")
		if err != nil {
			b.Fatalf(fmt.Sprintf("failed to rebase, err: %s", err.Error()))
		}
	}
}

func PrintMemUsage(pre string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf(pre)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
