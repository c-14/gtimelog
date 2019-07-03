package cmd

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"encoding/csv"
	"os/signal"

	"github.com/c-14/gtimelog/db"
)

func Store(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: gtimelog store <output_db>")
	}
	var dbPath string = args[0]

	base, err := db.OpenDatabase(dbPath)
	if err != nil {
		return err
	}
	defer base.Close()

	s := bufio.NewReader(os.Stdin)
	cr := csv.NewReader(s)
	cr.FieldsPerRecord = 3
	cr.LazyQuotes = true
	cr.ReuseRecord = true

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if base.Begin(ctx) != nil {
		return err
	}
	defer base.Commit()

	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var numInsert = 0
	for {
		select {
		case <-sigs:
			return nil
		default:
		}
		record, err := cr.Read()
		if err != nil {
			switch err.(type) {
			case *csv.ParseError:
				if err.(*csv.ParseError).Err == csv.ErrFieldCount {
					continue
				}
			default:
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		date, err := time.Parse("2006-01-02 15:04:05 MST", record[0])
		if err != nil {
			return err
		}

		err = base.EndSegment(ctx, date)
		if err != nil {
			return err
		}

		err = base.Insert(ctx, date, record[1], record[2])
		if err != nil {
			return err
		}

		numInsert++ 
		if numInsert > 100 {
			base.Commit()
			if base.Begin(ctx) != nil {
				return err
			}
		}
	}
}
