package db

import (
	"database/sql"
	"context"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db	*sql.DB
	tx  *sql.Tx

	classes		map [string]int64
	titles		map [string]int64
	last_message int64
	last_mdate	 time.Time

	insertClass		*sql.Stmt
	insertTitle		*sql.Stmt
	insertMessage	*sql.Stmt
	updateMessage	*sql.Stmt
}

var TXOPTIONS = sql.TxOptions{ Isolation: 0, ReadOnly: false }

func (db Database) enforceForeignKeys() error {
	var i int;
	err := db.db.QueryRow("PRAGMA foreign_keys;").Scan(&i)
	if err == sql.ErrNoRows {
		return errors.New("Running without FOREIGN_KEY is not supported and may corrupt your database.")
	} else if err != nil {
		return err
	}

	if i == 0 {
		_, err = db.db.Exec("PRAGMA foreign_keys = ON;")
		if err != nil {
			return err
		}
	}

	return nil
}

func (db Database) checkUserVersion() error {
	var i int;
	err := db.db.QueryRow("PRAGMA user_version;").Scan(&i)
	if err != nil {
		return err
	}

	switch (i) {
	case 0:
		_, err = db.db.Exec(createStmt)
		if err != nil {
			return err
		}
		_, err = db.db.Exec(fmt.Sprintf("PRAGMA user_version = %v;", USER_VERSION))
		return err
	case USER_VERSION:
		return nil
	default:
		return fmt.Errorf("Database version %v does not match internal version %v, aborting", i, USER_VERSION)
	}
}

func OpenDatabase(path string) (Database, error) {
	var db Database
	var err error

	db.classes = make(map[string]int64)
	db.titles = make(map[string]int64)

	db.db, err = sql.Open("sqlite3", path)
	if err != nil {
		return db, err
	}

	err = db.enforceForeignKeys()
	if err != nil {
		return db, err
	}

	err = db.checkUserVersion()
	if err != nil {
		return db, err
	}

	c_rows, err := db.db.Query("SELECT id, class FROM wm_class;")
	if err != nil {
		return db, err
	}
	defer c_rows.Close()

	for c_rows.Next() {
		var id int64
		var class string
		if err = c_rows.Scan(&id, &class); err != nil {
			return db, err
		}
		db.classes[class] = id
	}
	if err = c_rows.Err(); err != nil {
		return db, err
	}

	t_rows, err := db.db.Query("SELECT id, title FROM wm_title;")
	if err != nil {
		return db, err
	}
	defer t_rows.Close()

	for t_rows.Next() {
		var id int64
		var title string
		if err = t_rows.Scan(&id, &title); err != nil {
			return db, err
		}
		db.titles[title] = id
	}
	if err = c_rows.Err(); err != nil {
		return db, err
	}

	// db.insertClass, err = db.db.Prepare("INSERT OR IGNORE INTO wm_class (class) VALUES (?);")
	// if err != nil {
	// 	return db, err
	// }

	// db.insertTitle, err = db.db.Prepare("INSERT OR IGNORE INTO wm_title (title) VALUES (?);")
	// if err != nil {
	// 	return db, err
	// }

	// db.insertMessage, err = db.db.Prepare("INSERT OR REPLACE INTO messages (startTime, class, title) VALUES (?, ?, ?);")
	// if err != nil {
	// 	return db, err
	// }

	// db.updateMessage, err = db.db.Prepare("UPDATE messages SET endTime = ?, duration = endtime - starttime WHERE (id = ?);")
	// if err != nil {
	// 	return db, err
	// }

	return db, nil
}

func (db *Database) Insert(ctx context.Context, date time.Time, class string, title string) error {
	cid, ok := db.classes[class]
	if !ok {
		// res, err := db.insertClass.ExecContext(ctx, class)
		res, err := db.tx.ExecContext(ctx, "INSERT OR IGNORE INTO wm_class (class) VALUES (?);", class)
		if err != nil {
			return err
		}
		cid, err = res.LastInsertId()
		if err != nil {
			return err
		}
		db.classes[class] = cid
	}

	tid, ok := db.titles[title]
	if !ok {
		// res, err := db.insertTitle.ExecContext(ctx, title)
		res, err := db.tx.ExecContext(ctx, "INSERT OR IGNORE INTO wm_title (title) VALUES (?);", title)
		if err != nil {
			return err
		}
		tid, err = res.LastInsertId()
		if err != nil {
			return err
		}
		db.titles[title] = tid
	}

	// res, err := db.insertMessage.ExecContext(ctx, date.UTC().Unix(), cid, tid)
	res, err := db.tx.ExecContext(ctx, "INSERT OR REPLACE INTO messages (startTime, class, title) VALUES (?, ?, ?);", date.UTC().Unix(), cid, tid)
	if err != nil {
		return err
	}

	mid, err := res.LastInsertId()
	db.last_message = mid
	db.last_mdate = date

	return err
}

func (db *Database) Close() error {
	if db.last_message != 0 {
		_, err := db.db.Exec("UPDATE messages SET endTime = ?, duration = ?1 - starttime, killed = 1 WHERE (id = ?);", time.Now().UTC().Unix(), db.last_message)
		if err != nil {
			db.db.Close()
			return err
		}
	}
	return db.db.Close()
}

func (db *Database) Begin(ctx context.Context) (err error) {
	db.tx, err = db.db.BeginTx(ctx, &TXOPTIONS)
	return
}

func (db *Database) Commit() error {
	return db.tx.Commit()
}

func (db *Database) EndSegment(ctx context.Context, date time.Time) (err error) {
	if db.last_message != 0 && date.After(db.last_mdate) {
		// _, err = db.updateMessage.ExecContext(ctx, date.UTC().Unix(), db.last_message)
		_, err = db.tx.ExecContext(ctx, "UPDATE messages SET endTime = ?, duration = ?1 - starttime WHERE (id = ?);", date.UTC().Unix(), db.last_message)
		db.last_message = 0
	}
	return
}
