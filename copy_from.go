package pgx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgio"
)

// CopyFromRows returns a CopyFromSource interface over the provided rows slice
// making it usable by *Conn.CopyFrom.
func CopyFromRows(rows [][]interface{}) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

type copyFromRows struct {
	rows [][]interface{}
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

// CopyFromSlice returns a CopyFromSource interface over a dynamic func
// making it usable by *Conn.CopyFrom.
func CopyFromSlice(length int, next func(int) ([]interface{}, error)) CopyFromSource {
	return &copyFromSlice{next: next, idx: -1, len: length}
}

type copyFromSlice struct {
	next func(int) ([]interface{}, error)
	idx  int
	len  int
	err  error
}

func (cts *copyFromSlice) Next() bool {
	cts.idx++
	return cts.idx < cts.len
}

func (cts *copyFromSlice) Values() ([]interface{}, error) {
	values, err := cts.next(cts.idx)
	if err != nil {
		cts.err = err
	}
	return values, err
}

func (cts *copyFromSlice) Err() error {
	return cts.err
}

// CopyFromSource is the interface used by *Conn.CopyFrom as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]interface{}, error)

	// Err returns any error that has been encountered by the CopyFromSource. If
	// this is not nil *Conn.CopyFrom will abort the copy.
	Err() error
}

type copyFrom struct {
	conn          *Conn
	tableName     Identifier
	columnNames   []string
	rowSrc        CopyFromSource
	readerErrChan chan error
}

func (ct *copyFrom) run(ctx context.Context) (int64, error) {
	quotedTableName := ct.tableName.Sanitize()
	cbuf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	sd, err := ct.conn.Prepare(ctx, "", fmt.Sprintf("select %s from %s", quotedColumnNames, quotedTableName))
	if err != nil {
		return 0, err
	}

	r, w := io.Pipe()
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)

		// Purposely NOT using defer w.Close(). See https://github.com/golang/go/issues/24283.
		buf := ct.conn.wbuf

		buf = append(buf, "PGCOPY\n\377\r\n\000"...)
		buf = pgio.AppendInt32(buf, 0)
		buf = pgio.AppendInt32(buf, 0)
		fmt.Printf("buf is %v\n", buf)
		fmt.Printf("buf as string is %v\n", string(buf))

		moreRows := true
		for moreRows {
			var err error
			moreRows, buf, err = ct.buildCopyBuf(buf, sd)
			fmt.Printf("\nmoreRows %v, buf %v, err %v\n", moreRows, buf, err)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			fmt.Printf("ct.rowSrc.Err() is %v\n", ct.rowSrc.Err())
			if ct.rowSrc.Err() != nil {
				w.CloseWithError(ct.rowSrc.Err())
				return
			}

			if len(buf) > 0 {
				fmt.Printf("writing buf....\n")
				_, err = w.Write(buf)
				fmt.Printf("write buf error is %v\n", err)
				if err != nil {
					fmt.Printf("going to close...\n")
					w.Close()
					fmt.Printf("close finished...\n")
					return
				}
			}

			fmt.Printf("reset buf\n")
			buf = buf[:0]
		}

		fmt.Printf("closing...\n")
		w.Close()
		fmt.Printf("closing done...\n")
	}()

	startTime := time.Now()

	copyFromCommand := fmt.Sprintf("copy %s ( %s ) from stdin binary;\n", quotedTableName, quotedColumnNames)
	fmt.Printf("copyFromCommand is %s\n", copyFromCommand)
	commandTag, err := ct.conn.pgConn.CopyFrom(ctx, r, copyFromCommand)

	r.Close()
	<-doneChan

	rowsAffected := commandTag.RowsAffected()
	endTime := time.Now()
	if err == nil {
		if ct.conn.shouldLog(LogLevelInfo) {
			ct.conn.log(ctx, LogLevelInfo, "CopyFrom", map[string]interface{}{"tableName": ct.tableName, "columnNames": ct.columnNames, "time": endTime.Sub(startTime), "rowCount": rowsAffected})
		}
	} else if ct.conn.shouldLog(LogLevelError) {
		ct.conn.log(ctx, LogLevelError, "CopyFrom", map[string]interface{}{"err": err, "tableName": ct.tableName, "columnNames": ct.columnNames, "time": endTime.Sub(startTime)})
	}

	return rowsAffected, err
}

func (ct *copyFrom) buildCopyBuf(buf []byte, sd *pgconn.StatementDescription) (bool, []byte, error) {
	log.Printf("starting build copy buf, %v %v\n", buf, *sd)
	for ct.rowSrc.Next() {
		values, err := ct.rowSrc.Values()
		if err != nil {
			return false, nil, err
		}

		log.Printf("values %v err %v\n", values, err)
		if len(values) != len(ct.columnNames) {
			return false, nil, fmt.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		buf = pgio.AppendInt16(buf, int16(len(ct.columnNames)))
		for i, val := range values {
			log.Printf("\nstarting encode prepared statement for field %v with val %v index %v with DataTypeOID %v\n", string(sd.Fields[i].Name), val, i, sd.Fields[i].DataTypeOID)
			lenBufBefore := len(buf)
			expectedBufChange := sd.Fields[i].DataTypeSize
			buf, err = encodePreparedStatementArgument(ct.conn.connInfo, buf, sd.Fields[i].DataTypeOID, val)
			lenBufChange := len(buf) - lenBufBefore
			log.Printf("\nbuf is %v, err is %v\n\n", buf, err)
			log.Printf("expected buf change is %v, actual change is %v\n", expectedBufChange, lenBufChange)
			log.Printf("len before is %v len after is %v", lenBufBefore, lenBufChange)
			log.Printf("new bytes added %v", buf[lenBufBefore:lenBufChange])
			log.Printf("new bytes as string %v", string(buf[lenBufBefore:lenBufChange]))
			if err != nil {
				return false, nil, err
			}
		}

		if len(buf) > 65536 {
			return true, buf, nil
		}
	}

	return false, buf, nil
}

// CopyFrom uses the PostgreSQL copy protocol to perform bulk data insertion.
// It returns the number of rows copied and an error.
//
// CopyFrom requires all values use the binary format. Almost all types
// implemented by pgx use the binary format by default. Types implementing
// Encoder can only be used if they encode to the binary format.
func (c *Conn) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	ct := &copyFrom{
		conn:          c,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
	}

	return ct.run(ctx)
}
