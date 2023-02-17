// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"strings"
	"unsafe"

	"github.com/alexbrainman/odbc/api"
)

type Conn struct {
	h                api.SQLHDBC
	tx               *Tx
	bad              bool
	isMSAccessDriver bool
}

var accessDriverSubstr = strings.ToUpper(strings.Replace("DRIVER={Microsoft Access Driver", " ", "", -1))

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	if d.initErr != nil {
		return nil, d.initErr
	}

	var out api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_DBC, api.SQLHANDLE(d.h), &out)
	if IsError(ret) {
		return nil, NewError("SQLAllocHandle", d.h)
	}
	h := api.SQLHDBC(out)
	drv.Stats.updateHandleCount(api.SQL_HANDLE_DBC, 1)

	b := api.StringToUTF16(dsn)
	ret = api.SQLDriverConnect(h, 0,
		(*api.SQLWCHAR)(unsafe.Pointer(&b[0])), api.SQL_NTS,
		nil, 0, nil, api.SQL_DRIVER_NOPROMPT)
	if IsError(ret) {
		defer releaseHandle(h)
		return nil, NewError("SQLDriverConnect", h)
	}
	isAccess := strings.Contains(strings.ToUpper(strings.Replace(dsn, " ", "", -1)), accessDriverSubstr)
	return &Conn{h: h, isMSAccessDriver: isAccess}, nil
}

func (c *Conn) Close() (err error) {
	if c.tx != nil {
		c.tx.Rollback()
	}
	h := c.h
	defer func() {
		c.h = api.SQLHDBC(api.SQL_NULL_HDBC)
		e := releaseHandle(h)
		if err == nil {
			err = e
		}
	}()
	ret := api.SQLDisconnect(c.h)
	if IsError(ret) {
		return c.newError("SQLDisconnect", h)
	}
	return err
}

func (c *Conn) newError(apiName string, handle interface{}) error {
	err := NewError(apiName, handle)
	if err == driver.ErrBadConn {
		c.bad = true
	}
	return err
}

func (c *Conn) Tables(catalog string, schema string, table string) (*Rows, error) {
	var out api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, api.SQLHANDLE(c.h), &out)
	if IsError(ret) {
		return nil, c.newError("SQLAllocHandle", c.h)
	}
	h := api.SQLHSTMT(out)
	err := drv.Stats.updateHandleCount(api.SQL_HANDLE_STMT, 1)
	if err != nil {
		return nil, err
	}

	var catalogP *api.SQLWCHAR
	if catalog != "" {
		catalogB := api.StringToUTF16(catalog)
		catalogP = (*api.SQLWCHAR)(unsafe.Pointer(&catalogB[0]))
	}

	var schemaP *api.SQLWCHAR
	if schema != "" {
		schemaB := api.StringToUTF16(schema)
		schemaP = (*api.SQLWCHAR)(unsafe.Pointer(&schemaB[0]))
	}

	var tableP *api.SQLWCHAR
	if table != "" {
		tableB := api.StringToUTF16(table)
		tableP = (*api.SQLWCHAR)(unsafe.Pointer(&tableB[0]))
	}

	ret = api.SQLTables(
		h,
		catalogP, api.SQL_NTS,
		schemaP, api.SQL_NTS,
		tableP, api.SQL_NTS)
	if IsError(ret) {
		defer releaseHandle(h)
		return nil, NewError("SQLTables", h)
	}

	os := &ODBCStmt{
		h: h,
		usedByStmt: true,
	}

	err = os.BindColumns()
	if err != nil {
		return nil, err
	}

	return &Rows{os: os}, nil
}

func (c *Conn) Columns(catalog string, schema string, table string, column string) (*Rows, error) {
	var out api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, api.SQLHANDLE(c.h), &out)
	if IsError(ret) {
		return nil, c.newError("SQLAllocHandle", c.h)
	}
	h := api.SQLHSTMT(out)
	err := drv.Stats.updateHandleCount(api.SQL_HANDLE_STMT, 1)
	if err != nil {
		return nil, err
	}

	var catalogP *api.SQLWCHAR
	if catalog != "" {
		catalogB := api.StringToUTF16(catalog)
		catalogP = (*api.SQLWCHAR)(unsafe.Pointer(&catalogB[0]))
	}

	var schemaP *api.SQLWCHAR
	if schema != "" {
		schemaB := api.StringToUTF16(schema)
		schemaP = (*api.SQLWCHAR)(unsafe.Pointer(&schemaB[0]))
	}

	var tableP *api.SQLWCHAR
	if table != "" {
		tableB := api.StringToUTF16(table)
		tableP = (*api.SQLWCHAR)(unsafe.Pointer(&tableB[0]))
	}

	var columnP *api.SQLWCHAR
	if column != "" {
		columnB := api.StringToUTF16(column)
		columnP = (*api.SQLWCHAR)(unsafe.Pointer(&columnB[0]))
	}

	ret = api.SQLColumns(
		h,
		catalogP, api.SQL_NTS,
		schemaP, api.SQL_NTS,
		tableP, api.SQL_NTS,
		columnP, api.SQL_NTS)
	if IsError(ret) {
		defer releaseHandle(h)
		return nil, NewError("SQLColumns", h)
	}

	os := &ODBCStmt{
		h: h,
		usedByStmt: true,
	}

	err = os.BindColumns()
	if err != nil {
		return nil, err
	}

	return &Rows{os: os}, nil
}
