package duplicatematl

import (
	"fmt"
	"github.com/eaciit/database/base"
	"github.com/eaciit/database/mongodb"
	"github.com/eaciit/orm"
	"os"
)

var conn base.IConnection
var ctx *orm.DataContext

func DbConn() base.IConnection {
	if conn == nil {
		conn = mongodb.NewConnection("localhost:27123", "", "", "ecstora")
		e := conn.Connect()
		if e != nil {
			fmt.Println("Unable to connect database. " + e.Error())
			os.Exit(100)
		}
	}

	return conn
}

func DbCtx() *orm.DataContext {
	conn := DbConn()
	if ctx == nil {
		ctx = orm.New(conn)
	}
	//fmt.Printf("Init ctx. Conn: %v \n", conn)
	return ctx
}
