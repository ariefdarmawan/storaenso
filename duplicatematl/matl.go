package duplicatematl

import (
	//"github.com/eaciit/database/base"
	//"github.com/eaciit/database/mongodb"
	//"fmt"
	"github.com/eaciit/orm"
	"github.com/eaciit/toolkit"
	//"os"
)

type Material struct {
	orm.ModelBase
	Id          string `bson"_id`
	Description string
	IsRootItem  bool
	Status      string
	IdOrig      string
}

func PopulateMaterial() []toolkit.M {
	mats := make([]toolkit.M, 0)
	csr := DbCtx().Connection.Query().From("MaintenancePart").Select("_id", "Matnr", "Trimmed").Cursor(nil)
	csr.SetPooling(true)
	//var b bool

	//fmt.Println("Populate data")
	//var _ error
	/*
		m := new(toolkit.M)
		b = true
		iRead := 0
		for ; b == true; b, _ = csr.Fetch(&m) {
			iRead++
			mats = append(mats, *m)
			//fmt.Printf("Reading %d data \n", iRead)
		}
	*/
	csr.FetchAll(&mats, false)
	return mats
}
