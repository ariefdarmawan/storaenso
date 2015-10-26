package duplicatematl

import (
	//"github.com/eaciit/database/base"
	//"github.com/eaciit/database/mongodb"
	"fmt"
	//"github.com/eaciit/orm"
	"github.com/eaciit/pque"
	"github.com/eaciit/toolkit"
	//"os"
)

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

func FindSimilarity_1(sources []toolkit.M) []toolkit.M {
	result := make([]toolkit.M, 0)

	pqs := pque.NewQue()
	pqs.WorkerCount = 100

	pqs.Fn = func(in interface{}) interface{} {
		s := in.(toolkit.M)
		sname := s["Trimmed"].(string)
		fmt.Printf("Received job for %v - %s \n", s["_id"], s["Trimmed"])
		for _, s1 := range sources {
			//-- do nothing
			s1name := s1["Trimmed"].(string)
			if s1name == sname {
				//-- do something
			}
		}
		return s
	}
	pqs.FnDone = func(in interface{}) {
		s := in.(toolkit.M)
		fmt.Printf("Complete job for %v - %s. %d of %d \n", s["_id"], s["Trimmed"], pqs.CompletedJob, pqs.PreparedJob)
	}

	pqs.WaitForKeys()
	for _, s := range sources {
		pqs.SendKey(s)
	}
	pqs.KeySendDone()
	pqs.WaitForCompletion()
	return result
}
