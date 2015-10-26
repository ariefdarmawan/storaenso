package main

import (
	"fmt"
	"github.com/eaciit/pque"
	tk "github.com/eaciit/toolkit"
	. "github.com/juragan360/storaenso/duplicatematl"
	"time"
)

func main() {
	tStart := time.Now()
	defer DbCtx().Close()

	fmt.Println("Material Duplicate Detector")
	fmt.Println("v0.8")
	fmt.Println("")

	tPopulate := time.Now()
	fmt.Print("1. Populate master material ... ")
	mats := PopulateMaterial()
	fmt.Printf("Done (%v). %d records has been populated \n",
		time.Since(tPopulate), len(mats))

	tBuildIndex := time.Now()
	var matrefs []tk.M
	copy(mats, matrefs)

	byId := make(map[string][]string, 0)
	byNames := make(map[string][]string, 0)

	exactNameCount := 0
	fmt.Print("2. Build index ... ")
	for _, matl := range mats {
		matName := matl["Trimmed"].(string)
		matId := matl["Matnr"].(string)
		if _, byNameExist := byNames[matName]; !byNameExist {
			byNames[matName] = []string{matId}
			byId[matId] = []string{matId}
		} else {
			firstId := byNames[matName][0]
			if firstId != matId {
				byNames[matName] = append(byNames[matName], matId)
				copy(byId[byNames[matName][0]], byNames[matName])
				if len(byNames[matName]) == 2 {
					exactNameCount++
				}
			}
		}
	}
	fmt.Printf("Done (%v). Found %d index, %d are duplicated using exactly same description\n",
		time.Since(tBuildIndex), len(byNames), exactNameCount)

	fmt.Println("3. Saving data ... ")
	tSave := time.Now()
	recordCount := len(byNames)
	DbCtx().Connection.Query().From("Items").Delete().Run(nil)
	que := pque.NewQue()
	que.WorkerCount = 50
	que.Fn = func(in interface{}) interface{} {
		m := in.(tk.M)
		DbCtx().Connection.Query().From("Items").Save().Run(tk.M{"data": m})
		return m
	}
	que.FnDone = func(in interface{}) {
		m := in.(tk.M)
		fmt.Printf("Saving %s, completed: %3.1f pct \n", m["title"], float64(que.CompletedJob*100)/float64(recordCount))
	}

	que.WaitForKeys()
	for matname, ids := range byNames {
		m := tk.M{}
		m.Set("_id", ids[0])
		m.Set("title", matname)
		if len(ids) == 1 {
			m.Set("duplicated", 0)
		} else {
			m.Set("duplicated", 1)
			m.Set("duplicateid", ids[1:])
		}
		que.SendKey(m)
	}
	que.KeySendDone()
	que.WaitForCompletion()

	fmt.Printf("Saving %d records in %v \n", que.CompletedJob, time.Since(tSave))

	//FindSimilarity_1(mats)
	fmt.Printf("All process completed in %v \n", time.Since(tStart))
}
