package main

import (
	. "eaciit/storaenso/processapp/duplicatematl"
	"fmt"
	ts "github.com/eaciit/textsearch"
	tk "github.com/eaciit/toolkit"
	"sync"
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
	byNames := make(map[string][]float64, 0)

	exactNameCount := 0
	fmt.Print("2. Build index ... ")
	for _, matl := range mats {
		matName := matl["Trimmed"].(string)
		matId := matl["_id"].(float64)
		if _, byNameExist := byNames[matName]; !byNameExist {
			byNames[matName] = []float64{matId}
		} else {
			byNames[matName] = append(byNames[matName], matId)
			exactNameCount++
		}
	}
	fmt.Printf("Done (%v). Found %d index, %d are duplicated using exactly same description\n",
		time.Since(tBuildIndex), len(byNames), exactNameCount)

	final := tk.M{}
	tSimilarity := time.Now()
	fmt.Println("3. Get by similarity group ... ")
	wg := new(sync.WaitGroup)
	for _, matl := range mats {
		wg.Add(1)
		go getSimilarity_0(matl, &mats, &final, wg)
	}
	wg.Wait()
	/*
		inloop := true
		idxLoop := 0
			for inloop && idxLoop < 10 {
				if len(mats) > 0 {
					matl := mats[idxLoop]
					findSimilarity(matl, &mats, &final)
				} else {
					inloop = false
				}
				idxLoop++
			}
	*/
	fmt.Printf("Done (%v). \n", time.Since(tSimilarity))
	fmt.Printf("All process completed in %v \n", time.Since(tStart))
}

func getSimilarity_0(find tk.M, compareToP *[]tk.M, resultP *tk.M, wg *sync.WaitGroup) {
	matName := find["Trimmed"].(string)
	matId := find["_id"].(float64)
	tstart := time.Now()
	fmt.Printf("Find similarity for %v - %s \n", matId, matName)
	results := *resultP
	cs := *compareToP

	for _, _ = range cs {
		//cname := c["Trimmed"].(string)
		//cid := c["_id"].(float64)
		s := ts.NewSimilaritySetting()
		s.SplitDelimeters = []rune{' ', '-', '.'}
		//if ts.Similarity(matName, cname, s) >= 80 {
		//		//break
		//}
	}

	fmt.Printf("Done for %v - %s in %v. Collected similar items: %d. Remaining items: %d \n",
		matId, matName,
		time.Since(tstart),
		0,
		//len(results[matName].([]float64)),
		len(cs))

	//*compareToP = cs
	*resultP = results
	wg.Done()
}

func findSimilarity(find tk.M, compareToP *[]tk.M, resultP *tk.M) {
	results := *resultP
	cs := *compareToP

	matName := find["Trimmed"].(string)
	matId := find["_id"].(float64)
	tstart := time.Now()
	fmt.Printf("Find similarity for %v - %s ... ", matId, matName)

	results[matName] = make([]float64, 0)
	idx := 0
	inloop := true
	for inloop {
		if idx == 0 {
			fmt.Printf("El 0: %v == %v \n", find, cs[0])
		}
		idx++
		if idx > len(cs) && len(cs) > 0 {
			inloop = false
		} else {
			//fmt.Printf("Len S now: %d \n", len(cs))
			c := cs[0]
			cName := c["Trimmed"].(string)
			cId := c["_id"].(float64)
			if cId == matId {
				addSimilarity(0, &results, &cs)
			} else {
				s := ts.NewSimilaritySetting()
				s.SplitDelimeters = []rune{' ', '-', '.'}
				if ts.Similarity(matName, cName, s) >= 80 {
					addSimilarity(0, &results, &cs)
				}
			}
		}
	}
	fmt.Printf("Done in %v. Collected similar items: %d. Remaining items: %d \n",
		time.Since(tstart),
		len(results[matName].([]float64)),
		len(cs))

	*compareToP = cs
	*resultP = results
}

func addSimilarity(idx int, outP *tk.M, sourceP *[]tk.M) {
	out := *outP
	source := *sourceP

	matl := source[idx]
	name := matl["Trimmed"].(string)
	id := matl["_id"].(float64)
	if _, b := out[name]; !b {
		out[name] = make([]float64, 0)
	}
	out[name] = append(out[name].([]float64), id)
	source = append(source[:idx], source[idx+1:]...)

	*outP = out
	*sourceP = source
}
