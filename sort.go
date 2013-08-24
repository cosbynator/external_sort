package external_sort

import (
  "io"
  "os"
  "path"
  "sort"
  "strconv"
  "io/ioutil"
  "encoding/gob"
)

type ComparableItem interface {
  LessThan(other *ComparableItem) bool
}

type ComparableItems []*ComparableItem
func (s ComparableItems) Len() int { return len(s) }
func (s ComparableItems) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ComparableItems) Less(i, j int) bool { return (*s[i]).LessThan(s[j]) }

func mergeFiles(path1, path2, outputPath string) {
    f1, err := os.Open(path1)
    if err != nil { panic(err) }
    defer f1.Close()
    f2, err := os.Open(path2)
    if err != nil { panic(err) }
    defer f2.Close()
    fout, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
    if err != nil { panic(err) }
    defer fout.Close()

    g1 := gob.NewDecoder(f1)
    g2 := gob.NewDecoder(f2)
    gout := gob.NewEncoder(fout)

    var tmp ComparableItem
    var tmp2 ComparableItem
    h1 := &tmp
    h2 := &tmp2
    var err1, err2 error
    err1 = g1.Decode(h1)
    if err1 != nil { panic(err1) }
    err2 = g2.Decode(h2)
    if err2 != nil { panic(err2) }

    for err1 != io.EOF && err2 != io.EOF {
      if (*h1).LessThan(h2) {
        gout.Encode(h1)
        err1 = g1.Decode(h1)
      } else {
        gout.Encode(h2)
        err2 = g2.Decode(h2)
      }
    }

    for err1 != io.EOF {
        gout.Encode(h1)
        err1 = g1.Decode(h1)
    }

    for err2 != io.EOF {
        gout.Encode(h2)
        err2 = g2.Decode(h2)
    }
}

func ExternalSort(numMemory int, inputChan chan *ComparableItem, outputChan chan *ComparableItem) {
  memoryItems := make(ComparableItems, 0, numMemory)
  unmergedFiles := make([]string, 0)

  tmpDir, err := ioutil.TempDir("", "go_external_sort")
  if err != nil { panic(err) }
  defer os.RemoveAll(tmpDir)

  fileCount := 0
  uniqueFileName := func() string {
    ret := path.Join(tmpDir, strconv.Itoa(fileCount))
    fileCount++
    return ret
  }

  flushMemory := func() {
    sort.Sort(memoryItems)
    fileName := uniqueFileName()
    outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
    if err != nil { panic(err) }
    defer outputFile.Close()

    gobEncoder := gob.NewEncoder(outputFile)
    for _, item := range memoryItems {
      err := gobEncoder.Encode(item)
      if err != nil { panic(err) }
    }
    unmergedFiles = append(unmergedFiles, fileName)
    memoryItems = memoryItems[:0]
  }

  for i := range inputChan {
    memoryItems = append(memoryItems, i)
    if len(memoryItems) >= numMemory {
      flushMemory()
    }
  }

  if len(memoryItems) > 0 {
    flushMemory()
  }

  for len(unmergedFiles) > 1 {
    mergeName := uniqueFileName()
    mergeFiles(unmergedFiles[0], unmergedFiles[1], mergeName)
    unmergedFiles = append(unmergedFiles[2:], mergeName)
  }

  f, err := os.Open(unmergedFiles[0])
  defer f.Close()
  g := gob.NewDecoder(f)

  for {
    var h ComparableItem
    err := g.Decode(&h)
    if err == io.EOF {
      close(outputChan)
      break
    } else if err != nil {
      panic(err)
    } else {
      outputChan <- &h
    }
  }

  return
}
