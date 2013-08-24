package external_sort

import (
  "math/rand"
  "testing"
  "encoding/gob"
)

type ComparableInt int
func (i ComparableInt) LessThan(other *ComparableItem) bool {
  o := (*other).(ComparableInt)
  return i < o
}

func TestExternalSort(t *testing.T) {
  gob.Register(ComparableInt(0))

  r := rand.New(rand.NewSource(0))

  for memorySize := 1; memorySize < 5; memorySize++ {
    unsortedChan := make(chan *ComparableItem)
    sortedChan := make(chan *ComparableItem)
    go ExternalSort(memorySize, unsortedChan, sortedChan)

    for i := 0; i < 1049; i++ {
      in := ComparableItem(ComparableInt(r.Int()))
      unsortedChan <- &in
    }
    close(unsortedChan)

    last := ComparableInt(-1)
    for iw := range sortedChan {
      i := (*iw).(ComparableInt)
      if last > i {
        t.Errorf("Output isn't sorted!")
      }
      last = i
    }
  }
}
