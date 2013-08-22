# Go External Sort #

This module provides a sort for go data that is unable to fit into memory.
Call `ExternalSort` with channels to any interfaces that are [Gob](http://golang.org/pkg/encoding/gob)-encodable 
and provide a `LessThan` method. 

It works by writing sorting chunks of the input onto disk, and merging the
resulting files.

See `sort_test.go` for example usage.
