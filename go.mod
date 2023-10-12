module github.com/blevesearch/zapx/v15

go 1.19

require (
	github.com/RoaringBitmap/roaring v1.2.3
	github.com/blevesearch/bleve_index_api v1.0.5
	github.com/blevesearch/go-faiss v0.2.1-0.20230718193937-72c2455dad4c
	github.com/blevesearch/mmap-go v1.0.4
	github.com/blevesearch/scorch_segment_api/v2 v2.1.5
	github.com/blevesearch/vellum v1.0.10
	github.com/golang/snappy v0.0.1
	github.com/spf13/cobra v1.4.0
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d
)

require (
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.13.0 // indirect
)

replace github.com/blevesearch/bleve_index_api => ../bleve_index_api

replace github.com/blevesearch/go-faiss => ../go-faiss

replace github.com/blevesearch/scorch_segment_api/v2 => ../scorch_segment_api
