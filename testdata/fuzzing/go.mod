module github.com/xdg-go/jibby/testdata/fuzzing

go 1.13

require (
	github.com/dvyukov/go-fuzz v0.0.0-20191206100749-a378175e205c // indirect
	github.com/xdg-go/jibby v0.0.0-00010101000000-000000000000
	go.mongodb.org/mongo-driver v1.3.0
)

replace github.com/xdg-go/jibby => ../..
