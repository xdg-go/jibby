# Fuzzing jibby

- Download go-fuzz and go-fuzz-build

```
GO111MODULES=yes go get -u github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build
```

- Run `./fuzz-prep.sh`

- Run `go-fuzz` with one of the `Fuzz*` functions

```
go-fuzz -bin=jibbyfuzz.zip -func=FuzzCrash
```
