default:
	GO111MODULE=on go build ./...

test:
	GO111MODULE=on go test ./...

check:
	GO111MODULE=off go get golang.org/x/lint/golint
	GO111MODULE=on golint `go list ./...`
