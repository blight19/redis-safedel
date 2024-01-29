CGO_ENABLED=0 go build  -ldflags="-s -w" -o safedel main.go && upx -9 safedel
