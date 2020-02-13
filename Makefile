run-otc: 
	go run cmd/app/*.go
run-cvttool:
	go run cmd/cvttool/*.go
build:
	mkdir -p out && cd out && \
	go build -o service.otc ../cmd/app/* && \
	go build -o worker.expirecheck ../cmd/expirecheck/*
clean:
	rm -rf out
	
tidy:
	go fmt ./...
	go mod tidy