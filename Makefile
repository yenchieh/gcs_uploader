.PHONY: deps vet test dev build clean

PACKAGES = $(shell glide novendor)
DOCKER_REPO_URL = jack08300/gcs_uploader

deps:
	dep ensure

vet:
	go vet $(PACKAGES)

dev:
	DEBUG=1 \
	BUCKET_NAME=komiic \
	FILE_NAME=000001.jpg.webp \
	go run main.go

build: clean
	GOOS=linux go build -o ./build/main *.go

clean:
	rm -rf build/*
	find . -name '*.test' -delete

push-image: build build-image
	docker tag gcs_uploader $(DOCKER_REPO_URL):latest
	docker push $(DOCKER_REPO_URL):latest

build-image:
	docker build --rm -t gcs_uploader:latest .