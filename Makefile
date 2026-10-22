.PHONY: validate full test

validate:
	cd dag && go run .

full:
	cd dag && go run . -full

test:
	cd dag && go test ./...
