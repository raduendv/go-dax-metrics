APPS ?= $(shell find ./apps -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
BUILD_DIR := ./build
EC2_KEY := ~/.ssh/my-key.pem
EC2_IP := 3.254.119.128
EC2_PATH := /home/ec2-user/app/

.PHONY: all clean build deploy run stop

all: build

build: $(APPS)

$(APPS):
	@echo "Building lambda: $@"
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$@ ./apps/$@/*.go

clean:
	@echo "Cleaning build directory"
	rm -rf $(BUILD_DIR)

deploy: build
	rsync -avz -e "ssh -i $(EC2_KEY)" ./build/* ec2-user@$(EC2_IP):$(EC2_PATH)
	ssh -i $(EC2_KEY) ec2-user@$(EC2_IP) "chmod 755 $(EC2_PATH)/*"

#./dax-metrics -test=CacheHitDefault -op=GetItem
run:
	ssh -i $(EC2_KEY) ec2-user@$(EC2_IP) "ls -la $(EC2_PATH)"

stop:
	ssh -i $(EC2_KEY) ec2-user@$(EC2_IP) "ls -la $(EC2_PATH)"
