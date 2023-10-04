.phony: all docker_build docker_up docker_down clean

all: docker_down docker_build docker_up fill

docker_build:
	@echo "Docker Compose Build"
	@docker compose build

docker_up:
	@echo "Docker Compose Up, detatched"
	@docker compose up -d

docker_down:
	@echo "Docker Compose Down and delete"
	@docker compose down -v

fill:
	@go run main.go

clean:
	@echo "Nuking Docker..."
	$(MAKE) docker_down
	@yes | docker system prune -a