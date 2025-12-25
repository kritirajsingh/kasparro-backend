.PHONY: up down test logs rebuild

up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f

rebuild:
	docker compose build --no-cache

test:
	@echo "Running basic API health checks..."
	docker compose up -d
	@sleep 10
	curl http://localhost:8000/data || exit 1
	curl http://localhost:8000/stats || exit 1
	@echo "✅ API endpoints responding correctly"
