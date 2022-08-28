reset:
	docker-compose down
	docker-compose up db -d

clean:
	python3 clean.py