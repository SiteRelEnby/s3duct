PY := .venv/bin/python

MINIO_NAME := s3duct-minio
MINIO_PORT := 9000
MINIO_BUCKET := s3duct-test
MINIO_ENV := AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin

.PHONY: test lint fix typecheck check minio-up minio-test minio-down

test:
	$(PY) -m pytest tests/ -q

lint:
	$(PY) -m ruff check s3duct tests

fix:
	$(PY) -m ruff check s3duct tests --fix

typecheck:
	$(PY) -m mypy

check: lint typecheck test

# --- Local MinIO integration (mirrors .github/workflows/minio.yml) ---

minio-up:
	docker run -d --name $(MINIO_NAME) -p $(MINIO_PORT):9000 \
	  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
	  minio/minio server /data
	@echo "Waiting for MinIO..."
	@for i in $$(seq 1 30); do \
	  docker exec $(MINIO_NAME) mc ready local >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	$(MINIO_ENV) $(PY) -c "import boto3; boto3.client('s3', endpoint_url='http://localhost:$(MINIO_PORT)', region_name='us-east-1').create_bucket(Bucket='$(MINIO_BUCKET)')"
	@echo "MinIO ready at http://localhost:$(MINIO_PORT) (bucket: $(MINIO_BUCKET))"

minio-test:
	$(MINIO_ENV) \
	S3DUCT_TEST_BUCKET=$(MINIO_BUCKET) \
	S3DUCT_ENDPOINT_URL=http://localhost:$(MINIO_PORT) \
	PATH="$(CURDIR)/.venv/bin:$(PATH)" \
	  bash tests/integration/roundtrip.sh

minio-down:
	docker rm -f $(MINIO_NAME)
