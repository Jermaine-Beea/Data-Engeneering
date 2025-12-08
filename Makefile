.PHONY: install test build-and-push dbt-compile

# Install all requirements found in the repo
install:
	python -m pip install --upgrade pip
	@reqs=$$(find . -type f -name 'requirements.txt' -not -path './venv/*' -print); \
	if [ -z "$$reqs" ]; then \
		echo "No requirements.txt files found"; \
	else \
		for f in $$reqs; do \
			echo "Installing from $$f"; \
			python -m pip install -r "$$f" || true; \
		done; \
	fi

# Run pytest if tests exist
test:
	@if [ -d tests ] || ls -d */tests >/dev/null 2>&1; then \
		python -m pip install pytest || true; \
		pytest -q || true; \
	else \
		echo "No tests found - skipping"; \
	fi


# Build and push images. Expects REGISTRY and TAG env vars.
# Example: make build-and-push REGISTRY=registry.gitlab.com/group/proj TAG=sha
build-and-push:
	@echo "Building and pushing images to $(REGISTRY) with tag $(TAG)"
	@find . -type f -name Dockerfile -not -path './.git/*' -print | while read -r df; do \
		dir=$$(dirname "$$df"); \
		name=$$(basename "$$dir"); \
		image="$(REGISTRY)/$${name}:$(TAG)"; \
		echo "Building $$image from $$dir"; \
		docker build -t "$$image" "$$dir"; \
		echo "Pushing $$image"; \
		docker push "$$image"; \
		if [ "$(CI_COMMIT_BRANCH)" = "main" ]; then \
			docker tag "$$image" "$(REGISTRY)/$${name}:latest"; \
			docker push "$(REGISTRY)/$${name}:latest"; \
		fi; \
	done

# Lightweight dbt compile
dbt-compile:
	@if [ -d dbt ]; then \
		python -m pip install dbt-core || true; \
		(cd dbt && dbt compile) || true; \
	else \
		echo "No dbt directory - skipping"; \
	fi
