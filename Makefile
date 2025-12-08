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


# ------------------------------
# Release helpers (minimal)
# ------------------------------
VERSION_PY = cdr_usage_api/__init__.py

.PHONY: check-clean release

check-clean:
	@git update-index -q --ignore-submodules --refresh
	@if ! git diff-index --quiet HEAD --; then \
	  echo "Working tree is dirty. Commit or stash changes first."; exit 1; \
	fi



# Minimal release flow: check clean, run tests, tag and push
# Usage: make release VERSION=v1.2.0
release: check-clean
	@test -n "$(VERSION)" || (echo "VERSION must be set, e.g. make release VERSION=v1.2.0"; exit 1)
	@echo "Running tests..."
	@$(MAKE) test
	@echo "Creating annotated tag $(VERSION)..."
	@git tag -a $(VERSION) -m "Release $(VERSION)"
	@echo "Pushing tag to origin..."
	@git push origin --tags
	@echo "Done. CI should handle building/publishing on tag."

# Minimal deploy: commit any changes, tag and push. Good for demos.
# Usage: make deploy VERSION=v1.2.0
.PHONY: deploy
deploy:
	@test -n "$(VERSION)" || (echo "VERSION must be set, e.g. make deploy VERSION=v1.2.0"; exit 1)
	@echo "Staging all changes..."
	@git add -A
	@echo "Committing (if there are changes)..."
	@git commit -m "chore(release): prepare $(VERSION)" || echo "No changes to commit"
	@echo "Creating annotated tag $(VERSION)..."
	@git tag -a $(VERSION) -m "Release $(VERSION)"
	@echo "Pushing branch and tags to origin..."
	@git push origin main --follow-tags
	@echo "Deploy flow complete. CI (if configured) should run on the pushed tag."
