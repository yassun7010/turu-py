.PHONY: install
install:
	bash scripts/poetry-install.sh

.PHONY: ci
ci:
	bash scripts/ci.sh

.PHONY: format
format:
	bash scripts/format.sh

.PHONY: lint
lint:
	bash scripts/lint.sh

.PHONY: typecheck
typecheck:
	bash scripts/typecheck.sh

.PHONY: test
test:
	bash scripts/test.sh

.PHONY: update
update:
	bash scripts/update.sh

.PHONY: poetry-install
poetry-install:
	bash scripts/poetry-install.sh

.PHONY: poetry-update
poetry-update:
	bash scripts/poetry-update.sh

.PHONY: poetry-lock
poetry-lock:
	bash scripts/poetry-lock.sh

.PHONY: docs
docs:
	poetry run task docs-serve
