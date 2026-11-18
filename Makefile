# One-function-at-a-time deploy tooling for the Cloud Functions in this repo.
# The argument is the GCP function name exactly as in GCP (e.g. upload-amazon, FUJ_shopify_orders).
#
#   make list                  every function with a deploy.sh: name, region, folder
#   make check <function>      read-only: compare deploy.sh + local source with what is live in GCP
#   make deploy <function>     deploy one function (asks for confirmation; YES=1 skips it,
#                              DRY_RUN=1 only prints the gcloud command)
#   make describe <function>   gcloud functions describe, as YAML
#
# There is deliberately no "deploy all" target.

FN := $(word 2,$(MAKECMDGOALS))

# Treat the second goal (the function name) as a no-op target instead of a file to build.
ifneq ($(FN),)
$(eval $(FN):;@:)
endif

.PHONY: help list check deploy describe

help:
	@sed -n '1,10p' Makefile | sed 's/^# \{0,1\}//'

list:
	@scripts/fn.sh list

check:
	@scripts/fn.sh check $(FN)

deploy:
	@DRY_RUN=$(DRY_RUN) YES=$(YES) scripts/fn.sh deploy $(FN)

describe:
	@scripts/fn.sh describe $(FN)
