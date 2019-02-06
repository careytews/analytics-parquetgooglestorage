# More documentation is here:
# https://wiki.dev.trustnetworks.com/development/golang/go-build-system
#
# PROJSL        - project symlink; we go build here
# COMMONDIR     - common is cloned here
# COMMONVENDSL  - common symlink in the vendor folder
#
# make all         - builds everything including docker container image
# make godeps      - gets all the go build dependencies
# make build       - just runs go build, no dependency fetching
# make test        - just runs go test, no dependency fetching
# make mostlyclean - removes anything created by this make, except dep cache
# make clean       - removes anything created by this make

ANALYTIC=parquetgooglestorage
VERSION=unknown
CONTAINER=gcr.io/trust-networks/analytics-${ANALYTIC}:${VERSION}

SRCDIR=go/src
PROJSL=${SRCDIR}/project
COMMONDIR=${SRCDIR}/analytics-common
COMMONREPO=trustnetworks/analytics-common
GITHUBVEND=vendor/github.com
COMMONVENDSL=${GITHUBVEND}/${COMMONREPO}

DEPTOOL=dep ensure -vendor-only -v
SETGOPATH=export GOPATH=$$(pwd)/go

all: godeps build container

version:
	@echo ${VERSION}

build:
	${SETGOPATH} && cd ${PROJSL} && go build -o ${ANALYTIC}

godeps: vend-common vend-analytic ${COMMONVENDSL}

${PROJSL}: ${SRCDIR}
	ln -s ../.. ${PROJSL}

${SRCDIR}:
	mkdir -p ${SRCDIR}

vend-analytic: Gopkg.lock ${PROJSL}
	${SETGOPATH} && cd ${PROJSL} && ${DEPTOOL}

${COMMONVENDSL}: get-common vend-analytic
	mkdir -p ${GITHUBVEND}/trustnetworks
	ln -s ../../../${COMMONDIR} ${COMMONVENDSL}

vend-common: get-common ${COMMONDIR}/Gopkg.lock
	${SETGOPATH} && cd ${COMMONDIR} && ${DEPTOOL}

get-common: ${SRCDIR}
	@if [ -d "${COMMONDIR}" ]; then \
		cd ${COMMONDIR} && git pull; \
	else \
		cd ${SRCDIR} && git clone git@github.com:${COMMONREPO}; \
	fi

container:
	docker build -t ${CONTAINER} -f Dockerfile .

push:
	gcloud docker -- push ${CONTAINER}

mostlyclean:
	rm -f ${ANALYTIC}
	rm -rf ${SRCDIR} # leaves dep cache
	rm -rf vendor

clean: mostlyclean
	rm -rf go # clears dep cache

test:
	${SETGOPATH} && cd ${PROJSL} && go test
