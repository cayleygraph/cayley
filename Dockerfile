FROM alpine:3.5

LABEL maintainer "napalmbrain (psev)"
LABEL email "github@napalmbrain.sugarush.io"
LABEL github "github.com/sugarush"
LABEL twitter "@iamnapalmbrain"

ENV INSTALL_PATH="/usr/local/bin"

ENV PKG="ca-certificates" \
    PKG_TMP="g++ git go glide" \
    PKG_CACHE="/var/cache/apk"

ENV BUILD_LIB="github.com/cayleygraph/cayley" \
    BUILD_TAG="master"

ENV GOPATH="/go"

# running all these steps together builds a smaller image
RUN mkdir -p ${GOPATH} && \
    apk update && \
    apk add ${PKG} && \
    apk add ${PKG_TMP} && \
    go get ${BUILD_LIB} && \
    cd ${GOPATH}/src/${BUILD_LIB} && \
    git checkout ${BUILD_TAG} && \
    mkdir /assets && \
    cp -a docs /assets && \
    cp -a static /assets && \
    cp -a templates /assets && \
    glide install && \
    go install -v ./cmd/cayley && \
    cp -a ${GOPATH}/bin/* ${INSTALL_PATH} && \
    rm -rf ${GOPATH} && \
    rm -rf /root/.glide && \
    apk del --purge ${PKG_TMP} && \
    rm -rf ${PKG_CACHE}/*

ENTRYPOINT [ "/usr/local/bin/cayley" ]
