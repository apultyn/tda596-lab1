FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY src/ ./src/
RUN CGO_ENABLED=0 go build -trimpath -o /out/http_server ./src && mkdir /public

FROM scratch
COPY --from=build /out/http_server /http_server
COPY --from=build --chown=65532:65532 /public /public
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/http_server"]
CMD ["8080", "/public"]
