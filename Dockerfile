FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /controlplane ./cmd/controlplane

FROM alpine:3.20
COPY --from=build /controlplane /controlplane
ENTRYPOINT ["/controlplane"]
