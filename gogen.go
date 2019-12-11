package cayley

//go:generate docker run --rm -v ${PWD}:/local openapitools/openapi-generator-cli generate -i /local/docs/api/swagger.yml -g go -o /local/client
