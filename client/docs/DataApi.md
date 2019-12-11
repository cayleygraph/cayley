# \DataApi

All URIs are relative to *http://localhost:64210*

Method | HTTP request | Description
------------- | ------------- | -------------
[**DeleteNode**](DataApi.md#DeleteNode) | **Post** /api/v2/node/delete | Removes a node add all associated quads
[**DeleteQuads**](DataApi.md#DeleteQuads) | **Post** /api/v2/delete | Delete quads from the database
[**ListFormats**](DataApi.md#ListFormats) | **Get** /api/v2/formats | Returns a list of supported data formats
[**ReadQuads**](DataApi.md#ReadQuads) | **Get** /api/v2/read | Reads all quads from the database
[**WriteQuads**](DataApi.md#WriteQuads) | **Post** /api/v2/write | Writes quads to the database



## DeleteNode

> InlineResponse2002 DeleteNode(ctx, body, optional)

Removes a node add all associated quads

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**body** | ***os.File*****os.File**| File in one of formats specified in Content-Type. | 
 **optional** | ***DeleteNodeOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a DeleteNodeOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **format** | **optional.String**| Data decoder to use for request. Overrides Content-Type. | 

### Return type

[**InlineResponse2002**](inline_response_200_2.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/n-quads, application/json, application/x-protobuf
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteQuads

> InlineResponse2001 DeleteQuads(ctx, body, optional)

Delete quads from the database

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**body** | ***os.File*****os.File**| File in one of formats specified in Content-Type. | 
 **optional** | ***DeleteQuadsOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a DeleteQuadsOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **format** | **optional.String**| Data decoder to use for request. Overrides Content-Type. | 

### Return type

[**InlineResponse2001**](inline_response_200_1.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/n-quads, application/ld+json, application/json, application/x-json-stream, application/x-protobuf
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListFormats

> InlineResponse200 ListFormats(ctx, )

Returns a list of supported data formats

### Required Parameters

This endpoint does not need any parameter.

### Return type

[**InlineResponse200**](inline_response_200.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ReadQuads

> *os.File ReadQuads(ctx, optional)

Reads all quads from the database

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
 **optional** | ***ReadQuadsOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a ReadQuadsOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **format** | **optional.String**| Data encoder to use for response. Overrides Accept header. | [default to nquads]
 **sub** | **optional.String**| Subjects to filter quads by | 
 **pred** | **optional.String**| Predicates to filter quads by | 
 **obj** | **optional.String**| Objects to filter quads by | 
 **label** | **optional.String**| Labels to filter quads by | 
 **iri** | **optional.String**| IRI format to use | 

### Return type

[***os.File**](*os.File.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/n-quads, application/ld+json, application/json, application/x-json-stream, application/x-protobuf

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## WriteQuads

> InlineResponse2001 WriteQuads(ctx, body, optional)

Writes quads to the database

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**body** | ***os.File*****os.File**| File in one of formats specified in Content-Type. | 
 **optional** | ***WriteQuadsOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a WriteQuadsOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **format** | **optional.String**| Data decoder to use for request. Overrides Content-Type. | 

### Return type

[**InlineResponse2001**](inline_response_200_1.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/n-quads, application/ld+json, application/json, application/x-json-stream, application/x-protobuf
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

