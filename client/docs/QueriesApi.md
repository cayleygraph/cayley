# \QueriesApi

All URIs are relative to *http://localhost:64210*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GephiGraphStream**](QueriesApi.md#GephiGraphStream) | **Get** /gephi/gs | Gephi GraphStream endpoint
[**Query**](QueriesApi.md#Query) | **Post** /api/v2/query | Query the graph
[**QueryGet**](QueriesApi.md#QueryGet) | **Get** /api/v2/query | Query the graph



## GephiGraphStream

> *os.File GephiGraphStream(ctx, optional)

Gephi GraphStream endpoint

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
 **optional** | ***GephiGraphStreamOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a GephiGraphStreamOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mode** | **optional.String**| Streamer mode | [default to raw]
 **limit** | **optional.Int32**| Limit the number of nodes or quads | 
 **sub** | **optional.String**| Subjects to filter quads by | 
 **pred** | **optional.String**| Predicates to filter quads by | 
 **obj** | **optional.String**| Objects to filter quads by | 
 **label** | **optional.String**| Labels to filter quads by | 

### Return type

[***os.File**](*os.File.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/stream+json, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## Query

> QueryResult Query(ctx, lang, body)

Query the graph

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**lang** | **string**| Query language to use | 
**body** | **string**| Query text | 

### Return type

[**QueryResult**](QueryResult.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## QueryGet

> QueryResult QueryGet(ctx, lang, qu)

Query the graph

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**lang** | **string**| Query language to use | 
**qu** | **string**| Query text | 

### Return type

[**QueryResult**](QueryResult.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

