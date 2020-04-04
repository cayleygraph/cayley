package linkedql

import (
	"fmt"

	"github.com/piprate/json-gold/ld"
)

// datasetToCompact transforms a RDF dataset to a compact JSON-LD document.
func datasetToCompact(dataset *ld.RDFDataset, context interface{}, opts *ld.JsonLdOptions) (interface{}, error) {
	api := ld.NewJsonLdApi()
	proc := ld.NewJsonLdProcessor()
	d, err := api.FromRDF(dataset, opts)
	if err != nil {
		return nil, err
	}
	c, err := proc.Compact(d, context, opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// singleDocumentFromRDF transforms a RDF dataset to a single map JSON-LD document.
func singleDocumentFromRDF(dataset *ld.RDFDataset) (interface{}, error) {
	api := ld.NewJsonLdApi()
	opts := ld.NewJsonLdOptions("")
	documents, err := api.FromRDF(dataset, opts)
	if err != nil {
		return nil, err
	}
	if len(documents) != 1 {
		return nil, fmt.Errorf("Unexpected number of documents")
	}
	return documents[0], nil
}
