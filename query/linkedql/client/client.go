package client

import (
	"encoding/json"
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
)

type StepType struct {
	ID      string `json:"@id"`
	Type    string `json:"@type"`
	Comment string `json:"rdfs:comment"`
}

func GenerateClient() (string, error) {
	jsonFile, err := os.Open("schema.json")
	if err != nil {
		return "", err
	}
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var a []StepType
	err = json.Unmarshal(byteValue, &a)
	if err != nil {
		return "", err
	}
	fmt.Printf("%v\n", a)
	src := `
package client

type Step map[string]interface{}

type Path struct{
	cursor Step
}
`
	stepName := "Vertex"
	argumentNames := []string{
		"values",
	}
	argumentsString := ""
	for _, argument := range argumentNames {
		argumentsString += argument + " " + "[]interface{}"
	}
	fieldsString := ""
	for _, argument := range argumentNames {
		fieldsString += "\n\"" + argument + "\": " + argument + ","
	}
	generated := fmt.Sprintf(`
func (p *Path) %s (%s) *Path {
	return Path{
		cursor: Step{
			"@type": "linkedql:%s",
			"from": p.cursor,%s
		},
	}
}
	`, stepName, argumentsString, stepName, fieldsString)

	all := src + generated

	formatted, err := format.Source([]byte(all))
	formattedStr := string(formatted)

	if err != nil {
		return "", err
	}

	return formattedStr, nil
}
