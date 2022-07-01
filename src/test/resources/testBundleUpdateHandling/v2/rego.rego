package kafka

default filter = false

filter {
    input.personal == true
}

maskingByField = {
	"version" : "v2"
}

maskingConfig[field] {
	field = maskingByField[input.fieldName]
}