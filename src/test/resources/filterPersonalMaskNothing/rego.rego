package kafka

default filter = false

filter {
    input.personal == true
}

maskingByField = {
}

maskingConfig[field] {
	field = maskingByField[input.fieldName]
}