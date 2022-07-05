package kafka

default filter = false

filter {
    false
}

maskingByField = {
}

maskingConfig[field] {
	field = maskingByField[input.fieldName]
}