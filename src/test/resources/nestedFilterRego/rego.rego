package kafka

default filter = false

filter {
    input.address.personal == true
}

maskingByField = {
}

maskingConfig[field] {
	field = maskingByField[input.fieldName]
}