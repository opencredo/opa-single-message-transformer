package kafka

default filter = false

filter {
    input.personal == true
}

maskingByField = {
	"pii" : "****",
    "phone": "000 0000 0000"
}

maskingConfig[field] {
	field = maskingByField[input.fieldName]
}