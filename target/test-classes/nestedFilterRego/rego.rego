package kafka

default filter = false

filter {
    input.address.personal == true
}

maskingByField = {
}