Each entry contains the following information:

| Json Path               | Description |
|-------------------------|-------------|
|timestamp                | When the event was created                   |
|.event.DataElement.travelrecord.passengerList[].uci                     | The unique identifier of the passenger |
|.event.DataElement.travelrecord.passengerList[].age                     | The age of the passenger |
|.event.DataElement.travelrecord.passengerList[].passengerType           | The type of the passenger, an enum with the following possible values: \[Adt, Chd\] |
|.event.DataElement.travelrecord.productsList[].bookingStatus            | The status of the booking, an enum with the following possible values: \[Confirmed, Cancelled, WaitingList, OnRequest, SeatAvailable, Unaccepted\]|
|.event.DataElement.travelrecord.productsList[].flight.operatingAirline  | The 2 character code of the airline operating the flight, KL is KLM|
|.event.DataElement.travelrecord.productsList[].flight.originAirport     | The IATA code of the departure airport |
|.event.DataElement.travelrecord.productsList[].flight.destinationAirport| The IATA code of the destination airport |
|.event.DataElement.travelrecord.productsList[].flight.departureDate     | The UTC time of the flight departure |
|.event.DataElement.travelrecord.productsList[].flight.arrivalDate       | The UTC time of the flight arrival |


The data is UTF-8 encoded.