# Nakadi4Rich
Simple try of AWS Kinesis + Nakadi API

# Run
1) Update aws creadentials file with:

\> `mai login aruha-test `

2) Run application

\> `gradle run`

3) The app will be running at:

`http://localhost:4567`

# API
publish single event: `POST /event-types/{event-type}/events`

read events stream: `GET /event-types/{event-type}/events`

get partitions info: `GET /event-types/{event-type}/partitions`

Currently there's one event-type that exists in Kinesis in our test-account: `kintry`.

