#!/usr/bin/env bash

curl -v -X GET \
-H 'X-Nakadi-Cursors: [{"partition":"shardId-000000000002","offset":"BEGIN"},{"partition":"shardId-000000000001","offset":"LATEST"},{"partition":"shardId-000000000007","offset":"49568740689862039868575257762873836821812488551106871410"}]' \
"http://localhost:4567/event-types/kintry/events"
