### Test `max.request.size`
- `max.request.size` is related with max size of single kafka record.
- If you want to see more, please refer to this test codes.
  - https://github.com/chickenchickenlove/kafka-max-request-size/blob/1f40894cbfb0a788978ac30e4ea42296359d93bd/src/test/java/org/example/MainTest.java#L93-L122



### Test
```shell
$ gradle test
```


### Description for test code
- `should_fail_with_bigSingleRecord()` : when `max.request.size == 1MB`, big single record (Size == 3MB) should be failed.
- `should_success_with_smallRecords()` : when `max.request.size == 1MB`, small records (Size = 130KB * 100) should be success.
