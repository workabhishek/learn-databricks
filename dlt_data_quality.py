# retain invalid records
@dlt.expect("valid timestamp", "col('timestamp') > '2012-01-01'")

# drop invalid records
@dlt.expect_or_drop("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")

# fail on invalid records
@dlt.expect_or_fail("valid_count", "count > 0")

##########################
## Multiple expectation ##
##########################

# python dictionary as args
# expect_all - include records that violate expectation.
# expect_all_or_drop
# expect_all_or_fail

@dlt.expect_all(
    {
        "valid_count" : "count > 0",
        "valid_current_page" : "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"
    }
)