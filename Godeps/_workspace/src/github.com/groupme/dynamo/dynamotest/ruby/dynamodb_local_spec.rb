require "minitest/spec"
require "minitest/autorun"
require_relative "dynamodb_local"

describe DynamoDBLocal do
  it "can be started and stopped" do
    db = DynamoDBLocal.new
    db.running?.must_equal(false)
    db.url.must_match(%r{http://127\.0\.0\.1:\d+/})

    db.start
    db.running?.must_equal(true)

    db.ready?.must_equal(false)
    db.wait_until_ready
    db.ready?.must_equal(true)

    db.stop
    db.wait_until_stopped
    db.running?.must_equal(false)
  end
end
