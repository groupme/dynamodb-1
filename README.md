To test this package or use it to develop a project without paying for dynamo usage download and install dynamo local:

http://aws.typepad.com/aws/2013/09/dynamodb-local-for-desktop-development.html

Run it like this:

java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar

Also you'll need to have a CA certificate file setup in order to connect over tls. You can use the one inlcluded in this repository and then ensure that you have setup the environment variable:

export CACERT=~/.ssl/ca.cert

Now run:

go run test

If the tests pass, all is well and you'll be able to use this package in your go projects.
