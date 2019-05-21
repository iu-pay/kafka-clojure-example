# kafka-example

An example showing how to use Kafka producer and consumer using Clojure and the Java API via Java interop. Simple example that consumers from a topic logs the input and then sends on the value to another topic for the user to see. You can run it as a plain jar with all the Kafka Broker and Zookeeper running or you can use the Docker set up we have done and follow the blog post https://perkss.github.io/#/DevOps! Up to you.

## Installation

Requires Zookeeper and Kafka to be set up. Check the ports match from the code.
Checkout the project view the start script and follow those commands.

You can also just install confluent platform and run the start-example.sh script from the bin directory.

## Usage

In the project directory run :

    $ lein uberjar

    $ java -jar target/uberjar/kafka-example-0.1.0-SNAPSHOT-standalone.jar

This starts the project and you should see it log out that it has started.

You then need to set up the Kafka topics, producer and consumer as specified in the start.sh script. Once set up you can produce to the example topic for example Hello

With the app running it will log out:

    INFO  kafka-example.core: Sending on value Value: Hello

Then with a consumer on the example-prouced-topic it will log out Value: Hello

## Running with Docker

Docker makes the above very simple.

    $ docker build -t producer-consumer-example .

    $ docker run -i -t producer-consumer-example

## Example

Have fun with the example, kept very simple purposely to show the Java interop API of Kafka Clients in Clojure.
