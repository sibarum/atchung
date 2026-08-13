package sibarum.elektro.queue.example;

import sibarum.elektro.queue.message.Message;

/** Fire-and-forget message used to exercise emit/react. */
@Message(id = 1)
public record Greeting(String text) {
}
