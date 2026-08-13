package sibarum.elektro.queue.example;

import sibarum.elektro.queue.message.Message;

/** Request message used to exercise request/reply correlation. */
@Message(id = 2)
public record Ping(long seq) {
}
