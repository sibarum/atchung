package sibarum.elektro.queue.example;

import sibarum.elektro.queue.message.Message;

/** Reply message correlated to a {@link Ping}. */
@Message(id = 3)
public record Pong(long seq, String note) {
}
