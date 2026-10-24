namespace Messaging.NATS.IntegrationTests;

public record TestMessage(string Content);

public record OrderEvent(string OrderId, decimal Amount);

public record PingRequest(string Payload);

public record PingResponse(string Payload, string HandledBy);

public record PaymentEvent(string PaymentId, decimal Amount);
