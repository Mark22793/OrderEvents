namespace Contracts;

public record OrderPlaced(
    Guid OrderId,
    string StudentId,
    decimal Total,
    DateTime PlacedAtUtc);

