namespace Library.Contracts.RabbitMq;

/// <summary>
/// Команда на оформление заказа
/// </summary>
/// <param name="OrderId">Идентификатор заказа</param>
/// <param name="Items">Элементы заказа</param>
public record SubmitOrderCommand(Guid OrderId, List<LineItem> Items);