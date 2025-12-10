namespace Library.Contracts.Orders.Request;

/// <summary>
/// Элемент заказа в запросе
/// </summary>
/// <param name="BookId">Идентификатор книги</param>
/// <param name="Quantity">Количество</param>
public record LineItemRequest(Guid BookId, int Quantity);