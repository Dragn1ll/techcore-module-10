namespace Library.Contracts.RabbitMq;

/// <summary>
/// Элемент заказа
/// </summary>
/// <param name="BookId">Идентификатор книги</param>
/// <param name="Quantity">Количество</param>
public record LineItem(Guid BookId, int Quantity);