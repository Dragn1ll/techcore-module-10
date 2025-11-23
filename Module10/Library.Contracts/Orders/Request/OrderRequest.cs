namespace Library.Contracts.Orders.Request;

/// <summary>
/// Запрос на создание заказа
/// </summary>
/// <param name="Items">Элементы заказа</param>
public record OrderRequest(List<LineItemRequest> Items);