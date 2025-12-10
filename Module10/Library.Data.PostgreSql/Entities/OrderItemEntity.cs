namespace Library.Data.PostgreSql.Entities;

/// <summary>
/// Сущность элемента заказа
/// </summary>
public class OrderItemEntity
{
    /// <summary>Идентификатор элемента</summary>
    public Guid Id { get; set; }
    
    /// <summary>Идентификатор книги</summary>
    public Guid BookId { get; set; }
    
    /// <summary>Книга</summary>
    public BookEntity Book { get; set; }
    
    /// <summary>Количество книг</summary>
    public int Quantity { get; set; }
    
    /// <summary>Стоимость одной книги</summary>
    public decimal UnitPrice { get; set; }

    /// <summary>Идентификатор заказа</summary>
    public Guid OrderId { get; set; }
    
    /// <summary>Заказ</summary>
    public OrderEntity Order { get; set; } = null!;
}