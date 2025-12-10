using Library.SharedKernel.Enums;

namespace Library.Data.PostgreSql.Entities;

/// <summary>
/// Сущность заказа
/// </summary>
public class OrderEntity
{
    /// <summary>Идентификатор заказа</summary>
    public Guid Id { get; set; }
    
    /// <summary>Дата заказа</summary>
    public DateTime OrderDate { get; set; }
    
    /// <summary>Электронная почта заказчика</summary>
    public string CustomerEmail { get; set; }

    /// <summary>Статус заказа</summary>
    public OrderStatus Status { get; set; }

    /// <summary>Элементы заказа</summary>
    public ICollection<OrderItemEntity> Items { get; set; }
}