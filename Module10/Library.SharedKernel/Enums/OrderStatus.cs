namespace Library.SharedKernel.Enums;

/// <summary>
/// Статус заказа
/// </summary>
public enum OrderStatus
{
    Default = 0,
    
    /// <summary>В ожидании</summary>
    Pending = 1,
    
    /// <summary>В обработке</summary>
    Processing = 2,
    
    /// <summary>Отгружен</summary>
    Shipped,
    
    /// <summary>Доставлен</summary>
    Delivered = 3,
    
    /// <summary>Отменен</summary>
    Cancelled = 4 
}