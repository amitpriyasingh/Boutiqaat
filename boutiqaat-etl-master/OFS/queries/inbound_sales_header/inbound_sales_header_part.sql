SELECT 
    Id, 
    OrderJSONId, 
    WebOrderNo, 
    CustomerId, 
    PaymentMethodCode, 
    PaymentGateway, 
    OrderDateTime, 
    OrderType, 
    Frequency, 
    Priority, 
    IsExchange, 
    IsGiftWrap, 
    Country, 
    ReadyForArchive, 
    InsertedOn, 
    InsertedBy, 
    UpdatedOn, 
    UpdatedBy, 
    OrderCategory, 
    Confirm, 
    ReferenceOrderNo, 
    AppOrderNo, 
    OrderSource, 
    Company, 
    ErrorMessage, 
    IsSync, 
    RetryCount
FROM OFS.InboundSalesHeader
where DATE(InsertedOn) = DATE('{{DATE}}') AND \$CONDITIONS