CREATE OR REPLACE VIEW firebase.user_master as
(
    SELECT 
        cdi.customer_id, 
        sua.*
    FROM firebase.sessions_users_aggregated sua
    LEFT JOIN 
    (
        SELECT 
            customer_id, 
            lower(email) email 
        FROM magento.customer_demographic_info
        WHERE email IS NOT NULL
        group by 1,2
    )  cdi 
    ON lower(sua.email) = cdi.email 
    WHERE sua.email IS NOT NULL
)
WITH NO SCHEMA BINDING;