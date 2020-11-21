select max(lpo_voucher_time) into @lpo_max_date from purchase_agg;
INSERT INTO purchase_agg(
st_bill_no,
st_branch_id,
st_voucher_time,
st_supplier_id,
st_supplier_name,
st_supplier_billno,
st_gate_time,
st_invoice1,
st_invoice2,
st_purchase_order,
st_sku_id,
st_sku_name,
st_store_id,
st_sku_price,
st_sku_qty,
st_sku_unit,
st_sku_qty1,
st_total,

pr_bill_no,
pr_branch_id,
pr_voucher_time,
pr_supplier_id,
pr_supplier_name,
pr_supplier_billno,
pr_gate_time,
pr_invoice1,
pr_invoice2,
pr_purchase_order,
pr_sku_id,
pr_sku_name,
pr_store_id,
pr_sku_price,
pr_sku_qty,
pr_sku_unit,
pr_sku_qty1,
pr_total,

pi_bill_no,
pi_branch_id,
pi_voucher_time,
pi_supplier_id,
pi_supplier_name,
pi_supplier_billno,
pi_gate_time,
pi_invoice1,
pi_invoice2,
pi_purchase_order,
pi_sku_id,
pi_sku_name,
pi_store_id,
pi_sku_price,
pi_sku_qty,
pi_sku_unit,
pi_sku_qty1,
pi_total,

grn_bill_no,
grn_branch_id,
grn_voucher_time,
grn_supplier_id,
grn_supplier_name,
grn_supplier_billno,
grn_gate_time,
grn_invoice1,
grn_invoice2,
grn_purchase_order,
grn_sku_id,
grn_sku_name,
grn_store_id,
grn_sku_price,
grn_sku_qty,
grn_sku_unit,
grn_sku_qty1,
grn_total,

lpo_bill_no,
lpo_branch_id ,
lpo_voucher_time,
lpo_supplier_id,
lpo_supplier_name,
lpo_supplier_billno,
lpo_gate_time,
lpo_invoice1,
lpo_invoice2,
lpo_purchase_order,
lpo_sku_id,
lpo_sku_name,
lpo_store_id,
lpo_sku_price,
lpo_sku_qty,
lpo_sku_unit,
lpo_sku_qty1,
lpo_total, 
category1,
category2,
category3,
category4,
branch,
lpo_supplier_accountno,
lpo_soh
)

SELECT 
ST.BILNO,
ST.BRANCHID,
ST.VDATE,
ST.SUPNO,
ST.SUPNAME,
ST.SUPBILL,
ST.GATETIME,
ST.INVOICE1,
ST.INVOICE2,
ST.PURCHASEORDER,
ST.SKUID,
ST.SKUNAME,
ST.STOREID,
ST.SKUPRICE,
ST.SKUQTY,
ST.SKUNIT,
ST.SKUQTY1,
ST.TOTAL,

PR.BILNO,
PR.BRANCHID,
PR.VDATE,
PR.SUPNO,
PR.SUPNAME,
PR.SUPBILL,
PR.GATETIME,
PR.INVOICE1,
PR.INVOICE2,
PR.PURCHASEORDER,
PR.SKUID,
PR.SKUNAME,
PR.STOREID,
PR.SKUPRICE,
PR.SKUQTY,
PR.SKUNIT,
PR.SKUQTY1,
PR.TOTAL,

PI.BILNO,
PI.BRANCHID,
PI.VDATE,
PI.SUPNO,
PI.SUPNAME,
PI.SUPBILL,
PI.GATETIME,
PI.INVOICE1,
PI.INVOICE2,
     PI.PURCHASEORDER,
     PI.SKUID,
     PI.SKUNAME,
     PI.STOREID,
     PI.SKUPRICE,
     PI.SKUQTY,
     PI.SKUNIT,
     PI.SKUQTY1,
     PI.TOTAL,

     GRN.BILNO,
     GRN.BRANCHID,
     GRN.VDATE,
     GRN.SUPNO,
     GRN.SUPNAME,
     GRN.SUPBILL,
     GRN.GATETIME,
     GRN.INVOICE1,
     GRN.INVOICE2,
     GRN.PURCHASEORDER,
     GRN.SKUID,
     GRN.SKUNAME,
     GRN.STOREID,
     GRN.SKUPRICE,
     GRN.SKUQTY,
     GRN.SKUNIT,
     GRN.SKUQTY1,
     GRN.TOTAL,

     LPO.BILNO,
     LPO.BRANCHID,
     LPO.VDATE,
     LPO.SUPNO,
     LPO.SUPNAME,
     LPO.SUPBILL,
     LPO.GATETIME,
     LPO.INVOICE1,
     LPO.INVOICE2,
     LPO.PURCHASEORDER,
     LPO.SKUID,
     LPO.SKUNAME,
     LPO.STOREID,
     LPO.SKUPRICE,
     LPO.SKUQTY,
     LPO.SKUNIT,
     LPO.SKUQTY1,
     LPO.TOTAL,
     LPO.CAT1,
     LPO.CAT2,
     LPO.CAT3,
     LPO.CAT4,
     LPO.BRANCH,
     LPO.SUPACC,
     LPO.SOH

          FROM
          
               (SELECT    
               GRN2.BILNO            BILNO,
               GRN2.PCENTER          BRANCHID,
               GRN1.VDATE             VDATE,
               GRN1.SUPNO        SUPNO,
               SUP.NAME               SUPNAME,
               SUP.ACCNO                SUPACC,
               GRN1.SBILNO             SUPBILL,
               GRN1.GATE_TIME          GATETIME,
               GRN1.INV1               INVOICE1,
               GRN1.INV2                  INVOICE2,
               GRN1.PUR_ORDER          PURCHASEORDER,
               GRN2.PRODN              SKUID,
               SKU.PRODNAME_E           SKUNAME,
               GRN2.PRODS           STOREID,
               GRN2.PRICE          SKUPRICE,
               GRN2.QTY            SKUQTY,
               GRN2.UNIT           SKUNIT,
               GRN2.QTY1           SKUQTY1,
               GRN2.TOTAL              TOTAL,
               CAT1.NAME_E             CAT1,
               CAT2.NAME_E             CAT2,
               CAT3.NAME_E             CAT3,
               CAT4.NAME_E             CAT4,
               BRN.NAME_E               BRANCH,
               SKU.HOLD_QTY             SOH


          FROM RECIEPT_2 GRN2
          LEFT JOIN RECIEPT_1 GRN1 ON GRN1.CODE = GRN2.CODE
                                                    AND GRN1.PCENTER = GRN2.PCENTER 
                                                    AND GRN1.BILNO = GRN2.BILNO
          LEFT JOIN     SUPPLIERS SUP   ON SUP.ID = GRN1.SUPNO
          LEFT JOIN     ITEMS SKU        ON SKU.PRODN = GRN2.PRODN
          LEFT JOIN     PRODUCT_CAT1 CAT1 ON SKU.CAT1 = CAT1.ID
          LEFT JOIN     PRODUCT_CAT2 CAT2 ON SKU.CAT2 = CAT2.ID
          LEFT JOIN     PRODUCT_CAT3 CAT3 ON SKU.CAT3 = CAT3.ID
          LEFT JOIN     PRODUCT_CAT4 CAT4 ON SKU.CAT4 = CAT4.ID
          LEFT JOIN     centers BRN ON GRN2.PCENTER = BRN.ID


          WHERE GRN1.CODE = 11 and GRN1.VDATE > @lpo_max_date ) LPO
     LEFT JOIN 
          (SELECT 
               GRN2.BILNO              BILNO,
               GRN2.PCENTER            BRANCHID,
               GRN1.VDATE              VDATE,
               GRN1.SUPNO              SUPNO,
               SUP.NAME                SUPNAME,
               GRN1.SBILNO             SUPBILL,
               GRN1.GATE_TIME          GATETIME,
               GRN1.INV1               INVOICE1,
               GRN1.INV2               INVOICE2,
               GRN1.PUR_ORDER          PURCHASEORDER,
               GRN2.PRODN               SKUID,
               SKU.PRODNAME_E            SKUNAME,
               GRN2.PRODS           STOREID,
               GRN2.PRICE          SKUPRICE,
               GRN2.QTY            SKUQTY,
               GRN2.UNIT           SKUNIT,
               GRN2.QTY1           SKUQTY1,
               GRN2.TOTAL              TOTAL

          FROM RECIEPT_2 GRN2
          LEFT JOIN RECIEPT_1 GRN1 ON GRN1.CODE = GRN2.CODE
                                                    AND GRN1.PCENTER = GRN2.PCENTER 
                                                    AND GRN1.BILNO = GRN2.BILNO
          LEFT JOIN     SUPPLIERS SUP   ON SUP.ID = GRN1.SUPNO
          LEFT JOIN     ITEMS SKU        ON SKU.PRODN = GRN2.PRODN

          WHERE GRN1.CODE = 81) ST
          ON   LPO.PURCHASEORDER = ST.BILNO
          AND LPO.BRANCHID = ST.BRANCHID
          AND LPO.SKUID = ST.SKUID
    LEFT JOIN 
          (SELECT    
               GRN2.BILNO            BILNO,
               GRN2.PCENTER          BRANCHID,
               GRN1.VDATE             VDATE,
               GRN1.SUPNO        SUPNO,
               SUP.NAME               SUPNAME,
               GRN1.SBILNO             SUPBILL,
               GRN1.GATE_TIME          GATETIME,
               GRN1.INV1               INVOICE1,
               GRN1.INV2                  INVOICE2,
               GRN1.PUR_ORDER          PURCHASEORDER,
               GRN2.PRODN              SKUID,
               SKU.PRODNAME_E           SKUNAME,
               GRN2.PRODS           STOREID,
               GRN2.PRICE          SKUPRICE,
               GRN2.QTY            SKUQTY,
               GRN2.UNIT           SKUNIT,
               GRN2.QTY1           SKUQTY1,
               GRN2.TOTAL              TOTAL

          FROM RECIEPT_2 GRN2
          LEFT JOIN RECIEPT_1 GRN1 ON GRN1.CODE = GRN2.CODE
                                                    AND GRN1.PCENTER = GRN2.PCENTER 
                                                    AND GRN1.BILNO = GRN2.BILNO
          LEFT JOIN     SUPPLIERS SUP   ON SUP.ID = GRN1.SUPNO
          LEFT JOIN     ITEMS SKU        ON SKU.PRODN = GRN2.PRODN

          WHERE GRN1.CODE = 3) GRN
          ON   LPO.PURCHASEORDER = GRN.BILNO
          AND LPO.BRANCHID = GRN.BRANCHID
          AND LPO.SKUID = GRN.SKUID

    LEFT JOIN 
          (SELECT    
               GRN2.BILNO            BILNO,
               GRN2.PCENTER          BRANCHID,
               GRN1.VDATE             VDATE,
               GRN1.SUPNO        SUPNO,
               SUP.NAME               SUPNAME,
               GRN1.SBILNO             SUPBILL,
               GRN1.GATE_TIME          GATETIME,
               GRN1.INV1               INVOICE1,
               GRN1.INV2                  INVOICE2,
               GRN1.PUR_ORDER          PURCHASEORDER,
               GRN2.PRODN              SKUID,
               SKU.PRODNAME_E           SKUNAME,
               GRN2.PRODS           STOREID,
               GRN2.PRICE          SKUPRICE,
               GRN2.QTY            SKUQTY,
               GRN2.UNIT           SKUNIT,
               GRN2.QTY1           SKUQTY1,
               GRN2.TOTAL              TOTAL

          FROM RECIEPT_2 GRN2
          LEFT JOIN RECIEPT_1 GRN1 ON GRN1.CODE = GRN2.CODE
                                                    AND GRN1.PCENTER = GRN2.PCENTER 
                                                    AND GRN1.BILNO = GRN2.BILNO
          LEFT JOIN     SUPPLIERS SUP   ON SUP.ID = GRN1.SUPNO
          LEFT JOIN     ITEMS SKU        ON SKU.PRODN = GRN2.PRODN

          WHERE GRN1.CODE = 2) PR
          ON   LPO.PURCHASEORDER = PR.BILNO
          AND LPO.BRANCHID = PR.BRANCHID
          AND LPO.SKUID = PR.SKUID

    LEFT JOIN 
          (SELECT    
               GRN2.BILNO            BILNO,
               GRN2.PCENTER          BRANCHID,
               GRN1.VDATE             VDATE,
               GRN1.SUPNO        SUPNO,
               SUP.NAME               SUPNAME,
               GRN1.SBILNO             SUPBILL,
               GRN1.GATE_TIME          GATETIME,
               GRN1.INV1               INVOICE1,
               GRN1.INV2                  INVOICE2,
               GRN1.PUR_ORDER          PURCHASEORDER,
               GRN2.PRODN              SKUID,
               SKU.PRODNAME_E           SKUNAME,
               GRN2.PRODS           STOREID,
               GRN2.PRICE          SKUPRICE,
               GRN2.QTY            SKUQTY,
               GRN2.UNIT           SKUNIT,
               GRN2.QTY1           SKUQTY1,
               GRN2.TOTAL              TOTAL

          FROM RECIEPT_2 GRN2
          LEFT JOIN RECIEPT_1 GRN1 ON GRN1.CODE = GRN2.CODE
                                                    AND GRN1.PCENTER = GRN2.PCENTER 
                                                    AND GRN1.BILNO = GRN2.BILNO
          LEFT JOIN     SUPPLIERS SUP   ON SUP.ID = GRN1.SUPNO
          LEFT JOIN     ITEMS SKU        ON SKU.PRODN = GRN2.PRODN

          WHERE GRN1.CODE = 1) PI 
          ON   LPO.PURCHASEORDER = PI.BILNO
          AND LPO.BRANCHID = PI.BRANCHID
          AND LPO.SKUID = PI.SKUID;

