--New Schedule present 1.30 UTC to 7.30 UTC - 0 30 7 ? * * *
set start_date=current_date-2;

set end_date=current_date;

MERGE INTO 65f98121a162a56a.efa1f375d76194fa.f6071cb0f02112c9 a USING
(
  with base as
(
select distinct order_id,pg_chargeback,PG_CHARGEBACK_REVERSAL,PG_REFUND,PG_TRANSACTION,CREATED_AT,TRANSACTION_ID,lob,updated_at
 from 66d9c488b5fa766b.3ad2e1180e2ad695.e91f525043751268 a

 where (updated_at) between $start_date and $end_date
  

),
PG_CHARGEBACK as
(
select order_id,
  'PG_CHARGEBACK' as description,
  coalesce(regexp_replace( vm.value:pg ,'\\(|\\)',''),'')PG,
 
 coalesce(regexp_replace( vm.value:merchantId,'\\(|\\)',''),'') MID,
  coalesce(TRANSACTION_ID ,'')txn_id,
  coalesce(regexp_replace(vm.value:pgTransactionId,'\\(|\\)',''),'') Pg_TXN_ID,
  coalesce(regexp_replace(vm.value:transactionType,'\\(|\\)',''),'') TXN_TYPE,
 coalesce(regexp_replace( vm.value:transactionTime,'\\(|\\)',''),'') TXN_DATE,
  coalesce(regexp_replace(vm.value:settlementDate,'\\(|\\)',''),'') SETTLEMENT_DATE,
  coalesce(regexp_replace(vm.value:pgTransactionAmount,'\\(|\\)',''),'') GROSS_TXN_VALUE,
  coalesce(regexp_replace(vm.value:pgCommission,'\\(|\\)',''),'') PG_COMMISSION,
  coalesce(regexp_replace(vm.value:cgst,'\\(|\\)',''),'') CGST,
  coalesce(regexp_replace(vm.value:sgst,'\\(|\\)',''),'') SGST,
  coalesce(regexp_replace(vm.value:igst,'\\(|\\)',''),'') IGST,
  coalesce(regexp_replace(vm.value:amountTransferredToNodal,'\\(|\\)',''),'') AMOUNT_TRANSFERRED_TO_NODAL,
  coalesce(replace(replace(REPLACE(vm.value:utr, ']',''),'[',''),'"',''),'') UTR,
  coalesce(regexp_replace(vm.value:pgRefundId,'\\(|\\)',''),'') REFUND_ID,
  coalesce(CREATED_AT,'')CREATED_AT,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') paymentMode,
  coalesce(regexp_replace(vm.value:bank,'\\(|\\)',''),'') cardIssuer,
  coalesce(regexp_replace(vm.value:cardType,'\\(|\\)',''),'') cardType,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') createdBy,
  coalesce(regexp_replace(vm.value:cardNumber,'\\(|\\)',''),'') cardNumber,
  coalesce(regexp_replace(vm.value:binNumber,'\\(|\\)',''),'') binNumber,
  coalesce(regexp_replace(vm.value:last4Digits,'\\(|\\)',''),'') cardLastFourDigits,
  coalesce(regexp_replace(vm.value:authCode,'\\(|\\)',''),'') authCode,
  coalesce(regexp_replace(vm.value:bankReference,'\\(|\\)',''),'') bankReference,
 coalesce(regexp_replace( vm.value:rrn,'\\(|\\)',''),'')rrn,
  coalesce(lob,'')lob,
  coalesce(updated_at,'')updated_at,
  vm.value as arr
  
  
  
  from base
    , lateral flatten(input => parse_json(pg_chargeback)) vm
)

,

PG_CHARGEBACK_REVERSAL as
(
select order_id,
  'PG_CHARGEBACK_REVERSAL' as description,
    coalesce(regexp_replace( vm.value:pg ,'\\(|\\)',''),'')PG,
 
 coalesce(regexp_replace( vm.value:merchantId,'\\(|\\)',''),'') MID,
  coalesce(TRANSACTION_ID ,'')txn_id,
  coalesce(regexp_replace(vm.value:pgTransactionId,'\\(|\\)',''),'') Pg_TXN_ID,
  coalesce(regexp_replace(vm.value:transactionType,'\\(|\\)',''),'') TXN_TYPE,
 coalesce(regexp_replace( vm.value:transactionTime,'\\(|\\)',''),'') TXN_DATE,
  coalesce(regexp_replace(vm.value:settlementDate,'\\(|\\)',''),'') SETTLEMENT_DATE,
  coalesce(regexp_replace(vm.value:pgTransactionAmount,'\\(|\\)',''),'') GROSS_TXN_VALUE,
  coalesce(regexp_replace(vm.value:pgCommission,'\\(|\\)',''),'') PG_COMMISSION,
  coalesce(regexp_replace(vm.value:cgst,'\\(|\\)',''),'') CGST,
  coalesce(regexp_replace(vm.value:sgst,'\\(|\\)',''),'') SGST,
  coalesce(regexp_replace(vm.value:igst,'\\(|\\)',''),'') IGST,
  coalesce(regexp_replace(vm.value:amountTransferredToNodal,'\\(|\\)',''),'') AMOUNT_TRANSFERRED_TO_NODAL,
  coalesce(replace(replace(REPLACE(vm.value:utr, ']',''),'[',''),'"',''),'') UTR,
  coalesce(regexp_replace(vm.value:pgRefundId,'\\(|\\)',''),'') REFUND_ID,
  coalesce(CREATED_AT,'')CREATED_AT,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') paymentMode,
  coalesce(regexp_replace(vm.value:bank,'\\(|\\)',''),'') cardIssuer,
  coalesce(regexp_replace(vm.value:cardType,'\\(|\\)',''),'') cardType,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') createdBy,
  coalesce(regexp_replace(vm.value:cardNumber,'\\(|\\)',''),'') cardNumber,
  coalesce(regexp_replace(vm.value:binNumber,'\\(|\\)',''),'') binNumber,
  coalesce(regexp_replace(vm.value:last4Digits,'\\(|\\)',''),'') cardLastFourDigits,
  coalesce(regexp_replace(vm.value:authCode,'\\(|\\)',''),'') authCode,
  coalesce(regexp_replace(vm.value:bankReference,'\\(|\\)',''),'') bankReference,
 coalesce(regexp_replace( vm.value:rrn,'\\(|\\)',''),'')rrn,
  coalesce(lob,'')lob,
  coalesce(updated_at,'')updated_at,
  vm.value as arr
  
  
  from base
    , lateral flatten(input => parse_json(PG_CHARGEBACK_REVERSAL)) vm
),
PG_REFUND as
(
select order_id,
  'PG_REFUND' as description,
   coalesce(regexp_replace( vm.value:pg ,'\\(|\\)',''),'')PG,
 
 coalesce(regexp_replace( vm.value:merchantId,'\\(|\\)',''),'') MID,
  coalesce(TRANSACTION_ID ,'')txn_id,
  coalesce(regexp_replace(vm.value:pgTransactionId,'\\(|\\)',''),'') Pg_TXN_ID,
  coalesce(regexp_replace(vm.value:transactionType,'\\(|\\)',''),'') TXN_TYPE,
 coalesce(regexp_replace( vm.value:transactionTime,'\\(|\\)',''),'') TXN_DATE,
  coalesce(regexp_replace(vm.value:settlementDate,'\\(|\\)',''),'') SETTLEMENT_DATE,
  coalesce(regexp_replace(vm.value:pgTransactionAmount,'\\(|\\)',''),'') GROSS_TXN_VALUE,
  coalesce(regexp_replace(vm.value:pgCommission,'\\(|\\)',''),'') PG_COMMISSION,
  coalesce(regexp_replace(vm.value:cgst,'\\(|\\)',''),'') CGST,
  coalesce(regexp_replace(vm.value:sgst,'\\(|\\)',''),'') SGST,
  coalesce(regexp_replace(vm.value:igst,'\\(|\\)',''),'') IGST,
  coalesce(regexp_replace(vm.value:amountTransferredToNodal,'\\(|\\)',''),'') AMOUNT_TRANSFERRED_TO_NODAL,
  coalesce(replace(replace(REPLACE(vm.value:utr, ']',''),'[',''),'"',''),'') UTR,
  coalesce(regexp_replace(vm.value:pgRefundId,'\\(|\\)',''),'') REFUND_ID,
  coalesce(CREATED_AT,'')CREATED_AT,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') paymentMode,
  coalesce(regexp_replace(vm.value:bank,'\\(|\\)',''),'') cardIssuer,
  coalesce(regexp_replace(vm.value:cardType,'\\(|\\)',''),'') cardType,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') createdBy,
  coalesce(regexp_replace(vm.value:cardNumber,'\\(|\\)',''),'') cardNumber,
  coalesce(regexp_replace(vm.value:binNumber,'\\(|\\)',''),'') binNumber,
  coalesce(regexp_replace(vm.value:last4Digits,'\\(|\\)',''),'') cardLastFourDigits,
  coalesce(regexp_replace(vm.value:authCode,'\\(|\\)',''),'') authCode,
  coalesce(regexp_replace(vm.value:bankReference,'\\(|\\)',''),'') bankReference,
 coalesce(regexp_replace( vm.value:rrn,'\\(|\\)',''),'')rrn,
  coalesce(lob,'')lob,
  coalesce(updated_at,'')updated_at,
  vm.value as arr
  
  
  from base
    , lateral flatten(input => parse_json(PG_REFUND)) vm
),
PG_TRANSACTION as
(
select order_id,
  'PG_TRANSACTION' as description,
  coalesce(regexp_replace( vm.value:pg ,'\\(|\\)',''),'')PG,
 
 coalesce(regexp_replace( vm.value:merchantId,'\\(|\\)',''),'') MID,
  coalesce(TRANSACTION_ID ,'')txn_id,
  coalesce(regexp_replace(vm.value:pgTransactionId,'\\(|\\)',''),'') Pg_TXN_ID,
  coalesce(regexp_replace(vm.value:transactionType,'\\(|\\)',''),'') TXN_TYPE,
 coalesce(regexp_replace( vm.value:transactionTime,'\\(|\\)',''),'') TXN_DATE,
  coalesce(regexp_replace(vm.value:settlementDate,'\\(|\\)',''),'') SETTLEMENT_DATE,
  coalesce(regexp_replace(vm.value:pgTransactionAmount,'\\(|\\)',''),'') GROSS_TXN_VALUE,
  coalesce(regexp_replace(vm.value:pgCommission,'\\(|\\)',''),'') PG_COMMISSION,
  coalesce(regexp_replace(vm.value:cgst,'\\(|\\)',''),'') CGST,
  coalesce(regexp_replace(vm.value:sgst,'\\(|\\)',''),'') SGST,
  coalesce(regexp_replace(vm.value:igst,'\\(|\\)',''),'') IGST,
  coalesce(regexp_replace(vm.value:amountTransferredToNodal,'\\(|\\)',''),'') AMOUNT_TRANSFERRED_TO_NODAL,
  coalesce(replace(replace(REPLACE(vm.value:utr, ']',''),'[',''),'"',''),'') UTR,
  coalesce(regexp_replace(vm.value:pgRefundId,'\\(|\\)',''),'') REFUND_ID,
  coalesce(CREATED_AT,'')CREATED_AT,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') paymentMode,
  coalesce(regexp_replace(vm.value:bank,'\\(|\\)',''),'') cardIssuer,
  coalesce(regexp_replace(vm.value:cardType,'\\(|\\)',''),'') cardType,
  coalesce(regexp_replace(vm.value:paymentMode,'\\(|\\)',''),'') createdBy,
  coalesce(regexp_replace(vm.value:cardNumber,'\\(|\\)',''),'') cardNumber,
  coalesce(regexp_replace(vm.value:binNumber,'\\(|\\)',''),'') binNumber,
  coalesce(regexp_replace(vm.value:last4Digits,'\\(|\\)',''),'') cardLastFourDigits,
  coalesce(regexp_replace(vm.value:authCode,'\\(|\\)',''),'') authCode,
  coalesce(regexp_replace(vm.value:bankReference,'\\(|\\)',''),'') bankReference,
 coalesce(regexp_replace( vm.value:rrn,'\\(|\\)',''),'')rrn,
  coalesce(lob,'')lob,
  coalesce(updated_at,'')updated_at,
  vm.value as arr
  
  
  from base
    , lateral flatten(input => parse_json(PG_TRANSACTION)) vm
)

(select * from pg_chargeback
union
select * from PG_CHARGEBACK_REVERSAL
union
select * from PG_REFUND
union
select * from PG_TRANSACTION)
  
) b ON 
 a.PG=b.pg and 
a.MID=b.mid and
a.Pg_TXN_ID=b.Pg_TXN_ID and
a.TXN_TYPE=b.TXN_TYPE and
 a.AMOUNT_TRANSFERRED_TO_NODAL=b.AMOUNT_TRANSFERRED_TO_NODAL and
 a.UTR=b.utr and
a.REFUND_ID=b.REFUND_ID and
  a.paymentMode=b.paymentMode
WHEN NOT MATCHED  THEN
 INSERT
 (order_id,
description,
 PG,
MID,
txn_id,
Pg_TXN_ID,
TXN_TYPE,
  TXN_DATE,
SETTLEMENT_DATE,
  GROSS_TXN_VALUE,
PG_COMMISSION,
  CGST,
SGST,
 IGST,
 AMOUNT_TRANSFERRED_TO_NODAL,
 UTR,
REFUND_ID,
  CREATED_AT,
 paymentMode,
cardIssuer,
cardType,
createdBy,
 cardNumber,
binNumber,
 cardLastFourDigits,
 authCode,
 bankReference,
rrn,
 lob,
  updated_at,arr
 ) VALUES
 (b.order_id,
b.description,
 b.PG,
b.MID,
b.txn_id,
b.Pg_TXN_ID,
b.TXN_TYPE,
  b.TXN_DATE,
b.SETTLEMENT_DATE,
  b.GROSS_TXN_VALUE,
b.PG_COMMISSION,
  b.CGST,
b.SGST,
 b.IGST,
 b.AMOUNT_TRANSFERRED_TO_NODAL,
 b.UTR,
b.REFUND_ID,
  CREATED_AT,
 paymentMode,
cardIssuer,
cardType,
createdBy,
 cardNumber,
binNumber,
 cardLastFourDigits,
 authCode,
 bankReference,
rrn,
 lob,
  updated_at,arr
 );
