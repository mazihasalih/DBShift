SET START_DATE = CURRENT_DATE-1;

SET END_DATE = CURRENT_DATE-1;

-- queryid:01b53d95-3201-1e87-0005-7101b9d2304f --
create or replace TEMPORARY TABLE 2f7c2b19e8d34ada.d9262e7fb868c502.02389c9ef7f103b2 AS ( 
WITH PLACE_ORDER AS (
            SELECT 	DT,
                    SID,PARSE_JSON(REQUEST_PARAMS):place_order_source::VARCHAR as place_order_source
            FROM 	6754af9632a2745e.a2064f061d17278b.883058003aebca26
            WHERE 	DT BETWEEN $START_DATE AND $END_DATE
            AND 	place_order_source = 'turbocheckout'
            GROUP BY 1,2,3
  ),DP_PAYMENT_API AS (
        select a.DT,TIME_STAMP,TENANT,customer_id,BASIC_DATA, a.sid,
				          RESTAURANT_ID, 
				          PAYMENT_GROUP,
                          ADDITIONAL_DETAILS 
                         ,b.sid as dp_place_order_sid
           FROM (SELECT DT,TIME_STAMP,TENANT,customer_id,BASIC_DATA, BASIC_DATA:sid::varchar as sid,
            				          RESTAURANT_ID, 
            				          PAYMENT_GROUP,
                                      ADDITIONAL_DETAILS 
                    FROM 	6754af9632a2745e.a2064f061d17278b.de68711fcef34220 
            		WHERE 	DT  BETWEEN $START_DATE AND $END_DATE
                    ) A
         LEFT JOIN (SELECT SID,DT FROM PLACE_ORDER) B ON A.SID=B.SID AND A.DT=B.DT 
    
    )
            SELECT 
            A.DT,REQUEST_ID,
            A.SID,FINALFLAG,S_FINALFLAG,PAGE,dp_place_order_sid as dp_sid,P_FLAG,P_SESSION_FLAG,PREF_FLAG,PREFERRED_FLAG,
       CASE WHEN PAGE='payment'  AND  FINALFLAG = 0 AND S_FINALFLAG = 0 THEN 'F'
            WHEN PAGE='checkout' AND FINALFLAG = 1 AND dp_place_order_sid IS NOT NULL THEN 'T'
            WHEN PAGE='payment'  AND FINALFLAG = 0 AND S_FINALFLAG=1  THEN 'T'       
            END AS TURBO_FLAG,

       CASE WHEN PAGE='payment' AND  FINALFLAG = 0 AND S_FINALFLAG = 0 THEN 'NA'
            WHEN PAGE='checkout'  AND FINALFLAG = 1 AND dp_place_order_sid IS NOT NULL THEN 'TURBO-SLIDE'
            WHEN PAGE='payment'  AND FINALFLAG = 0 AND S_FINALFLAG=1  THEN 'TURBO-CHANGE' END AS TURBO_TYPE,PAYMENT_GROUP,

            ROW_NUMBER() OVER (PARTITION BY  A.SID ORDER BY ROUNDED_TIME DESC)  as rk ,
            CASE WHEN NOT(PAGE = 'checkout' AND dp_sid IS NULL AND P_SESSION_FLAG = 0) 
             AND NOT(PAGE = 'checkout' AND TURBO_TYPE IS NULL) 
             AND NOT(PAGE = 'payment' AND TURBO_TYPE IS NULL) THEN 1 ELSE 0  END AS condition_met

      FROM (
                          SELECT  DT,TO_TIMESTAMP_LTZ(TIME_STAMP) AS ROUNDED_TIME,
                          BASIC_DATA:request_id AS REQUEST_ID,
                          BASIC_DATA:sid::varchar as sid,
                          ADDITIONAL_DETAILS:page_visited AS PAGE,PAYMENT_GROUP,
                          CASE    WHEN B.VALUE:group_name = 'single_click' THEN 1 
                                                  ELSE 0 
                                                  END AS TC_FLAG,
                          CASE    WHEN B.VALUE:group_name = 'preferred' THEN 1 
                                                  ELSE 0 
                                                  END AS PREF_FLAG,
                        CASE    WHEN PAGE= 'payment' THEN 1 
                                                  ELSE 0 
                                                  END AS P_FLAG,dp_place_order_sid,
                  MAX(TC_FLAG) OVER( PARTITION BY REQUEST_ID) AS FINALFLAG, 
                  MAX(TC_FLAG) OVER( PARTITION BY SID) AS S_FINALFLAG,
                  MAX(P_FLAG) OVER( PARTITION BY SID) AS P_SESSION_FLAG,
                  MAX(PREF_FLAG) OVER( PARTITION BY SID) AS PREFERRED_FLAG
                  FROM DP_PAYMENT_API A,
                  LATERAL FLATTEN (INPUT => PAYMENT_GROUP) B
                  WHERE  A.DT BETWEEN  $START_DATE  AND $END_DATE
                  AND TENANT IN ('SW-FOOD', 'SW-FOOD-PWA')
                  --GROUP by 1,2,3,4,5,6,7,8
      ) A
      WHERE condition_met =1
      QUALIFY RK=1
      )
     ;

-- queryid:01b53c53-3201-1e87-0005-7101b9b55ccf --
create or replace TEMPORARY TABLE 2f7c2b19e8d34ada.d9262e7fb868c502.5fe47e35e26cf162  AS ( 
SELECT DT,SID, 
MAX(CASE WHEN GROUP_NAME='preferred' and PAYMENT_INDEX=0 THEN PAYMENT_NAME END) AS PPM1,
MAX(CASE WHEN GROUP_NAME='preferred' and PAYMENT_INDEX=1 THEN CONCAT(PAYMENT_NAME,'-',INTENT_APP) END) AS PPM2,
MAX(CASE WHEN GROUP_NAME='preferred' and PAYMENT_INDEX=2 THEN CONCAT(PAYMENT_NAME,'-',INTENT_APP) END) AS PPM3,
MAX(CASE WHEN GROUP_NAME='single_click' and PAYMENT_INDEX=0 THEN PAYMENT_NAME END) AS PAYMENT_SHOWN_ON_TURBO
FROM ( 

		SELECT P.*,CASE WHEN PAYMENT_NAME = 'UPIIntent' AND IAM.PACKAGE_NAME IS NOT NULL THEN IAM.INTENT_APP
					 WHEN PAYMENT_NAME <> 'UPIIntent' THEN 'INTENT N.A.'ELSE 'OTHER INTENT APP' END AS INTENT_APP
        FROM (
        SELECT 
				DT,	
				SID,
				TRY_PARSE_JSON(F.value):"group_name"::VARCHAR AS GROUP_NAME,
				TRY_PARSE_JSON(F.INDEX)::VARCHAR AS GROUP_INDEX,
				TRY_PARSE_JSON(F1.value):"payment_code"::VARCHAR AS PAYMENT_NAME,
				TRY_PARSE_JSON(F1.value):"enabled"::VARCHAR AS ENABLED,
                TRY_PARSE_JSON(F1.INDEX)::VARCHAR AS PAYMENT_INDEX,
				LOWER(TRY_PARSE_JSON(F1.value):"name"::VARCHAR) AS name,
                CASE WHEN GROUP_NAME = 'preferred' THEN 1 ELSE 0 END AS PREFERRED_FLAG 

		FROM 	2f7c2b19e8d34ada.d9262e7fb868c502.02389c9ef7f103b2,
				LATERAL FLATTEN(INPUT => PAYMENT_GROUP) F,
				LATERAL FLATTEN(INPUT => F.VALUE:"payment_methods") F1
        WHERE DT BETWEEN $START_DATE AND $END_DATE        
           ) P
         LEFT JOIN	2f7c2b19e8d34ada.d28aba2698af5fb0.30eeae34db51e44e IAM ON IAM.PACKAGE_NAME = P.NAME  
)
GROUP BY 1,2
)
;

DELETE FROM 2f7c2b19e8d34ada.d9262e7fb868c502.fad3b36a65ae6b1f
WHERE 	(DT BETWEEN $START_DATE AND $END_DATE) OR (DT < $START_DATE - 360);

-- queryid:01b53c5a-3201-1e87-0005-7101b9b5b5b7 --
INSERT INTO 2f7c2b19e8d34ada.d9262e7fb868c502.fad3b36a65ae6b1f (  
WITH  CHECKOUT AS (
		SELECT 	C.DT,USER_ID,
				BASIC_DATA:sid::VARCHAR AS SID_C,
				TRIM(LOWER(CITY)) AS CITY_NAME
		FROM 	6754af9632a2745e.a2064f061d17278b.64a52cf24d16c9c1 C
		WHERE 	C.DT BETWEEN $START_DATE AND $END_DATE 
        --and sid_c='drz8bf0d-b451-435d-bf9b-3dd5d52c2258'
        GROUP BY 1,2,3,4
),TURBO_CALLS_CARTS AS (
        SELECT  (SID)
        FROM    2f7c2b19e8d34ada.d9262e7fb868c502.02389c9ef7f103b2
        WHERE   S_FINALFLAG = 1
        AND PAGE = 'checkout'

),ORDER_DATA AS (    
		SELECT 	DT,
				SID AS SID_O,
				POST_STATUS,
				ORDER_ID::VARCHAR AS ORDER_ID,
				UPPER(COMPOSITE_PAYMENT_METHOD) AS COMPOSITE_PAYMENT_METHOD,
				UPPER(PAYMENT_GATEWAY) AS PAYMENT_GATEWAY,
				RESTAURANT_ID,
				CUST_PAYABLE,
				ORDER_CREATED_TIME 
		FROM 	6754af9632a2745e.1e7dc6d6c1656540.66cbf792c934f76e
		WHERE 	DT BETWEEN $START_DATE AND $END_DATE
		AND 	IGNORE_ORDER_FLAG=0 
) 
		SELECT	c.DT,C.USER_ID,
                C.SID_C AS SID_C,
                CASE WHEN CA.CLASSIFICATION IS NOT NULL THEN UPPER(CA.CLASSIFICATION) 
					ELSE IFNULL(CA.CLASSIFICATION, 'UNCLASSIFIED CITY') 
					END AS CITY_CLASSIFIER,	
				CASE WHEN LAUNCH_VERSION = '1' THEN UPPER(CA.CITY_NAME) 
					ELSE 'OTHER CITIES' 
					END AS CITY_NAME, 
                CASE    WHEN    TURBO_CARTS.SID IS NOT NULL THEN 1
                        ELSE    0
                END     AS      TURBO_SHOWN_ON_CART,
                CASE    WHEN    P.SID   IS NOT NULL THEN 1
                        ELSE    0
                END     AS      IS_PAYMENT_SESSION,

                CASE    WHEN    P.SID IS NOT NULL THEN P.TURBO_TYPE
                        ELSE    'NA'
                END     AS      TURBO_TYPE,
                CASE    WHEN    P.SID IS NOT NULL THEN P.TURBO_FLAG
                        ELSE    'NA'
                END     AS      TURBO_FLAG,
                CASE    WHEN    P.SID IS NOT NULL THEN P.REQUEST_ID::varchar
                        ELSE    'NA'
                END     AS      REQUEST_ID,

                CASE    WHEN    P.SID IS NOT NULL THEN P.P_SESSION_FLAG::varchar
                        ELSE    'NA'
                END     AS      P_SESSION_FLAG,
                CASE    WHEN    P.SID IS NOT NULL THEN P.PREFERRED_FLAG::varchar
                        ELSE    'NA'
                END     AS      PREFERRED_FLAG,
                CASE    WHEN    PA.SID IS NOT NULL THEN PA.PPM1::varchar
                        ELSE    'NA'
                END     AS      PPM1,
                CASE    WHEN    PA.SID IS NOT NULL THEN PA.PPM2::varchar
                        ELSE    'NA'
                END     AS      PPM2,
                 CASE    WHEN    PA.SID IS NOT NULL THEN PA.PPM3::varchar
                        ELSE    'NA'
                END     AS      PPM3,
                CASE    WHEN    PA.SID IS NOT NULL THEN PA.PAYMENT_SHOWN_ON_TURBO::varchar
                        ELSE    'NA'
                END     AS      PAYMENT_SHOWN_ON_TURBO,
                
                CASE    WHEN    O.SID_O IS NOT NULL THEN 1
                        ELSE    0
                END     AS      IS_ORDER_SESSION,
                CASE    WHEN    O.SID_O IS NOT NULL THEN O.ORDER_ID::varchar
                        ELSE    'NA'
                END     AS      ORDER_ID,
                
                CASE    WHEN    O.SID_O IS NOT NULL THEN O.POST_STATUS::varchar
                        ELSE    'NA'
                END     AS      POST_STATUS,
                CASE    WHEN    O.SID_O IS NOT NULL THEN O.COMPOSITE_PAYMENT_METHOD::varchar
                        ELSE    'NA'
                END     AS      COMPOSITE_PAYMENT_METHOD,
                CASE    WHEN    O.SID_O IS NOT NULL THEN PT.ORDER_ID::varchar
                        ELSE    'NA'
                END     AS      TRANSACTIN_ID,
                CASE    WHEN    O.SID_O IS NOT NULL THEN O.RESTAURANT_ID::varchar
                        ELSE    'NA'
                END     AS      RESTAURANT_ID
		 				
		FROM    CHECKOUT C
		LEFT JOIN 2f7c2b19e8d34ada.d9262e7fb868c502.02389c9ef7f103b2 P
		ON      C.SID_C = P.SID AND C.DT=P.DT
        LEFT    JOIN    TURBO_CALLS_CARTS TURBO_CARTS
         ON    C.SID_C = TURBO_CARTS.SID 
		LEFT JOIN ORDER_DATA O
		ON      P.SID = O.SID_O AND C.DT=O.DT
        LEFT JOIN 2f7c2b19e8d34ada.d9262e7fb868c502.5fe47e35e26cf162  PA
        ON PA.SID=C.SID_C
        LEFT JOIN 	(
        SELECT DT,ORDER_ID,			
        TRY_PARSE_JSON(PAYMENT_DETAILS):"omsOrderId"::VARCHAR AS OMS_ORDER_ID
        FROM dataoutput2.csv
        WHERE DT BETWEEN $START_DATE AND $END_DATE) 
         PT ON	O.ORDER_ID = PT.OMS_ORDER_ID AND O.DT=PT.DT
        LEFT JOIN (SELECT CLASSIFICATION,TRIM(LOWER(CITY_NAME)) AS CITY_NAME ,LAUNCH_VERSION FROM 2f7c2b19e8d34ada.d9262e7fb868c502.98b5843bc4ac4a3f) CA
		ON 		C.CITY_NAME = CA.CITY_NAME 
         );