-- OLD Refresh : 0 0 1,5,10,12,15,17 ? * * *
-- New Refresh : 0 15 0,3,6,9,12,15,18,21 ? * * *
delete from b969079f88836732.d9262e7fb868c502.297051a6397167f5 where dt>=current_date-1;

insert into b969079f88836732.d9262e7fb868c502.297051a6397167f5(
SELECT DT,TEMP_SENS,ORDERJOBID FROM(
(
with cte as
(select distinct * from (select * from (
select dt,orderjobid,parse_json(value)['perishable'] Temp_sens,hour(to_timestamp_ltz(timestamp::decimal/1000)) order_hour
from 6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d,lateral flatten(itemdetails)
where
  (dt>= CURRENT_DATE-1 AND DT <=CURRENT_DATE)
)))
,cte1 as(select * from cte where dt>= CURRENT_DATE-1 AND DT<= CURRENT_DATE and temp_sens='true')
,cte2 as(select * from cte where dt>= CURRENT_DATE-1 AND DT<=CURRENT_DATE and temp_sens='false'),cte3 as(
select * from cte2 where orderjobid not in(select distinct orderjobid from cte1))
select * from cte1 union select *  from cte3)))
;

delete from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4;

insert into 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
select orderjobid,slotted, timestamp,lower(state) as state1,dt,storeid,metadata,time_stamp 
from 6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d 
where dt=current_date
and state1 in ('picking','picked','delivery_ready','cancelled','assigned','un_assigned');

delete from 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039;

insert into 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039
select job_id, time_stamp,lower(status) as status1
from 6754af9632a2745e.a2064f061d17278b.75460396915f2efe 
where dt=current_date 
and status1 in ('delivery_pickedup','delivery_arrived');

delete from a6864eb339b0e1f6.efa1f375d76194fa.59d3d2e5a7291554 where dt=current_Date;

insert into a6864eb339b0e1f6.efa1f375d76194fa.59d3d2e5a7291554
with cte as
(
	select *, timestampdiff('second',ordered_time,vendor_assigned_time)/60 o2a, 
	timestampdiff('second',vendor_assigned_time,vendor_pickingstart_time)/60 a2c,
	timestampdiff('second',vendor_pickingstart_time,vendor_picked_time)/60 c2p, 
    timestampdiff('second',vendor_picked_time,vendor_markedready_time)/60 p2b,
    timestampdiff('second',ordered_time,vendor_pickingstart_time)/60 o2p,
	timestampdiff('second',ordered_time,vendor_markedready_time)/60 o2b, 
	timestampdiff('second',vendor_markedready_time,de_arrived)/60 b2ar,
	timestampdiff('second',de_arrived,de_picked)/60 ar2p, hour(ordered_time) hr
	from
	(
		select a.orderjobid,a.slotted, a.vendor_picking_time vendor_pickingstart_time, 
		b.vendor_picked_time vendor_picked_time, c.vendor_markedready_time vendor_markedready_time, 
		d.vendor_cancelled_time, e.vendor_assigned_time,f.vendor_ph_time,
		unassigned_flag, m.ordered_time, z.city, to_date(m.ordered_time) dt, 
		cast(h.store_id as varchar) store_id, h.store_name, h.status, i.de_arrived, j.de_picked,
		coalesce(k.ftr_flag,0) ftr_flag, coalesce(k.qty_diff,0) qty_diff, null as igcc_flag,
		null as missing_item_igcc, 
		null as wrong_item_igcc, null as packaging_igcc, 
		null as quality_fnv_igcc, null as quality_nonfnv_igcc, 
		null as wrong_order_igcc, null as missing_order_igcc,
		(case when lower(status) in ('delivery_delivered') then 1 else 0 end) del_flag
		from
		(
			select orderjobid,slotted, max(to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp) vendor_picking_time
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
			where state in ('picking')
			group by 1,2
		) a 
		left join
		(
			select orderjobid, max(to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp) vendor_picked_time
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
			where state in ('picked') 
			group by 1
		) b 
		on cast(a.orderjobid as varchar) = cast(b.orderjobid as varchar)
		left join
		(
			select orderjobid, max(to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp) vendor_markedready_time
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4 
			where state in ('delivery_ready') 
			group by 1
		) c 
		on cast(a.orderjobid as varchar) = cast(c.orderjobid as varchar)
		left join
		(
			select orderjobid, max(to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp) vendor_cancelled_time
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
			where state in ('cancelled') 
			group by 1
		) d 
		on cast(a.orderjobid as varchar) = cast(d.orderjobid as varchar)
		left join
		(
			select orderjobid, min(to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp) vendor_assigned_time
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
			where state in ('assigned') 
			group by 1
		) e 
		on cast(a.orderjobid as varchar) = cast(e.orderjobid as varchar)
		left join
		(
			select object_value,max(to_timestamp_ltz(TIME_STAMP)) as vendor_ph_time 
			from 6754af9632a2745e.a2064f061d17278b."47ab24519425d65d" 
			where object_name='place-order-in -pigeon-hole' 
            and dt=current_date
			group by 1
		)f 
		on cast(a.orderjobid as varchar) = cast(f.object_value as varchar)
		left join
		(
			select distinct orderjobid,
			case when lower(state) in ('un_assigned') then 1 else 0 end as unassigned_flag 
			from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
			where state in ('un_assigned')
		)g 
		on cast(a.orderjobid as varchar) = cast(g.orderjobid as varchar)
		left join
		(
			select id, status, dt, max(city) city,
			(metadata_json)['storeInfo']['id'] as store_id,
			replace((metadata_json)['storeInfo']['name'],'"','') as store_name
			from 
			(
				select *,to_date(to_timestamp_ltz(created_at::decimal/1000)) as date,
				last_value(event_id)over(partition by id order by event_id asc) as last_event_id
				from 6754af9632a2745e.a2064f061d17278b.bd7cc17e0b98e0b5 a
				where job_type in ('BUY','CUSTOM','instacart')
				and to_date(dt)=(current_date)
				and (lower(replace((metadata_json)['storeInfo']['storeType'],'"','')) in ('listed') or lower(replace((metadata_json)['storeInfo']['storeType'],'"','')) is null)
			) 
			where event_id = last_event_id
			group by 1,2,3,5,6
		) h
		on cast(a.orderjobid as varchar) = cast(h.id as varchar)
        left join
		(
			select job_id, min(cast(substring(cast(to_timestamp_ltz(time_stamp/1000) as varchar),1,19) as datetime)) as ordered_time
            from 6754af9632a2745e.a2064f061d17278b.639f449b88b40065 
            where dt = current_date  
            and type in ('BUY','CUSTOM','instacart','HANDPICKED') and status = 'CONFIRMED'
            group by 1
		)m
		on cast(a.orderjobid as varchar) = cast(m.job_id as varchar)
		left join 
		(
			select city,store_id,lower(store_name) as type
			from 2f7c2b19e8d34ada.d9262e7fb868c502.edaa44eba63b76d1
			where type='instacart'
		) z
		on cast(h.store_id as varchar) = cast(z.store_id as varchar)
		left join
		(
			select job_id, min(to_timestamp_ltz(time_stamp::decimal/1000)) de_arrived
			from 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039 
			where status in ('delivery_arrived') 
			group by 1
		) i 
		on cast(a.orderjobid as varchar) = cast(i.job_id as varchar)
		left join
		(
			select job_id, min(to_timestamp_ltz(time_stamp::decimal/1000)) de_picked
			from 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039 
			where status in ('delivery_pickedup') 
			group by 1
		) j 
		on cast(a.orderjobid as varchar) = cast(j.job_id as varchar)
		left join
		(
			select orderjobid, '1' ftr_flag, sum(qty_diff) qty_diff
			from
			(
				select distinct a.orderjobid, a.sku_old, a.old_quantity, 
				b.sku_new, b.new_quantity, (a.old_quantity-coalesce(b.new_quantity,0)) as qty_diff
				from
				(
					select distinct orderjobid, parse_json(b.value):skuId sku_old, 
					sum(parse_json(b.value):quantity) as old_quantity
					from
					(
						select *,
						row_number() OVER (partition BY ORDERJOBID ORDER BY to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp asc) AS urank
						from 6754af9632a2745e.a2064f061d17278b."2007ffc9d8bfdd2d"
						where state = 'PICKED' and dt=current_date
					) a,
					lateral flatten(input => metadata:oldItems, path => '') b
					where urank = 1
					group by 1,2
				) a 
				left join 
				(
					select distinct orderjobid, parse_json(b.value):skuId sku_new, sum(parse_json(b.value):quantity) as new_quantity
					from
					(
						select *,
						row_number() OVER (partition BY ORDERJOBID ORDER BY to_timestamp_ltz(cast(timestamp/1000 as bigint))::timestamp asc) AS urank
						from 6754af9632a2745e.a2064f061d17278b."2007ffc9d8bfdd2d"
						where state = 'PICKED' and dt=current_date
					) a,
					lateral flatten(input => metadata:newItems, path => '') b
					where urank = 1
					group by 1,2
				) b 
				on a.orderjobid = b.orderjobid
				and a.sku_old = b.sku_new
				having qty_diff > 0
			)
			group by 1,2
		) k
		on cast(a.orderjobid as varchar) = cast(k.orderjobid as varchar)
		where lower(h.store_name) in ('instacart')
	)
)

select a.*,b.temp_sens 
from cte a 
left join 
(
	select distinct orderjobid,temp_sens 
	from a6864eb339b0e1f6.efa1f375d76194fa.297051a6397167f5
) b 
on a.orderjobid=b.orderjobid
;

delete from 65f98121a162a56a.efa1f375d76194fa.56269461814d9e29;

insert into 65f98121a162a56a.efa1f375d76194fa.56269461814d9e29

WITH base AS (select a.hour hr, b.*, cast(dayofweek(current_date) as varchar) day_id, 
              case when dayname(current_date) IN ('Sat', 'Sun') then 'weekend' else 'weekday' end as current_day_type 
              from a6864eb339b0e1f6.efa1f375d76194fa.3b9cf97645e80eb9 a cross join 
              (select city, cast(store_id as varchar) store_id from 65f98121a162a56a.efa1f375d76194fa.9c378d0a2ffdaf82 
               where lower(store_name)='instacart' and active=1) b),

bus_hr AS (select cast(day_id as varchar) day_id, cast(store_id as varchar) store_id, 
           cast(split_part(open_time, ':', 1) as integer) open_hr, 
           case when cast(split_part(close_time, ':', 2) as integer)=0 
           and cast(split_part(close_time, ':', 3) as integer)=0 then cast(split_part(close_time, ':', 1) as integer)-1 
           else cast(split_part(close_time, ':', 1) as integer) end as close_hr 
           from cef84c440aef9bd5.0ff26e54712e66d6.68d47a7a0b4c2ac4 where deleted_time is null 
           and store_id IN (select distinct cast(store_id as varchar) store_id from 65f98121a162a56a.efa1f375d76194fa.9c378d0a2ffdaf82 
                            where lower(store_name)='instacart' and active=1)),

raw AS (select storeid store_id, enabled, threshold capacity, cooldown, STRESSRATIOTHRESHOLD capacity_mul, STRESSRATIOCOOLDOWN cooldown_mul, slot, 
        split_part(slot, '-', 1) start_time, split_part(slot, '-', 2) end_time, split_part(slot, '-', 3) day_type from 
        6754af9632a2745e.0a5765423c8374e5.1e34b56e985c5ccf),

non_default AS (select store_id, capacity, cooldown, capacity_mul, cooldown_mul, slot, 
                cast(split_part(start_time, ':', 1) as integer) start_time, cast(split_part(end_time, ':', 1) as integer) end_time, 
                day_type from raw where start_time NOT IN ('default', 'mfr') 
                and day_type IN (select distinct current_day_type from base)),

default_cap AS (select store_id, capacity, cooldown, capacity_mul, cooldown_mul, slot, enabled, start_time, 
                case when slot='default' then store_id else SPLIT_PART(store_id, '_', 3) end as storeid from raw 
                where start_time IN ('default', 'mfr')),

pre_final AS (select a.*, case when c.start_time='mfr' then 'RFD' else 'Pre-MFR' end as capacity_type, 
              case when c.start_time='mfr' then c.capacity else coalesce(b.capacity, c.capacity) end as capacity, 
              case when c.start_time='mfr' then c.cooldown else coalesce(b.cooldown, c.cooldown) end as cooldown, 
              case when c.start_time='mfr' then c.capacity_mul else coalesce(b.capacity_mul, c.capacity_mul) end as capacity_mul, 
              case when c.start_time='mfr' then c.cooldown_mul else coalesce(b.cooldown_mul, c.cooldown_mul) end as cooldown_mul
              from base a left join non_default b ON a.hr=b.start_time and a.store_id=b.store_id 
              left join default_cap c ON a.store_id=c.storeid 
              where enabled='true' or enabled is null),

final AS (select a.city, a.store_id storeid, a.hr, a.day_id, a.current_day_type, a.capacity_type, a.capacity threshold, 
          a.cooldown, a.capacity_mul threshold_mul, a.cooldown_mul from pre_final a 
          join bus_hr b ON a.day_id=b.day_id and a.store_id=b.store_id and a.hr between b.open_hr and b.close_hr)

select city, storeid, hr, day_id, current_day_type, capacity_type, threshold, cooldown, threshold_mul, cooldown_mul from final;

delete from  65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b;

insert into 65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b
select job_id, time_stamp,lower(status) as status1,dt,store_id,update_time,update_json
from 6754af9632a2745e.a2064f061d17278b.75460396915f2efe 
where dt=current_date 
and status1 in ('delivery_pickedup','delivery_arrived','placed','delivery_delivered','cancelled');

delete from  65f98121a162a56a.efa1f375d76194fa.3fb7dfad6a3e7593;

insert into 65f98121a162a56a.efa1f375d76194fa.3fb7dfad6a3e7593 
WITH stores_det AS 
(
	select a.id as STORE_ID, b.name as area, c.name as city,lower(a.name) as store_name
	from 98b06824c41bbe8e.1d0a85e7bcc3606f.4e7cf3cf5960fe13 a                       
	left join 98b06824c41bbe8e.1d0a85e7bcc3606f.4a91ee5f0106c2b3 b on a.area_id=b.id                       
	left join 98b06824c41bbe8e.1d0a85e7bcc3606f.11a62c23412b7747 c on c.id=b.city_id                       
	where store_name = 'instacart'
	group by 1,2,3,4
),  

sku_spin2 AS 
(
	select combo_spin as combo_spin_id, const_sku as const_sku_id,a.store_id,
	combo_sku as combo_sku_id, combo_type, const_qty from
	(
		select combo_spin,const_sku,store_id,combo_sku,const_qty, 
		count(const_sku) over (partition by store_id,combo_sku) as no_items,
		case when no_items >1 then 'hetero' else 'homo' end as combo_type
		from 65f98121a162a56a.efa1f375d76194fa.713ac56e25cb85a7
	) a
	join
	stores_det b
	on a.store_id = b.store_id
), 

ftr_items AS 
(
	select dt, storeid store_id, state, orderjobid, metadata, time_stamp,                 
	case when slotted='TRUE' then 'SLOTTED' else 'INSTANT' end as delivery_type                 
	from 
	65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4 a
	join
	stores_det b
	on a.storeid = b.store_id
	where dt=current_date and state='picked'                 
),    

old AS 
(
	select dt, store_id, state, orderjobid, k.value:"skuId"::string sku_id, k.value:"quantity"::integer ordered_qty,           
	to_timestamp(to_timestamp_ltz(time_stamp::decimal/1000)) vdc_ftr_time, delivery_type 
	from ftr_items,           
	lateral flatten(input=> parse_json(cast(metadata as varchar)):"oldItems") k
), 

new AS 
(
	select dt, store_id, state, orderjobid, k.value:"skuId"::string sku_id, k.value:"quantity"::integer new_qty,           
	to_timestamp(to_timestamp_ltz(time_stamp::decimal/1000)) vdc_ftr_time, delivery_type 
	from ftr_items,           
	lateral flatten(input=> parse_json(cast(metadata as varchar)):"newItems") k
),  

old_new AS 
(
	select a.*, coalesce(b.new_qty, 0) new_qty, 'FTR' reason,              
	case when a.ordered_qty-coalesce(b.new_qty, 0) != 0 then 'FTR' else 'non-ftr' end as vdc_ftr_flag 
	from old a               
	left join new b ON a.orderjobid=b.orderjobid and a.sku_id=b.sku_id
),  

placed_orders AS 
(
	select dt, a.store_id, job_id orderjobid, status1 as status, min(update_time) ordered_time 
	from                     
	65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b a
	join
	stores_det b
	on a.store_id = b.store_id
	where dt=current_date and status='placed'                     
	group by 1,2,3,4
),

del_orders AS 
(
	select a.dt, a.store_id, job_id orderjobid, status1 as status, min(update_time) delivery_time 
	from                  
	65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b a
	join
	placed_orders b
	on a.job_id = b.orderjobid
	where a.dt=current_date and status1='delivery_delivered'                                   
	group by 1,2,3,4
), 

canc_orders AS 
(
	select a.dt, a.store_id, job_id orderjobid, status1 as status,                   
	k.value:"item":"itemId"::string sku_id, k.value:"item":"quantity"::integer ordered_qty,                   
	parse_json(parse_json(update_json):status_meta):reason::string reason,                   
	parse_json(parse_json(update_json):metadata):"deliveryType"::string delivery_type, 'VDC' vdc_ftr_flag,                  
	min(update_time) vdc_ftr_time from                   
	65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b a
    join
	placed_orders b
	on a.job_id = b.orderjobid,             
	lateral flatten(input=> parse_json(parse_json(update_json):metadata):"bill":"billedItems") k  
	where a.dt=current_date  and status1='cancelled'                  
	and reason like any ('%DE Issue%', '%Order Item(s) not available%', '%Stores_VDC | VDC_DE found store closed%',   
	'%Stores_VDC | VDC_vendor app issue%', '%Stores_VDC | VDC_OOS%','%Stores_VDC | Others%',                                        
	'%Stores_VDC| VDC_incorrect OOS%', '%Delivery Executive crowding at store%',                                        
	'%Unable to service online orders%', '%Closing my store%', '%Customer rush at store%',                                        
	'%Stores_VDC | VDC_incorrect store location%', '%Stores_VDC | VDC_bill difference%')                   
	group by 1,2,3,4,5,6,7,8,9
), 

ftr_orders AS 
(
	select a.dt, a.store_id, a.orderjobid, sku_id, ordered_qty, delivery_type, vdc_ftr_time, reason, vdc_ftr_flag 
	from                  
	old_new a
	join
	del_orders b
	on a.orderjobid = b.orderjobid
	where vdc_ftr_flag='FTR' 
),   

vdc_ftr_orders AS 
(
	select dt, store_id, orderjobid, sku_id temp_sku, ordered_qty, delivery_type, vdc_ftr_time, reason, vdc_ftr_flag 
	from ftr_orders                      
	union                      
	select dt, store_id, orderjobid, sku_id temp_sku, ordered_qty, delivery_type, vdc_ftr_time, reason, vdc_ftr_flag 
	from canc_orders
),   

vdc_ftr_orders_final AS 
(
	select a.*, coalesce(b.const_sku_id, temp_sku) sku_id, b.const_qty, ordered_qty*coalesce(b.const_qty, 1) final_ordered_qty,
	case when const_sku_id is not null then temp_sku end as combo_sku_id 
	from vdc_ftr_orders a 
	left join sku_spin2 b ON a.temp_sku=b.combo_sku_id
),  

closing_stock AS 
(
	select sku, sum(sellable) as sellable_units,
	max(update_time) as update_time, to_date(max(update_time)) update_date 
	from 65f98121a162a56a.efa1f375d76194fa.5c04c59e5aee2c1e 
	where insert_date=dateadd('day', -2, current_date) and loc!='#inwarding_area'
    group by 1
),

current_day_eligible_stock AS 
(
	select sku, (sellable-coalesce(ordered, 0)) sellable_units,
	to_timestamp(to_timestamp_ltz(sellable_updated::decimal/1000)) update_time,  
	to_date(to_timestamp(to_timestamp_ltz(sellable_updated::decimal/1000))) update_date from 
	6754af9632a2745e.c69fa29d1c1d7f2f.b401bc9d69192265  
	where update_date between dateadd('day', -2, current_date) and current_date
),

inv AS 
(
	select sku, sellable_units, update_time, update_date from closing_stock union 
    select sku, sellable_units, update_time, update_date from current_day_eligible_stock),

vdc_ftr_ordered_time AS 
(
	select a.*, b.ordered_time 
	from vdc_ftr_orders_final a 
	left join 
	placed_orders b ON a.orderjobid=b.orderjobid
),   

pre_final AS 
(
	select * from (    
		select a.*, b.update_time inv_ordered_time, b.sellable_units order_time_qty, 
		row_number() over(partition by orderjobid, sku_id, combo_sku_id order by update_time desc) rnk 
		from vdc_ftr_ordered_time a     
		left join inv b ON a.sku_id=b.sku and a.ordered_time>b.update_time
	) where rnk=1
),  

final AS 
(
	select * from (    
	select a.*, b.update_time inv_vdc_ftr_time, b.sellable_units vdc_ftr_time_qty, 
	row_number() over(partition by orderjobid, sku_id, combo_sku_id order by update_time desc) rnk2 
	from pre_final a     
	left join inv b 
	ON a.sku_id=b.sku and a.vdc_ftr_time>b.update_time) where rnk2=1
) 

select to_date(a.ordered_time) order_dt, a.store_id, c.area, c.city, a.orderjobid, a.sku_id, 
a.combo_sku_id, b.item_code, b.item_name, a.ordered_qty, a.const_qty, a.final_ordered_qty, 
a.delivery_type, a.reason, a.vdc_ftr_flag, a.ordered_time, a.vdc_ftr_time, a.inv_ordered_time, 
a.order_time_qty, a.inv_vdc_ftr_time, a.vdc_ftr_time_qty 
from final a   
left join 65f98121a162a56a.efa1f375d76194fa.a8eb7ae446365afd b ON a.sku_id=b.sku_id 
left join stores_det c ON a.store_id=c.store_id;

-- Subcription report tables
-- FTR ALERT
delete from 65f98121a162a56a.efa1f375d76194fa.5dc47d46a08b42b7;

-- FTR ALERT
insert into 65f98121a162a56a.efa1f375d76194fa.5dc47d46a08b42b7
WITH ftr_items AS (select dt, storeid store_id, state, orderjobid, metadata 
                   from 6754af9632a2745e.a2064f061d17278b.026bebdcb2d91d13 
                   where dt=current_date and state='PICKED' 
                   and try_to_number(store_id) in (select distinct store_id from 65f98121a162a56a.efa1f375d76194fa.a8eb7ae446365afd)),

old AS (select dt, store_id, state, orderjobid, k.value:"skuId"::string sku_id, k.value:"quantity"::integer ordered_qty 
        from ftr_items, lateral flatten(input=> parse_json(cast(metadata as varchar)):"oldItems") k),

new AS (select dt, store_id, state, orderjobid, k.value:"skuId"::string sku_id, k.value:"quantity"::integer new_qty 
        from ftr_items, lateral flatten(input=> parse_json(cast(metadata as varchar)):"newItems") k),

old_new AS (select a.*, coalesce(b.new_qty, 0) new_qty, 'ftr' reason,
            case when a.ordered_qty-coalesce(b.new_qty, 0) != 0 then 1 else 0 end as ftr_flag from old a 
            left join new b ON a.orderjobid=b.orderjobid and a.sku_id=b.sku_id),

order_logs AS (select * from (
  select dt, store_id, job_id orderjobid, status,
  row_number() over(partition by job_id, status order by update_time) rnk
  from 6754af9632a2745e.a2064f061d17278b.75460396915f2efe 
  where dt=current_date 
  and status IN ('PLACED', 'DELIVERY_DELIVERED') 
  and store_id in (select distinct store_id from 65f98121a162a56a.efa1f375d76194fa.a8eb7ae446365afd) 
) 
  where rnk=1),

placed_orders AS (select distinct dt, store_id, orderjobid, status from 
                  order_logs where dt=current_date and status='PLACED'),

del_orders AS (select distinct orderjobid, status from order_logs where status='DELIVERY_DELIVERED'),

del_orders2 AS (select a.dt, a.store_id, a.orderjobid, b.status, 1 as order_flag from placed_orders a 
                left join 
                del_orders b ON a.orderjobid=b.orderjobid
                where b.status='DELIVERY_DELIVERED'),

ftr_orders AS (select distinct dt, orderjobid, ftr_flag from old_new where ftr_flag=1),

pre_final AS (select month(to_date(a.dt)) order_month, week(to_date(a.dt)) order_week, a.dt, a.store_id, a.orderjobid, a.order_flag, 
          b.ftr_flag from del_orders2 a 
          left join ftr_orders b ON a.orderjobid=b.orderjobid),

mim_split AS (select a.*, case when a.dt>=b.date then 1 else 0 end as mim_store_identifier from pre_final a left join (
    select distinct CAST(store_id AS VARCHAR) as store_id, date from 65f98121a162a56a.efa1f375d76194fa.729cda63878716d8 
    where store_id not IN ('3141', '517194')) b ON a.store_id=b.store_id),

final AS (select dt, store_id, mim_store_identifier, sum(order_flag) order_count, coalesce(sum(ftr_flag), 0) ftr_count, 
          coalesce(round((sum(order_flag)-coalesce(sum(ftr_flag), 0))*100/sum(order_flag), 4), 0) ftr_percent from mim_split 
          group by 1,2,3)

select a.store_id, ftr_count, order_count, ftr_percent, 
case when ftr_percent<99.3 then 'alert'else 'no_alert' end as alert_status,
b.city, b.outlet_name as store_name, current_timestamp as updated_time
    from final a left join (select distinct store_id, city, outlet_name, dense_rank() over (partition by store_id order by date desc) as rn from 65f98121a162a56a.efa1f375d76194fa.c45b3b26ff0856f6 qualify rn=1) b ON a.store_id=b.store_id;

-- Subscription Report
-- Timelegs alerts
delete from 65f98121a162a56a.efa1f375d76194fa.fe9f903c54c06c6a;

-- Timelegs alerts
insert into 65f98121a162a56a.efa1f375d76194fa.fe9f903c54c06c6a
select a.last_updated_data_at, a.dt,city, a.store_id, a.status,a.slotted, a.avg_o2c, a.avg_p2b, a.avg_c2p, a.alert_status, a.reason, b.outlet_name as store_name, a.avg_o2b
from 
(
	select current_timestamp as last_updated_data_at, dt,city, store_id,status,slotted, avg_o2c, avg_p2b, avg_c2p, avg_o2b, alert_status, reason
	from
	(
		SELECT *,
		case when avg_o2c>=0.5 or avg_p2b>=0.5 or avg_c2p>=1.8 then 'alert' else 'no_alert' end as alert_status,
		case when avg_o2c>=0.5 then 'O2C'
		when avg_p2b>=0.5 then 'P2B'
		when avg_c2p>=1.8 then 'C2P' end as reason
		FROM 
		(
			select * from 
			(
                select dt,city, store_id,status,slotted,
				avg(o2p) as avg_o2c,
				avg(p2b) as avg_p2b,
				avg(c2p) as avg_c2p,
				avg(o2b) as avg_o2b
				from
				(
                    with cte as
                    (
                    	select *, timestampdiff('second',ordered_time,vendor_assigned_time)/60 o2a, 
                    	timestampdiff('second',vendor_assigned_time,vendor_pickingstart_time)/60 a2c,
                    	timestampdiff('second',vendor_pickingstart_time,vendor_picked_time)/60 c2p, 
                        timestampdiff('second',vendor_picked_time,vendor_markedready_time)/60 p2b,
                        timestampdiff('second',ordered_time,vendor_pickingstart_time)/60 o2p,
                        timestampdiff('second',ordered_time,vendor_markedready_time)/60 o2b, 
                    	timestampdiff('second',vendor_markedready_time,de_arrived)/60 b2ar,
                    	timestampdiff('second',de_arrived,de_picked)/60 ar2p, hour(ordered_time) hr
                    	from
                    	(
                    		select a.orderjobid, a.slotted, a.vendor_picking_time as vendor_pickingstart_time, 
                    		a.vendor_picked_time as vendor_picked_time, a.vendor_markedready_time vendor_markedready_time, 
                    		a.vendor_cancelled_time, a.vendor_assigned_time,f.vendor_ph_time,
                    		unassigned_flag, m.ordered_time, z.city, to_date(m.ordered_time) dt, 
                    		cast(h.store_id as varchar) store_id, h.store_name, h.status, j.de_arrived, j.de_picked,
                    		(case when lower(status) in ('delivery_delivered') then 1 else 0 end) del_flag
                    		from
                    		( select orderjobid, max(slotted) as slotted, 
                                max(case when state='picking' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) vendor_picking_time, 
                                max(case when state='picked' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) vendor_picked_time, 
                                max(case when state='delivery_ready' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) vendor_markedready_time, 
                                max(case when state='cancelled' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) vendor_cancelled_time, 
                                max(case when state='assigned' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) vendor_assigned_time, 
                                max(case when state='un_assigned' then to_timestamp_ltz(cast(timestamp/1000 as bigint)) end) unassigned_flag
                                from 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4 group by 1
                            ) a
    
                    		left join
                    		(
                    			select object_value,max(to_timestamp_ltz(TIME_STAMP)) as vendor_ph_time 
                    			from 6754af9632a2745e.a2064f061d17278b."47ab24519425d65d" 
                    			where object_name='place-order-in -pigeon-hole' 
                                and dt=current_date
                    			group by 1
                    		)f 
                    		on cast(a.orderjobid as varchar) = cast(f.object_value as varchar)
                    		left join
                    		(
                    			select id, status, dt, max(city) city,
                    			(metadata_json)['storeInfo']['id'] as store_id,
                    			replace((metadata_json)['storeInfo']['name'],'"','') as store_name
                    			from 
                    			(
                    				select *,to_date(to_timestamp_ltz(created_at::decimal/1000)) as date,
                    				last_value(event_id)over(partition by id order by event_id asc) as last_event_id
                    				from 6754af9632a2745e.a2064f061d17278b.bd7cc17e0b98e0b5 a
                    				where job_type in ('BUY','CUSTOM','instacart')
                    				and to_date(dt)=(current_date)
                    				and (lower(replace((metadata_json)['storeInfo']['storeType'],'"','')) in ('listed') or 
                                    lower(replace((metadata_json)['storeInfo']['storeType'],'"','')) is null)
                    			) 
                    			where event_id = last_event_id
                    			group by 1,2,3,5,6
                    		) h
                    		on cast(a.orderjobid as varchar) = cast(h.id as varchar)
                            left join
                    		(
                    			select job_id, min(cast(substring(cast(to_timestamp_ltz(time_stamp/1000) as varchar),1,19) as datetime)) as ordered_time
                                from 6754af9632a2745e.a2064f061d17278b.639f449b88b40065 
                                where dt = current_date  
                                and type in ('BUY','CUSTOM','instacart','HANDPICKED') and status = 'CONFIRMED'
                                group by 1
                    		)m
                    		on cast(a.orderjobid as varchar) = cast(m.job_id as varchar)
                    		left join 
                    		(
                    			select city,store_id,lower(store_name) as type
                    			from 2f7c2b19e8d34ada.d9262e7fb868c502.edaa44eba63b76d1
                    			where type='instacart'
                    		) z
                    		on cast(h.store_id as varchar) = cast(z.store_id as varchar)
                    		left join
                    		( 
                            select job_id, min(case when status='delivery_arrived' then to_timestamp_ltz(cast(time_stamp/1000 as bigint)) end) de_arrived, 
                                min(case when status='delivery_pickedup' then to_timestamp_ltz(cast(time_stamp/1000 as bigint)) end) de_picked
                                from 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039 group by 1
                    		) j 
                    		on cast(a.orderjobid as varchar) = cast(j.job_id as varchar)
                    		where lower(h.store_name) = 'instacart'
                    	)
                    )
                    
                    select a.*,b.temp_sens 
                    from cte a 
                    left join 
                    (
                    	select distinct orderjobid,temp_sens 
                    	from a6864eb339b0e1f6.efa1f375d76194fa.297051a6397167f5
                    ) b 
            on a.orderjobid=b.orderjobid
			)
            group by 1,2,3,4,5
         )   
			where store_id in
			(select distinct cast(store_id as string) from 65f98121a162a56a.efa1f375d76194fa.d916d45a9e673c6f where dt=current_date-2 and lower(type)='instacart')
			and status='DELIVERY_DELIVERED' AND SLOTTED='FALSE' AND DT=CURRENT_DATE
		)
	) 
	where alert_status='alert'
)a
left join
(select distinct store_id, outlet_name, dense_rank() over (partition by store_id order by date desc) as rn 
from 65f98121a162a56a.efa1f375d76194fa.c45b3b26ff0856f6 qualify rn=1)b
on a.store_id=b.store_id;