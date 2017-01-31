SELECT DISTINCT
    wsg.schedule_group_name sg,
    last_value (date_time_completion) over (partition BY wsg.schedule_group_name order by bp.date_time_completion range BETWEEN unbounded preceding AND unbounded following) last_scan,
    last_value (model_number) over (partition BY wsg.schedule_group_name order by bp.date_time_completion range BETWEEN unbounded preceding AND unbounded following) current_model,
    last_value (date_time_completion) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) last_time_by_model,
    last_value (model_number) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by bp.date_time_completion) sched_model,
    last_value (we.wip_entity_name) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) wo,
    last_value (wdj.build_sequence) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) bs,
    last_value (wdj.scheduled_start_date) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) sched,
    last_value (wdj.start_quantity) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) ord_qty,
    last_value (wdj.quantity_completed) over (partition BY wsg.schedule_group_name, we.wip_entity_name order by date_time_completion range BETWEEN unbounded preceding AND unbounded following) bf_d,
    SUM (case when BP.PRODUCTION_LINE IN ('RES1', 'SPL1', 'COM1','HSS','HSG','HSE') THEN 1 ELSE 0 END) over (partition BY wsg.schedule_group_name, we.wip_entity_name) scanned
    ,SUM (case when BP.PRODUCTION_LINE IN ('RES1', 'SPL1', 'COM1','HSS','HSG','HSE') THEN 1 ELSE 0 END) over (partition BY wsg.schedule_group_name order by wdj.build_sequence range unbounded preceding) day_scanned
    
FROM
    bwc_production bp
JOIN wip.wip_entities we
ON
    we.wip_entity_name = bp.work_order_number
    AND we.organization_id = bp.organization_id
    AND we.organization_id = 101
JOIN wip.wip_discrete_jobs wdj
ON
    wdj.wip_entity_id = we.wip_entity_id
JOIN wip.wip_schedule_groups wsg
ON
    wsg.schedule_group_id = wdj.schedule_group_id
    AND wsg.schedule_group_name IN ('COMMASSY', 'FINAL ASSY', 'SPECIAL ASSY','HS GAS','HS ELEC','SPEC')
WHERE
    bp.date_time_completion BETWEEN sysdate - 1 AND sysdate
    AND bp.production_line IN ('RES1', 'SPL1', 'COM1', 'REP', 'WIP', 'PEND','HSS','HSE','HSG')
    AND bp.organization_id = 101

UNION

SELECT
    wsg.schedule_group_name sg,
    NULL,
    NULL,
    NULL,
    msib.segment1,
    we.wip_entity_name,
    wdj.build_sequence,
    wdj.scheduled_start_date,
    wdj.start_quantity ord_qty,
    wdj.quantity_completed,
    0,
    0
FROM
    wip.wip_discrete_jobs wdj
JOIN wip.wip_entities we
ON
    we.wip_entity_id = wdj.wip_entity_id
JOIN wip.wip_schedule_groups wsg
ON
    wsg.schedule_group_id = wdj.schedule_group_id
    AND wsg.schedule_group_name IN ('FINAL ASSY', 'SPECIAL ASSY', 'COMMASSY','SPEC','HS GAS','HS ELEC')
JOIN inv.mtl_system_items_b msib
ON
    msib.organization_id = 101
    AND msib.organization_id = wdj.organization_id
    AND msib.inventory_item_id = wdj.primary_item_id
WHERE
    wdj.scheduled_start_date < sysdate + 7
    AND wdj.quantity_completed = 0
    AND wdj.status_type = 3
ORDER BY
    sg,
    last_time_by_model nulls last,
    bs