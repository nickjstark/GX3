SELECT DISTINCT
    wsg.schedule_group_name sg,
    MAX(model_number) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched_model,
    MAX(we.wip_entity_name) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) wo,
    MAX(wdj.build_sequence) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) bs,
    MAX(wdj.scheduled_start_date) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched,
    MAX(wdj.start_quantity) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) ord_qty,
    MAX(wdj.quantity_completed) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) quantity_completed

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
    AND wsg.schedule_group_name IN ('COMMASSY', 'HS GAS','HS ELEC','SPEC')
WHERE
    bp.date_time_completion BETWEEN sysdate - 1 AND sysdate
    AND bp.production_line IN ('RES1', 'SPL1', 'COM1', 'REP', 'WIP', 'PEND','HSS','HSE','HSG')
    AND bp.organization_id = 101

UNION

SELECT
    wsg.schedule_group_name sg,
    msib.segment1,
    we.wip_entity_name,
    wdj.build_sequence,
    wdj.scheduled_start_date,
    wdj.start_quantity ord_qty,
    wdj.quantity_completed

FROM
    wip.wip_discrete_jobs wdj
JOIN wip.wip_entities we
ON
    we.wip_entity_id = wdj.wip_entity_id
JOIN wip.wip_schedule_groups wsg
ON
    wsg.schedule_group_id = wdj.schedule_group_id
    AND wsg.schedule_group_name IN ('COMMASSY','SPEC','HS GAS','HS ELEC')
JOIN inv.mtl_system_items_b msib
ON
    msib.organization_id = 101
    AND msib.organization_id = wdj.organization_id
    AND msib.inventory_item_id = wdj.primary_item_id
WHERE
    wdj.scheduled_start_date <= NEXT_DAY(TRUNC(sysdate), 'MONDAY') - 7
    AND wdj.quantity_completed != wdj.start_quantity
    AND wdj.status_type = 3 OR wdj.status_type = 6
ORDER BY
    sg,
    bs;
