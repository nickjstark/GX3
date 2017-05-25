SELECT DISTINCT
    wsg.schedule_group_name sched_group,
--    SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN wmt.transaction_quantity ELSE 0 END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME) FIRST_SHFT,
--    SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN 0 ELSE wmt.transaction_quantity  END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME) SCND_SHFT,
    SUM(CASE WHEN shift_scanned.shift = 1 THEN wmt.transaction_quantity ELSE 0 END) OVER (PARTITION BY wsg.schedule_group_name) FIRST_SHIFT,
    SUM(CASE WHEN shift_scanned.shift = 2 THEN wmt.transaction_quantity ELSE 0 END) OVER (PARTITION BY wsg.schedule_group_name) SCND_SHIFT,
    SUM(wmt.transaction_quantity) OVER(PARTITION BY wsg.schedule_group_name) completed,
    SUM(repairs.scns) OVER (PARTITION BY wsg.schedule_group_name) repair_scans
FROM wip_move_transactions wmt
    JOIN wip_discrete_jobs wdj
        ON wdj.wip_entity_id = wmt.wip_entity_id
        AND wdj.organization_id = wmt.organization_id
        AND wdj.organization_id = 101
    JOIN wip_entities we
        ON we.wip_entity_id = wdj.wip_entity_id
    JOIN wip_schedule_groups wsg
        ON wsg.organization_id = wdj.organization_id
        AND wsg.schedule_group_id = wdj.schedule_group_id
        AND wsg.schedule_group_name IN ('COMMASSY','HS GAS','HS ELEC','SPEC','CONVERSIONS','COMMCONV')
    LEFT JOIN   (SELECT
                    COUNT(DISTINCT serial_number) scans,
                    bp.shift,
                    bp.work_order_number
                FROM bwc_production bp
                WHERE bp.organization_id = 101
                AND bp.date_time_completion BETWEEN TRUNC(TO_DATE('01-jan-2017 03:00:00', 'DD-MON-YYYY HH24:MI:SS')) + 3 / 24 AND TRUNC(SYSDATE) + 3 / 24 --SHIFT_SCANNED
                GROUP BY  bp.work_order_number,
                          bp.shift) SHIFT_SCANNED
        ON shift_scanned.work_order_number = we.wip_entity_name

    LEFT JOIN   (SELECT
                    COUNT(DISTINCT serial_number) OVER (PARTITION BY BP.WORK_ORDER_NUMBER, trunc(bp.DATE_TIME_COMPLETION)) scns,
                    bp.work_order_number
                FROM bwc_production bp
                WHERE bp.production_line = 'REP'
                AND bp.organization_id = 101
                AND bp.date_time_completion BETWEEN trunc(to_date('01-jan-2017 03:00:00','DD-MON-YYYY HH24:MI:SS')) + 3 / 24 AND trunc(SYSDATE) + 3 / 24) REPAIRS
        ON REPAIRS.WORK_ORDER_NUMBER = WE.WIP_ENTITY_NAME

WHERE
        wmt.organization_id = 101
    AND wmt.transaction_date BETWEEN trunc(to_date('01-jan-2017 03:00:00','DD-MON-YYYY HH24:MI:SS')) + 3 / 24 AND trunc(SYSDATE) + 3 / 24
ORDER BY 1;
