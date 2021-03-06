SELECT DISTINCT
    wsg.schedule_group_name sched_group,
    trunc(wmt.transaction_date - 3/24),
    SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN wmt.transaction_quantity ELSE 0 END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME, trunc(wmt.transaction_date - 3/24)) FIRST_SHFT,
    SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN 0 ELSE wmt.transaction_quantity  END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME, trunc(wmt.transaction_date - 3/24)) SCND_SHFT,
    SUM(wmt.transaction_quantity) OVER(PARTITION BY wsg.schedule_group_name) completed,
    repairs.scns
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
    LEFT JOIN
        (SELECT DISTINCT
                trunc(bp.DATE_TIME_COMPLETION + 3/24) dtc,
                COUNT(DISTINCT serial_number) OVER (PARTITION BY wsg.schedule_group_name, trunc(bp.DATE_TIME_COMPLETION + 3/24)) scns,
                wsg.schedule_group_name
            FROM bwc_production bp
            JOIN wip_entities we ON we.wip_entity_name = bp.work_order_number
            JOIN wip_discrete_jobs wdj ON wdj.wip_entity_id= we.wip_entity_id
            JOIN wip_schedule_groups wsg ON wsg.schedule_group_id = wdj.schedule_group_id
            WHERE bp.production_line = 'REP'
            AND bp.organization_id = 101
            AND bp.date_time_completion BETWEEN trunc(to_date('24-may-2017 03:00:00','DD-MON-YYYY HH24:MI:SS')) + 3 / 24 AND trunc(SYSDATE) + 3 / 24) REPAIRS
        ON REPAIRS.schedule_group_name = wsg.schedule_group_name AND repairs.dtc = trunc(wmt.transaction_date - 3/24)

WHERE
    wmt.organization_id = 101
    AND wmt.transaction_date BETWEEN trunc(to_date('24-may-2017 03:00:00','DD-MON-YYYY HH24:MI:SS')) + 3 / 24 AND trunc(SYSDATE) + 3 / 24
ORDER BY 1;
