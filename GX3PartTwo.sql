SELECT DISTINCT
    wsg.schedule_group_name sched_group,
    SUM(WMT.TRANSACTION_QUANTITY) OVER (partition by WSG.SCHEDULE_GROUP_NAME) COMPLETED
FROM WIP_MOVE_TRANSACTIONS WMT

JOIN wip_discrete_jobs wdj
ON
  wdj.wip_entity_id = wmt.wip_entity_id
  AND  wdj.organization_id = wmt.organization_id
  AND wdj.organization_id = 101
JOIN
  wip_schedule_groups wsg ON wsg.organization_id = wdj.organization_id
  AND wsg.schedule_group_id = wdj.schedule_group_id
  AND wsg.schedule_group_name IN ('COMMASSY', 'HS GAS','HS ELEC','SPEC')
WHERE
   wmt.organization_id = 101
   AND wmt.TRANSACTION_DATE between TRUNC(sysdate -1) + 3/24 and TRUNC(sysdate) + 3/24
   ORDER BY 1;
