SELECT
  DISTINCT ca.course_id as id,
  cc.name as name
FROM
  mwrite_peer_review.canvas_assignments ca
LEFT JOIN mwrite_peer_review.canvas_courses cc ON
  cc.id = ca.course_id
WHERE
  is_peer_review_assignment = 1
  AND ca.due_date_utc BETWEEN NOW() - INTERVAL {} MONTH AND NOW()
ORDER BY
  ca.due_date_utc DESC
  
