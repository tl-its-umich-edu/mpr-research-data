-- See query documentation at…
-- https://docs.google.com/document/d/1FEDjYng1FilD64xx_XHct1zff_Ds9T9hsswFw-p2_J4/edit#bookmark=id.3znysh7
SELECT
  canvas_courses.name AS 'Course',
  canvas_courses.id AS 'CourseID',
  prompts.title AS 'Prompt',
  prompts.id AS 'PromptID',
  authors.sortable_name AS 'Author',
  authors.id AS 'AuthorID',
  reviewers.sortable_name AS 'Reviewer',
  reviewers.id AS 'ReviewerID',
  criteria.description AS 'Criterion',
  criteria.id AS 'CriterionID',
  peer_review_comments.comment AS 'Comment',
  peer_review_comments.id AS 'CommentID',
  peer_review_comments.commented_at_utc AS 'CommentTimeUTC'
FROM
  peer_review_comments
LEFT JOIN peer_reviews ON
  peer_review_comments.peer_review_id = peer_reviews.id
LEFT JOIN canvas_students AS reviewers ON
  peer_reviews.student_id = reviewers.id
LEFT JOIN canvas_submissions ON
  peer_reviews.submission_id = canvas_submissions.id
LEFT JOIN canvas_assignments AS prompts ON
  canvas_submissions.assignment_id = prompts.id
LEFT JOIN canvas_courses ON
  prompts.course_id = canvas_courses.id
LEFT JOIN rubrics ON
  prompts.id = rubrics.reviewed_assignment_id
LEFT JOIN canvas_students AS authors ON
  canvas_submissions.author_id = authors.id
LEFT JOIN criteria ON
  peer_review_comments.criterion_id = criteria.id
WHERE
  canvas_courses.id IN (
    516081
  )
ORDER BY
  prompts.id,
  authors.sortable_name,
  reviewers.sortable_name,
  peer_review_comments.criterion_id
